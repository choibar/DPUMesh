# Go 어댑터 재구성 전후 — 2026-09-29

판정: **동시 goroutine이 있는 경로(gRPC 형태)에서 지연·처리량이 개선됐고, 단일 goroutine 경로의 p50은 그대로다.** 기능 회귀는 없었다.

- 변경 전은 main `b607129`의 `integrations/grpc/go`이고, 변경 후는 같은 기반에 이번 변경을 얹은 것이다.
- 변경 내용:
  - 전역 `t.mu`를 없애고 연결별 lock을 둔다.
  - EQ poll 반복을 C 안에서 돌린다.
  - Dial은 lock 없이 수행하고, context가 끝나면 늦게 열린 QP를 abort한다.
  - QP destroy는 poller에서만 수행한다.
  - carrier는 slot 상태를 release/acquire로 게시한다.
- 두 버전은 같은 host 라이브러리, 같은 DPU proxy, 같은 서버에서 번갈아 실행했다.

## 결과

| 측정 | 변경 전 | 변경 후 | 비고 |
|---|---|---|---|
| gRPC unary 64B, 동시 1 — p50 / p99 | 404, 452 µs / 1.00, 1.04 ms | 353, 302, 351, 332 µs / 0.74, 0.67, 0.82, 0.77 ms | 변경 전 2회, 변경 후 4회 |
| gRPC unary 64B, 동시 1 — req/s | 2,316, 2,142 | 2,744, 3,238, 2,752, 2,871 | |
| gRPC unary 64B, 동시 64 — req/s | 36,727, 35,799 | 43,467, 43,929, 44,108, 42,795 | 약 +20% |
| gRPC unary 64B, 동시 64 — p50 | 1.55, 1.61 ms | 1.32, 1.26, 1.29, 1.34 ms | |
| raw 109B echo, reader/writer goroutine 분리 (`bench-echo`) — p50 | 252, 257, 321 µs | 146, 138, 132, 154, 135 µs | |
| 같은 측정 — p99 | 823, 796, 913 µs | 663, 631, 540, 691, 581 µs | |
| 같은 측정 — msg/s | 9,108, 9,083, 7,220 | 14,705, 15,767, 17,146, 14,297, 16,534 | |
| 같은 측정 — client CPU / 메시지 | 약 180 µs | 약 84 µs | user+sys ÷ 메시지 수 |
| raw 109B ping-pong, goroutine 1개 — p50 | 61.6, 59.4 µs | 62.5, 60.6 µs | 교대 실행 |
| 같은 측정 — p90 / p99 | 130, 123 µs / 343, 324 µs | 78, 87 µs / 292, 298 µs | |
| 참고: C preload client, 같은 형태 — p50 | 53.8–56.2 µs | | 같은 서버·경로 |
| Go `bench-server` idle CPU (연결 0, pool 2) | 15.6, 12.2 % | 11.6, 11.4 % | 5초 샘플 |

- 옛 어댑터는 Read가 직접 poll했다. 그래서 goroutine이 하나뿐이면 전역 lock 경쟁이 없어 p50이 C와 비슷했다. 손실은 poller, reader, writer가 동시에 도는 형태(gRPC의 reader와 loopy writer)에서만 나타났다. 변경 전 mutex profile에서는 lock 대기의 57%가 poll 중에 lock을 잡은 poller 때문이었다.
- `bench-echo` 기준으로 변경 후 p50 132–146 µs와 ping-pong 61 µs의 차이는 poller → reader → writer로 이어지는 Go 스케줄러 wake 비용이다. 이 차이를 줄이려면 Go runtime netpoller에 통합해야 하며, 이번 변경의 범위 밖이다.
- idle CPU의 나머지는 core의 50 µs tick 몫이다. 열린 flow가 하나라도 있으면 EQ가 깨어난다.
- 표본은 2–3회로 작고 CPU pinning도 하지 않았다. 차이가 큰 항목(동시 64 처리량, `bench-echo`)만 개선으로 판단한다.

## 기능 검증

| 검증 | 결과 |
|---|---|
| `go test -race ./...` (fake native, 장비 없음) | PASS. 단위 19개 + gRPC end-to-end 7개 |
| 병렬 스트레스 | race 바이너리 8개를 동시에 20회씩 실행(`-test.count=2 -test.cpu=1,4`). 스위트 640회, 실패 0 |
| gRPC end-to-end 항목 | unary 0 B–1 MiB (8064/8065 B, post max ±1 포함), 64개 goroutine 동시 호출, deadline 초과 시 서버 context 취소 후 연결 재사용, bidi stream client 취소, window보다 큰 server stream, GracefulStop (진행 중 stream 완료 후 client가 Unavailable로 즉시 실패), client Close 중 호출, server keepalive PING |
| fake 계약 검사 | 반납된 버퍼를 `0xDD`로 덮어써 반납 후 읽기를 검출, 이중 반납과 lease 누수 검사 |
| `make test` (carrier 변경 포함) | PASS |
| 장비 `channel-smoke` 4회 / 40회 (`POOL=2 MAX=4`) | PASS 3회(중간 코드 2회, 최종 코드 1회). proxy 유지, DPA thread 해제 98 / 102 / 102, DOCA 에러 0 |
| 장비 Go echo health RPC | PASS (warm 331–662 µs, 실행당 1회 표본) |

스트레스 중 새 구조의 결함 두 개를 찾아 고쳤다. 둘 다 회귀 테스트가 있고, 수정을 되돌리면 그 테스트가 실패하는 것도 확인했다.

- **Dial 등록 전 이벤트 유실.** QP의 첫 이벤트가 `create_qp` 반환 직후 연결 등록보다 먼저 poll될 수 있었다. 상대가 먼저 보내는 경우가 그렇다(HTTP/2 서버의 SETTINGS). 이 이벤트가 모르는 QP의 이벤트로 버려져 데이터나 FIN이 사라졌다. 이제 dial이 진행 중일 때 온 이벤트는 보관했다가 등록 시 넘긴다. destroy 중이거나 거절된 QP의 이벤트는 반납한다.
- **종료 경쟁.** poller가 명령 대기열과 stop 플래그를 따로 읽어서, 마지막 Close의 destroy 명령을 실행하지 않고 끝날 수 있었다. 이제 두 값을 같은 lock 구간에서 읽는다.

또한 기존부터 있던 동작 결함 하나를 고쳤다. listener와 연결이 없는 서버는 poll을 멈추기 때문에, 그 뒤 들어온 스트림이 거절되지 않고 매달렸다. 변경 후에는 서비스 채널을 수명 동안 계속 poll한다.
- 첫 listen 전에 들어온 스트림은 대기열에 두었다가 listener에 넘긴다.
- listener가 닫힌 뒤 들어온 스트림은 즉시 abort한다.

`host-dpa` reverse mode는 측정하지 않았다. rapids4에서 host DPA를 쓸 수 있는 function이 없었기 때문이다(`94:00.1`은 다른 사용자가 쓰고 있다).

## 측정 조건

| 항목 | 설정 |
|---|---|
| Host | rapids4 `192.168.100.1`, x86, Go 1.27.1, pinning 없음 |
| DPU | BlueField-3, DOCA 3.1.0105, `~/DPUMesh-online-boutique` (main `b607129` + proxy `c09915bf`) |
| DPU dev / rep | `03:00.0` / `94:00.0`, Comch server `DPUMeshBoutique0` |
| Proxy | `LINKERD2_PROXY_CORES=1`, `DMESH_NUM_WORKERS=1`, `DMESH_SHARDED=1`, `DMESH_BUSY_POLL=1`, `MOCK_OUTBOUND_OPAQUE=1`, `MOCK_POLICY_ECHO_TARGET=1` |
| Reverse | `dpu-dma` |
| gRPC 측정 | `cmd/bench-server` + `cmd/bench-client` (`BENCH_P=1`, `BENCH_M=1/64`, warm 2 s, 5 s) |
| raw 측정 서버 | `examples/preload/tcp_echo` (preload, `DPUMESH_SERVICE=localhost:18086`) |
| raw 측정 client | `cmd/bench-echo` (`BENCH_SIZE=109 BENCH_WINDOW=109`, 5 s), goroutine 1개 ping-pong probe (4000회 × 3), C preload `iovbench` |
| Proxy 재시작 | 측정 묶음마다 새 proxy |

## 자료 보존

ping-pong probe와 `iovbench` 소스, 실행 스크립트, host 로그는 host의 `~/DPUMesh-online-boutique/build/e2e/go-adapter-rework-2026-09-29/`에 **로컬로만** 보존한다(`build/`는 Git에 포함하지 않는다). probe는 `dmeshgo.DialAddress`로 연결 하나를 열고, goroutine 하나에서 109B `Write` 후 `io.ReadFull`을 4000회씩 3번 반복하며 RTT 분위수를 출력한다. 변경 전 측정은 같은 probe를 `git archive b607129 integrations/grpc/go`에 대해 빌드했다.
