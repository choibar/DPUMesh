# DeathStarBench 이전 구현 비교 및 성능 회복 — 2026-09-29

**18K offered load에서 수정 전 9,177 → 최종 17,060 HTTP req/s (1.86배).** 최종은 30초 × 3회 중앙값이며 범위는 16,855–17,099다. Host 앱 CPU는 14.94 → 12.13코어, HTTP당 CPU는 1628 → 711µs다.

과거 18K 부하의 17,897 req/s에 비해 최종 중앙값은 4.7% 낮다. 큰 처리량 회귀는 제거했지만 과거 지연까지 완전히 회복한 결과는 아니다. 과거 18K의 p99 730–1,150ms에 비해 현재 18K는 3,320ms다. 과거/현재 조건 차이는 아래에 명시한다.

이전 17–18K HTTP req/s와 현재 구현의 차이를 소스와 실측으로 비교했다. Host polling 비용과 DPU worker 배치 불균형을 각각 분리해 재현했다. 최근 7.8K 결과는 8K offered load 측정이었으며 최대 처리량 자체가 아니었다. 별도로 14K/18K를 가한 결과 수정 전의 실제 포화도 확인됐다.

## 비교 기준

- 과거: [9월 3일 보고서](2026-09-03_dsb-hotelres-dma-jet1.md), root `3473469`, proxy `3ce3a7ab`. 당시 세션 로그에서 복원한 결과이며 이번에 옛 바이너리를 재실행한 값은 아니다.
- DSB 기준: `hotelres-bench-patches`의 `312855ac450bcd35550a482517fd47e093a26ac4` + 현재 포팅. 같은 mixed Lua의 SHA-256은 `af6230aaaf220fceaef6208d416c5a9d75f15f66d70e3cde00b6e441957bf352`.
- replica: reservation/rate/search 각 4, profile/geo/recommendation 각 2, user/review/attractions 각 1; frontend 4. 합계 25 Host 프로세스.
- Host jet1 CPU 0–15, 각 프로세스 GOMAXPROCS=4. DB/cache도 Host의 격리 Compose. DPU는 dpu-dma, sharded 16 Arm workers, busy-poll. wrk2만 r4 CPU 0–3에 배치했다.
- 이전 DSB wrk2 소스를 r4에서 다시 빌드했다. frontend당 `-D exp -t4 -c64`, 30초 측정, HTTP는 r4→jet1 Ethernet 직접 전송. warmup은 총 6K HTTP/s로 35초.
- 남는 과거 비교 차이: 외부 r4 부하, Go/runtime 설정, event→busy-poll DPU, Host RR→DPU per-RPC LB, 계측 설정, 부하점마다 재시작 여부. 이번 수정 전후 비교에서는 명시한 Host 코드/배치/proxy 변경 외의 조건을 유지했다.
- CPU는 앱 user+system 합계, 100%=1코어. DB/cache CPU는 JSON에 별도 기록. 과거 `host busy`는 wrk2/infra를 포함한 전체 Host이므로 수치를 같은 정의로 취급하면 안 된다.
- p99는 각 trial에서 frontend 4개의 p99 중 최댓값이며 전체 histogram의 p99가 아니다. 최종 반복은 중앙값, 진단은 1회이며 표에 n을 표시한다.

## 구현 차이

| 항목 | 이전 구현 | 수정 전 현재 구현 | 최종 변경 |
|---|---|---|---|
| Go 대기 | 각 Conn.Read에서 2→128µs sleep backoff | process EQ + C ppoll, native 1ms spin/50µs timer | DPU-DMA는 Go timer backoff, host-DPA는 Go netpoll |
| RX scan | 해당 connection | 매번 32 stripe에 원자적 lock 시도 | 활성/retirement 중인 stripe만 scan |
| control progress | per-flow Comch | 공유 Comch인데 flow/event마다 반복 progress | channel drain pass당 한 번 |
| replica 선택 | Host에서 replica별 ClientConn RR | DPU per-RPC P2C, process channel worker에 송신 집중 | per-RPC LB 유지, frontend worker 독립 배치 |
| DPU busy-poll | 과거 기준은 event mode | arm은 생략하지만 data notification clear는 반복 | busy-poll에서는 drain-only |
| RX 앱 copy | DMA window→pending ring→Go | native RX lease→Go | 현재의 1회 copy 유지 |

이전 코드에는 오히려 RX staging copy가 하나 더 있었다. 이번 회귀를 zero-copy 버퍼 설계 자체의 문제로 볼 근거는 없다. gRPC/H2/protobuf 처리는 여전히 Host에서 실행되며, TCP baseline은 같은 Host의 loopback이라는 점도 고려해야 한다.

## 재현 및 대조 결과

| 조건 | 목표 HTTP/s | 실제 HTTP/s | Host 앱 cores | p99 ms | n |
|---|---:|---:|---:|---:|---:|
| tcp-oldspec-v2 | 14000 | 13994 | 14.02 | 17.84 | 1 |
| tcp-oldspec-v2 | 18000 | 17925 | 14.31 | 45.92 | 1 |
| current-oldspec-v2 | 14000 | 9100 | 14.91 | 11670.00 | 1 |
| current-oldspec-v2 | 18000 | 9177 | 14.94 | 15200.00 | 1 |
| tuned-oldspec-v2 | 14000 | 12623 | 11.38 | 4680.00 | 1 |
| tuned-oldspec-v2 | 18000 | 13152 | 11.50 | 10540.00 | 1 |
| fixed-oldspec | 14000 | 13316 | 12.71 | 3650.00 | 1 |
| fixed-oldspec | 18000 | 13017 | 12.66 | 10360.00 | 1 |
| fixed-go-timer | 14000 | 13031 | 11.18 | 4470.00 | 1 |
| fixed-go-timer | 18000 | 13365 | 11.24 | 10770.00 | 1 |
| fixed-spread | 14000 | 13980 | 11.65 | 483.07 | 1 |
| fixed-spread | 18000 | 16788 | 12.08 | 3690.00 | 1 |
| final-spread | 14000 | 13931 | 11.71 | 372.22 | 3 |
| final-spread | 18000 | 17060 | 12.13 | 3320.00 | 3 |
| final-spread | 24000 | 16999 | 12.07 | 9540.00 | 1 |

- `current-oldspec-v2`: 수정 전 바이너리. frontend workers 5/6/7/8, search workers 6/7/8/9. 세 frontend가 search의 outbound 처리와 겹친다.
- `tuned-oldspec-v2`: 코드 변경 없이 spin=0, tick=1ms. CPU는 줄지만 처리량/지연 회복에 부족하다.
- `fixed-oldspec`: 활성 scan/control batching + EQ netpoll을 사용한 중간 실험. 이 단계에서 시험한 native idle backoff/default spin 변경은 최종 패치에서 제거했다.
- `fixed-go-timer`: DPU-DMA를 Go timer로 대기. Host CPU는 줄었지만 13K 부근의 DPU 병목은 남았다.
- `fixed-spread`: Host 수정은 동일, frontend workers만 14/15/3/4로 변경. backend endpoint/worker, replica 수, 연결 수는 같다.
- `final-spread`: 최종 Host/DPU 바이너리, frontend workers 14/15/12/13. native 기본 spin/tick은 원래 1000µs/50µs이며 DPU-DMA Go 경로에서는 EQ fd를 켜지 않아 이 notification polling 경로를 쓰지 않는다. 중간 spread 대비 DPU 변경과 배치가 함께 바뀌었으므로 마지막 소폭 향상을 notification clear 제거 하나의 효과로 단정하지 않는다.

## 원인과 검증 근거

1. **Host의 빈 polling 비용.** [이전 CPU 분석](2026-09-29_deathstarbench-hostcpu-r4.md)에서 40프로세스 기본 DMA는 idle 9.91코어를 사용했다. 이번 25프로세스에서도 수정 전 idle 6.37코어였다. Go timer 진단은 idle 0.57코어다. C ppoll의 OS-thread sleep/wake, eventfd spin, 반복 scan이 요청과 무관한 비용을 만들었다.
2. **불필요한 scan/control 작업.** 활성 비트는 flow가 열릴 때 publish하고 custody/FIN retirement 이후에만 지운다. shared PE를 한 번 progress한 후 모든 flow가 session error를 관찰하도록 했다. native 테스트는 공유 오류 전파, closed stripe retirement 및 버퍼 custody/close 계약을 검증한다.
3. **DPU source worker 집중.** Host 수정 이후에는 앱 CPU가 약 11.2코어인데도 처리량이 13K에 머물렀다. 같은 Host 코드에서 frontend 배치만 바꾸자 18K 부하의 달성 처리량이 13.4K→16.8K로 늘었다. busy-poll에서는 모든 core의 util=100%가 유효 L7 작업이 고르게 분산됐다는 뜻이 아니다. 별도 DPU perf에서 frontend/search worker에는 routing/H2 작업이, 다른 worker에는 idle driver/syscall이 더 많이 관찰됐다.
4. **대기 방식은 전송 모드에 맞춰야 한다.** DPU-DMA에는 data doorbell이 없어 Go timer로 bounded progress를 수행한다. host-DPA는 native EQ를 Go netpoll에 연결한다. Go가 duplicate fd만 닫도록 해서 native fd 소유권을 유지했다. local Write는 poller를 깨우고 native retained-TX deadline은 다음 대기를 줄인다.

14K offered load의 별도 진단에서 EQ netpoll 대비 Go timer의 kernel self sample은 25.81%→14.69%였다. native DPUMesh sample은 각각 2.67%/3.13%, Go runtime은 45.95%/50.52%였다. 이는 on-CPU sample 구성비이며 기다린 시간이나 코드별 인과 효과의 정확한 분해가 아니다. 프로파일 trial은 처리량 요약에서 제외했다.

## 코드와 재현

- `integrations/grpc/go/dmesh.go`, `eq_wait_linux.go`: 모드별 Go 대기, TX wakeup, netpoll duplicate 수명.
- `src/core/carrier.c`, `native_transport.h`, `dmesh_core.c`, `src/transport/host/channel.{c,h}`: 활성 stripe와 channel 단위 control progress. 공개 ABI5/Comch protocol, per-channel Comch, per-flow DPA resource 모델은 유지한다.
- `linkerd2-proxy/linkerd/doca/src/driver.rs`: busy-poll notification clear 제거. `build.rs`는 SDK 링크 순서를 C archive 뒤로 옮겨 standalone DOCA crate 테스트의 constructor symbol 링크 실패를 해결했다.
- `integrations/deathstarbench/topology.py`: `--frontend-workers`와 flow-budget 검증. workload마다 좋은 배치는 달라서 임의의 worker 배치를 모든 서비스의 최적값으로 간주하지 않는다.
- native 테스트/ABI 검사, Go 및 DSB race tests, DPU DOCA crate 14개 테스트를 실행했다. 최종 release 빌드는 `RUSTFLAGS="--cfg tokio_unstable -C target-cpu=native"` 및 기존 LTO 설정을 사용한다.

실제 lifecycle 검증 결과:

```json
[
  {
    "mode": "dpu-dma",
    "client_exit": 0,
    "ok": true,
    "server_exit": 0
  },
  {
    "mode": "host-dpa",
    "ok": false,
    "error": "server exit 1",
    "server_exit": 1
  }
]
```

원본 로그, 실행 스크립트, per-process CPU samples, RPC counters, perf, build logs는 `/tmp/dsb-regression-20260929`에 보존했다. 최종 artifact의 native/proxy/Go 바이너리 hash와 topology를 함께 기록한다. raw archive는 로컬 보존이며 Git에는 작은 MD/JSON/CSV를 남긴다.

측정 시작 전 실패도 보존했다: Host wrk 바이너리는 r4 glibc와 맞지 않아 r4에서 재빌드했고, 격리 infra 디렉터리는 재사용 대신 새 디렉터리로 생성했다. Rust 테스트 링크 순서 및 release tokio cfg 실패는 수정 후 재검증했다. 실패한 시작은 성능 trial에 포함하지 않았다.

## 최종 해석과 남은 한계

- 14K의 최종 실제 처리량은 13,931 req/s, 앱 CPU 11.71코어였다. 같은 이번 TCP 대조 실험은 13,994 req/s, 14.02코어였다. TCP 대조는 1회, 최종 DMA는 3회이므로 작은 차이의 유의성은 주장하지 않는다.
- 18K는 현재 포화점보다 높은 부하다. 24K를 넣어도 16,999 req/s, p99 9,540ms로 처리량이 늘지 않았다. 따라서 17K를 낮은 지연으로 안정적으로 처리하는 용량이라고 해석하면 안 된다. 후속 6K recovery는 5,890 req/s, p99 53.38ms로 회복했다.
- 최종 14K p99는 80.83–439.30ms(중앙값 372.22ms), 18K는 3,090–3,680ms다. 처리량 회복과 tail-latency 완전 회복은 구분한다.
- 이번 수정 전후의 원인 분리는 polling 설정 대조 및 frontend-only 재배치로 확인했다. 남는 한계는 per-process channel의 source worker가 해당 프로세스의 모든 송신 H2/L7 작업을 맡는 구조다. 모든 Arm util이 높아도 4개 frontend의 fan-out 작업을 16개 worker에 자동 분배하는 것은 아니다. 현재 per-RPC replica LB와 source-worker CPU 분산은 다른 문제다.
- 최종 18K 첫 trial의 worker별 CPU는 약 92–98%였고 busy-poll을 유지했다. CPU util 자체로 유효 작업 균형을 판단하지 않는다. DPU perf와 재배치 효과가 집중 병목의 근거다.
- 최종 throughput trials의 r4 최대 단일 코어 평균 util은 21.74% 이하, Host Ethernet TX는 56.7Mbps 이하/1Gbps였다. r4 CPU나 외부 링크 포화가 현재 17K 한계라는 증거는 없다.
- 최종 non-profile 부하 trial의 HTTP/socket 오류는 0이다. 내부 RPC error counter는 14K 세 trial 합계 108/4,763,051, 18K 세 trial 합계 18/5,815,659로 소량 증가했다. trial 종료 경계 취소 가능성은 있지만 개별 원인을 이 counter만으로 확정하지 않았다.
- HTTP당 내부 RPC 수는 약 3.8이다. 이 처리량의 단위는 mixed HTTP req/s이며, 64B echo microbenchmark의 gRPC RPC/s와 직접 비교하지 않는다.
- 최종 별도 Host perf self sample: Go runtime 50.85%, kernel 14.42%, native DPUMesh 3.30%, gRPC/H2 9.73%, protobuf 2.58%. 남은 Host 비용의 상당 부분은 runtime/serialization/application 처리다. native copy만 없애면 전체 비용이 사라지는 구조는 아니다.

## 검증 제한 및 정리

- DPU-DMA 실기기 channel smoke: 40 close/reopen rounds, sibling 통신 유지, 채널 재생성, payload 1/8064/8065/8192/8193/65537B를 검증했고 client/server가 정상 종료했다.
- host-DPA는 PF0 및 PF1 모두 `doca_dpa_start`에서 DOCA_ERROR_DRIVER, syndrome 0xb398a0, `Error message buffer init failed`로 시작하지 못했다. EQ 대기 코드에 도달하기 전 실패다. DPU의 read-only EU query는 configured partition 0개였다. host-DPA의 Go netpoll은 nested-epoll 단위/race 검증까지만 완료했고 실기기 통과로 표기하지 않는다.
- EU partition/NIC/SF/sysctl은 변경하지 않았다. 테스트 Host 프로세스 204개, DPU 역할 프로세스 32개, r4 wrk 프로세스 132개의 PID birth를 대조해 살아 있는 테스트 프로세스가 0개임을 확인했다. 격리 Compose container/network도 제거했다.
- 최종 DPU shared context는 1개이며 pool은 16 workers × 32 = 512 DPA threads다. 87/87은 flow에 대한 pool thread checkout/return 횟수이며 thread 생성 수가 아니다. DPU-DMA smoke는 45/45이며 전체 진단의 할당/반납도 일치한다.
- 원본 archive에서는 큰 proxy 바이너리와 symbol filesystem만 제외했다. 바이너리 및 소스 hash, perf 원본/분석 CSV, 실행 스크립트, build/test logs, r4 원본, topology, CPU/RPC samples를 보존했다. 바이너리 자체를 포함한 독립 재현 bundle은 아니다.

원본 archive: `2026-09-29_dsb-hotelres-performance-fix-raw.tar.gz` (로컬, Git 제외).
SHA-256: `2dd75fa05e720b35b1f2e514d91cf90af35d88667ac4659bd91b5d646108e4a4`.
