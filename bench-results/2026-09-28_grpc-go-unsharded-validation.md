# Non-sharded DPU proxy: 실제 gRPC-go 검증

2026-09-28 UTC. 현재 global DPA context 변경을 포함한 working tree에서 실행했다.
**Busy-poll 구성은 짧은 반복 검증을 통과했지만, event 구성의 유휴 후 재연결에서 DPA fatal을 두 번 관찰해 전체 FAIL로 판정한다.**

## 조건

- `DMESH_SHARDED`를 unset. 현재 코드는 환경변수 존재 여부를 검사하므로 `DMESH_SHARDED=0`은 비활성화가 아니다.
- DPU proxy OS process 1개, `LINKERD2_PROXY_CORES=4`, CPU affinity 12–15. `dmesh-shard-*` thread가 없는 것과 실행 환경을 확인했다.
- `DMESH_NUM_WORKERS=1` 또는 `2`: 여기서는 Comch/Driver 수이며 ARM thread 수와 다르다. 각 Driver는 같은 Tokio multi-thread runtime의 task로 실행된다.
- Reverse mode `dpu-dma`. DPU PF `03:00.1`, host representor/Comch PF `0b:00.1`; Comch `DPUMesh0`, `DPUMesh1`.
- Driver마다 host client process 1개, server process 1개. Client 4 connections와 backend pool 4개. Connection당 동시 RPC 64개, 64B gRPC raw-codec echo.
- Host client CPU 0–7, server CPU 8–15, 각 process `GOMAXPROCS=8`. Mock controllers는 DPU CPU 0–3.
- Run마다 warmup 3초, 측정 10초. 모든 응답 payload를 검사하고 RPC error/reconnect/native dial count와 transport close를 확인한다.
- 반복 실행은 같은 proxy/server에 새 client process/channel을 연결한다. 연결 종료 후 재생성을 포함한다.
- Host library와 channel-bench는 최신 소스를 별도 `/tmp` 경로에서 빌드했다. 기존 host 설치본은 교체하지 않았다.
- NIC/SF/EU partition 설정 변경 없음. `DMESH_NO_TEARDOWN` unset.

## 결과

| Case | Drivers | Client connections 합계 | Busy poll | Repeat | 측정 완료 RPC | RPC/s | 결과 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| w1-busy | 1 | 4 | 1 | 1 | 287,475 | 28,747.5 | PASS |
| w1-busy | 1 | 4 | 1 | 2 | 287,785 | 28,778.5 | PASS |
| w2-busy | 2 | 8 | 1 | 1 | 419,562 | 41,956.2 | PASS |
| w2-busy | 2 | 8 | 1 | 2 | 417,262 | 41,726.2 | PASS |
| w2-event | 2 | 8 | 0 | 1 | 413,425 | 41,342.5 | Traffic PASS; 이후 lifecycle 실패 |
| w2-event | 2 | 8 | 0 | 2 | — | — | Preflight FAIL, DPA fatal |
| w2-event-confirm | 2 | 8 | 0 | 1 | 414,934 | 41,493.4 | PASS |
| w2-event-confirm | 2 | 8 | 0 | 2 | 426,953 | 42,695.3 | PASS |
| w2-event-confirm | 2 | 8 | 0 | 3 | — | — | 180초 유휴 후 Preflight FAIL, DPA fatal |

성공한 traffic run은 payload 검증 통과, RPC error 0, reconnect 0, connection마다 native dial 1회다.
성공한 7개 측정 구간의 완료 RPC 합계는 2,667,396개다. 별도 2개 시도는 preflight 실패했다.
실패한 preflight를 0 RPC/s 성능 값으로 집계하지 않는다. 이 수치는 기능 검증 중 관측값이며 정식 sharded/unsharded 성능 비교가 아니다.

단일 Driver busy-poll run에서 4개 Tokio thread의 CPU 사용률은 각각 약 78–87%, proxy 합계 약 322%였다.
2-Driver busy-poll은 합계 약 357–358%, event traffic은 약 370% 수준이다.
CPU 사용률은 측정 구간 `/proc/PID/task/TID/stat` 차이로 구했으며 100%가 ARM core 하나다.
이는 non-sharded runtime에서 작업이 여러 ARM thread에 걸쳐 실행됨을 보여준다. 하나의 Driver future 내부 DOCA progress가 동시에 실행된다는 의미는 아니다.
Busy polling으로 발생한 CPU 사용률만으로 연산 병목을 확정하지 않는다.

## 실패 증거와 해석

`w2-event` 첫 traffic은 23:26:35 무렵 정상 종료했다. Backend flow 정리/재생성 로그는 23:26:40까지 존재한다.
23:29:30 두 번째 client 연결 시 pool thread 4 재생성의 `doca_dpa_thread_start()`에서 다음 오류를 관찰했다.

```text
Fatal error (0x4) on RPC polling
Flexio RPC for init device params failed
Failed to start DPA thread: DOCA Driver call failure
Failed to recreate DPA pool thread 4
DPA context reported a device-side error
```

Crash dump PC `0x40002280`는 실행한 proxy에서 추출한 DPA ELF의 `run_dma_manager` 안에 있으며,
disassembly상 descriptor ring control load 사이에 해당한다. Register `a0=0x8000774e2c05fc80`의 하위 주소는
첫 server backend flow `10.99.2.2:32768`의 exported ring `0x774e2c05fc80`와 일치한다.
Ring mapping/lifetime 및 DPA 실행 상태를 추가 조사할 근거이며, 이 주소 대응만으로 use-after-free나 특정 SDK 결함을 확정하지 않는다.
Thread start 실패가 최초 원인인지 기존 device fault를 드러낸 것인지도 확정하지 않았다.

두 Driver가 context 하나를 공유하므로 fatal 이후 양쪽의 새 연결이 모두 실패했다.
종료 시 host server exit는 `[1, 1]`, proxy log에는 `retaining live flow resources after driver drop`이 남았다.
Proxy OS process exit 0만으로 정상 resource teardown으로 판정하면 안 된다.
새 proxy의 즉시 반복 2회는 성공했으므로 event 방식이 항상 실패하는 것은 아니다.
그러나 이 proxy에서 180초 유휴 대기 후 세 번째 client를 연결하자 23:35:02에 같은 thread start/Flexio RPC fatal과 양쪽 client preflight 실패가 재현됐다.
두 번째 crash PC는 `0x400023ee`로 같은 `run_dma_manager` ring polling loop 안이며, `a5=0x8000705da005fdc0`는 해당 실행의 첫 server backend ring 주소와 대응한다.
이 실행도 host server exit `[1, 1]`과 proxy의 live resource 보존 로그가 남았다. 프로세스 정리는 완료했지만 정상적인 SDK resource teardown 검증은 실패했다.
Busy-poll에는 동일한 180초 idle 대조군을 실행하지 않았으므로 이 현상이 event 모드에만 발생한다고 결론 내리지 않는다.
Sharded 대조군/변경 전 context 구현을 같은 조건으로 실행하지 않아 sharding 해제 또는 global context 변경에 원인을 귀속하지 않는다.

## 범위와 자료

Host-dpa, 16 Drivers, 장시간 부하, 장애 복구, HPACK 실험 경로는 이번 검증에 포함하지 않는다.
Driver가 2개인 실행에서 shared DPA context 생성 1회와 두 번째 acquire를 확인했다.
Busy-poll 두 case는 host server exit 0과 마지막 shared context destroy까지 확인했다.

- 작은 결과/CPU/cleanup/build identity: [JSON](2026-09-28_grpc-go-unsharded-validation.json).
- 로그, 실행 스크립트, host build log, source snapshot, crash/core, 추출 ELF와 disassembly는 로컬 `2026-09-28_grpc-go-unsharded-validation-raw.tar.gz`에 보존한다. 대형 archive는 Git 제외.
- 원래 실행 경로는 양쪽 머신의 `/tmp/dmesh-unsharded-20260928`이다.
- Root HEAD `b60712998e8b0747fb62f2195716aeeeab6faffb`, proxy submodule HEAD `c09915bfb38cbb8401b4d9136194923a6d23edd2`와 미커밋 global context 변경을 사용했다. Binary SHA-256은 JSON 참조.
- 초기 harness의 구 API service 이름(`bench-echo-0`)을 최신 `<host>:<port>` 형식으로 고친 뒤 측정했다. 수정 전 setup 실패 로그도 보존했으며 proxy datapath 실패로 집계하지 않았다.

모든 시험의 기록된 host/DPU process 종료와 잔여 test channel-bench process가 없는 것을 확인했다.
실패한 두 case는 정상 SDK teardown을 완료하지 못하고 OS process 종료로 정리했다.
이번 확인 작업에서는 transport/proxy 구현을 수정하지 않았고 테스트 결과 문서만 추가했다.
Raw archive SHA-256: `ed14aa0383beedadb5eec4fa626595b22217b0a27fb0b3c4096adf977b303a74`.
