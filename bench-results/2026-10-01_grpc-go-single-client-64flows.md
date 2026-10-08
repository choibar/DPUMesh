# gRPC-go: 한 client process, 16 workers / 16 server replicas, 64 client flows

2026-10-01. DPU-dma, sharded, busy-poll. 6개 조건 × 3회 반복.

## 조건

- 64B request/response 검증 echo, flow당 64 concurrent RPC. 32 flows는 2048개, 64 flows는 4096개 동시 RPC다. Warmup 3초 + measurement 10초. 아래 수치는 3회 중앙값이며 단일 구간의 peak sample이 아니다.
- Client process는 항상 하나다. Host client CPU0–7, 모든 server process는 CPU8–15를 공유한다. 각 process GOMAXPROCS=8. 16 replica마다 active backend connection 하나를 유지한다.
- 현재 channel당 32-flow 제한을 변경하지 않았다. `/tmp/dmesh-replica64-20261001/integrations/grpc/go` 실험용 복사본에서 channel 상태를 인스턴스화하여 한 process에서 channel/EQ/Comch session 2개를 연다. 각 channel은 별도의 기존 polling goroutine/transport mutex를 가진다. 64 flows를 교대로 32개씩 배정한다. 제품 Go 코드, native library, proxy 및 NIC/SF/EU 설정은 바꾸지 않았다.
- 32-flow/channel 1개와 32-flow/channel 2개를 함께 측정해 channel 수 증가의 영향을 비교했다. 기존 Go transport/benchmark unit tests PASS 후 실행했다.
- Replica는 별도 endpoint 10.0.1.1–16:8086이며 direct endpoint round-robin이다. Kubernetes Service LB 실험이 아니다. Client flow는 worker당 32-flow 조건에서 2개, 64-flow 조건에서 4개다. 각 worker의 backend flow 하나는 별도다.

## Worker 간 전달 조건

이전 32-flow 결과는 client flow owner와 대상 backend owner가 모든 flow에서 달랐다. 전체 sweep의 누적 placement cursor 때문에 source worker=(flow_index+13)%16, backend worker=flow_index%16이었다. 이번에 proxy를 새로 시작하면 cursor=0이고 같은 endpoint 순서를 사용할 때 둘이 일치한다. 따라서 다음 두 경로를 따로 측정했다.

- `same-worker`: source worker=i%16, backend worker=i%16.
- `cross-worker`: source worker=i%16, backend worker=(i+3)%16. 이전과 동일한 상대 offset 및 모든 flow가 worker 간 전달되는 조건이다. 물리 worker/replica 번호를 회전시킨 재현으로, 과거와 완전히 동일한 시간대의 실행은 아니다.

Dispatcher placement와 client target 로그를 대조하여 모든 flow의 실제 관계를 검증했다. Flow 증가 효과는 동일한 route_mode 내부에서 비교해야 한다.

## 결과

| 경로 | Client flows | Channels | 동시 RPC | kRPC/s | 반복 min–max kRPC/s | P99 ms | Host client CPU | Host servers 합계 CPU |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| same-worker | 32 | 1 | 2048 | 213.63 | 209.46–214.75 | 15.90 | 478.1% | 566.5% |
| same-worker | 32 | 2 | 2048 | 213.36 | 209.10–213.96 | 17.43 | 475.1% | 564.3% |
| same-worker | 64 | 2 | 4096 | 209.93 | 209.10–210.68 | 30.13 | 503.5% | 565.0% |
| cross-worker | 32 | 1 | 2048 | 164.39 | 163.84–166.13 | 21.21 | 394.9% | 473.5% |
| cross-worker | 32 | 2 | 2048 | 165.14 | 163.65–165.89 | 21.39 | 395.8% | 476.3% |
| cross-worker | 64 | 2 | 4096 | 154.13 | 152.75–156.41 | 55.25 | 419.3% | 456.6% |

## 해석

동일한 channel 2개 조건에서 32→64 flows로 늘리면 same-worker는 213.36→209.93K RPC/s(-1.6%), cross-worker는 165.14→154.13K RPC/s(-6.7%)였다. 동시 RPC 수는 2048→4096으로 늘었지만 처리량 이득 없이 P99가 각각 17.43→30.13ms, 21.39→55.25ms로 증가했다.

32 flows의 channel 1→2개 비교는 same-worker -0.1%, cross-worker +0.5%로 반복 변동 범위 안이다. 이번 부하에서 channel 하나의 공유 경로를 둘로 나누는 것만으로 처리량이 개선되지는 않았다. 반면 source/backend owner의 동일 worker 배치는 cross-worker보다 32-flow/channel 2개에서 29.2%, 64-flow에서 36.2% 높은 처리량을 보였다. Worker 간 전달 경로가 성능에 중요한 영향을 준다는 증거이며, 정확한 잔여 병목은 CPU stack profile 없이 특정 함수/lock으로 단정하지 않는다.

이전 32-flow/channel 1개 cross-worker 159.30K와 이번 164.39K는 별도 실행이다. 64-flow 209.93K를 과거 159.30K와 직접 비교하면 worker 배치 효과까지 flow 증가 효과로 잘못 계산하게 된다.

## DPU worker CPU

100%=core 하나. 측정 10초의 thread user+system CPU 증가량을 이용한 반복 중앙값. 동일한 이름의 SDK helper는 별도 TID로 구분한다. Busy-poll 시간이 포함되므로 높은 CPU만으로 유효 RPC 연산 포화를 단정하지 않는다.

| 경로 | Flows/channels | W0 | W1 | W2 | W3 | W4 | W5 | W6 | W7 | W8 | W9 | W10 | W11 | W12 | W13 | W14 | W15 |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| same-worker | 32/1 | 97.9% | 96.1% | 98.5% | 98.1% | 99.3% | 96.7% | 98.9% | 99.1% | 94.9% | 96.4% | 96.6% | 97.3% | 96.6% | 98.5% | 97.7% | 97.4% |
| same-worker | 32/2 | 99.0% | 97.6% | 97.3% | 97.4% | 97.1% | 99.3% | 98.3% | 98.1% | 97.1% | 96.1% | 97.6% | 98.1% | 93.6% | 95.2% | 93.9% | 97.2% |
| same-worker | 64/2 | 98.7% | 98.4% | 96.5% | 98.7% | 99.5% | 98.6% | 99.2% | 99.3% | 93.2% | 99.4% | 97.8% | 98.6% | 98.1% | 98.7% | 98.3% | 97.3% |
| cross-worker | 32/1 | 96.4% | 95.9% | 95.6% | 95.5% | 96.6% | 96.3% | 96.7% | 96.2% | 95.0% | 96.2% | 92.4% | 96.5% | 96.5% | 96.2% | 96.0% | 95.8% |
| cross-worker | 32/2 | 96.3% | 96.2% | 96.9% | 96.5% | 96.7% | 95.8% | 96.1% | 95.9% | 94.2% | 96.7% | 95.0% | 95.9% | 94.6% | 95.9% | 94.9% | 95.6% |
| cross-worker | 64/2 | 94.5% | 94.7% | 96.2% | 96.2% | 94.8% | 96.3% | 95.8% | 95.8% | 95.2% | 95.3% | 91.6% | 94.3% | 94.1% | 94.7% | 95.9% | 95.3% |

## 검증과 재현

- 총 18회 모두 `ok=true`, RPC errors=0, reconnects=0, native dial은 flow당 정확히 한 번이다. Measurement 구간 33,562,885개 payload를 검증했다.
- 각 replica의 server RPC counter 합계가 client preflight + warmup + measurement + drain 합계와 정확히 일치했다. 모든 client/server가 exit 0이며 proxy scoped cleanup 완료, 종료 후 DPA processes=0을 확인했다.
- 각 경로의 `audit.json`, `host.py`, `suite.py`, 원본 로그, CPU snapshot, server counts, client PID/start time/command를 raw archive에 보존했다. 두 경로는 같은 benchmark binary를 사용한다.
- Proxy/native library는 직전 실험과 동일하다. 새 Go benchmark binary 및 수정된 Go source SHA-256은 `host.sha256`에 보존한다.

Raw directories: `/tmp/dmesh-replica64-20261001` (same-worker), `/tmp/dmesh-replica64-cross-20261001` (cross-worker). Host 실행 위치는 `youngmin@192.168.100.1`의 동일 경로다.

실행: DPU에서 `python3 /tmp/dmesh-replica64-20261001/suite.py`와 `python3 /tmp/dmesh-replica64-cross-20261001/suite.py`를 순차 실행했다. Lifecycle 로그가 남아 있는 directory는 재사용하지 않아야 한다.

Host client command: `taskset -c 0-7 channel-bench -mode client -connections 64 -concurrency 4096 -warmup 3s -duration 10s -rpc-timeout 10s -timeout 90s`, with `GOMAXPROCS=8 DPUMESH_BENCH_CHANNELS=2` and the common/endpoint environment recorded in `host.py`.

[JSON](2026-10-01_grpc-go-single-client-64flows.json), [CSV](2026-10-01_grpc-go-single-client-64flows.csv), [worker CPU CSV](2026-10-01_grpc-go-single-client-64flows-worker-cpu.csv).

[Raw archive](2026-10-01_grpc-go-single-client-64flows-raw.tar.gz), SHA-256: `4317bea84e61df11af1eba91fe2b378b4a543a3926ce8aa004c29a19813e4323`.
