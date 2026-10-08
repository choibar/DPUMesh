# DSB replica routing 구현 및 실측

2026-09-29. [승인 설계](../docs/2026-09-29_dsb-replica-routing-design.md)의 단계 A와 B를 구현했다. 이전의 동일 VIP/단일 Forward로 마지막 replica에 RPC가 집중되던 문제를 수정했다. Host client는 service VIP를 사용하고 DPU의 P2C/Peak-EWMA balancer가 RPC마다 고유 replica endpoint를 선택한다. Ingress worker별 routing stack과 endpoint owner worker의 공유 H2 transport를 사용한다.

## 정확성 및 lifecycle

- 9개 gRPC service health, User 업무 RPC 및 HTTP business 경로 검증: 9개 구성 모두 통과. HTTP 응답은 기존 직접 TCP 기준 응답과 비교했다.
- 단일 client connection의 실제 User RPC 2,048개를 4 replicas로 분산: [462, 612, 482, 492].
- 36개 replica endpoint를 각각 직접 health 검사: 모두 통과.
- 세 manifest 소비자의 적용 로그와 1초 전파 시간을 둔 뒤 user-0 제거 후 RPC 분포: [0, 674, 653, 721]. 제거한 endpoint에는 신규 RPC가 없었다.
- 복구 후: [209, 1001, 426, 412].
- 실제 Host user-0 process를 정상 종료/재시작하고 직접 probe 성공. 이후 VIP 분포: [229, 1030, 386, 403].
- P2C/Peak-EWMA는 적응형 분산이므로 균등한 round robin 비율을 요구하지 않는다.

## 측정 조건

DPU busy-poll, sharded dpu-dma, 4/8/16 Arm workers. 9개 gRPC service당 1/2/4 replicas와 HTTP frontend 4개. Host 서비스는 CPU 0–11/GOMAXPROCS=4, wrk2는 CPU 12–15, frontend당 HTTP connections 64개. Tracing=0, TLS=false, 원래 DSB mixed HTTP workload. 별도 Compose project에서 DB/cache를 구성별 reset했다. 서버 method audit counters를 모든 DMA run에서 켰다.

2,000 HTTP/s 20초 warmup 후 **8,000 HTTP/s offered, 30초 × 3회**의 중앙값을 표시한다. 4-replica 구성에는 4,000 offered/30초 측정도 추가했다. 기능 검증/probe는 부하 구간 밖에서 수행했다. 최종 matrix 중 compiler는 실행하지 않았다. 16-worker 구성은 mock/control-plane도 일부 Arm CPU를 공유한다.

**이 표는 고정 offered load 비교이며 안정 처리량/SLO에 따른 최대 처리량은 아니다.** p99는 frontend별 p99 중 최대값의 3회 중앙값이며 global p99가 아니다. 내부 RPC/s는 측정 전후 counter 차이/30초의 근사치로, 짧은 종료 drain 구간을 포함한다.

| Arm workers | replicas/service | HTTP/s 중앙값 | 내부 RPC/s ≈ | Host service CPU | DPU worker CPU 범위 | max frontend p99 |
|---:|---:|---:|---:|---:|---:|---:|
| 4 | 1 | 5,290 | 20,133 | 1033.8% | 98.4–99.0% | 13,470 ms |
| 4 | 2 | 6,008 | 22,852 | 1157.6% | 98.8–99.3% | 8,830 ms |
| 4 | 4 | 5,302 | 20,150 | 1190.8% | 98.3–99.4% | 10,350 ms |
| 8 | 1 | 6,391 | 24,252 | 1081.4% | 98.2–100.0% | 11,890 ms |
| 8 | 2 | 6,782 | 25,771 | 1164.1% | 98.8–99.1% | 6,090 ms |
| 8 | 4 | 6,216 | 23,608 | 1193.8% | 98.8–99.0% | 7,030 ms |
| 16 | 1 | 7,822 | 29,752 | 1091.7% | 92.5–99.8% | 1,270 ms |
| 16 | 2 | 7,061 | 26,842 | 1166.2% | 94.3–99.8% | 4,870 ms |
| 16 | 4 | 6,273 | 23,828 | 1193.0% | 92.0–99.2% | 7,040 ms |

CPU 100%는 한 core 기준이며 Host 서비스의 12-core 한계는 1,200%다. DPU는 busy-poll이므로 높은 CPU만으로 유효 작업이 병목이라고 판단할 수 없다. 4-replica 구성에서는 Host 서비스 합계가 약 1,190%로 포화됐고 DPU 8→16 cores 확장 이득도 작았다. Host CPU 예산이 제한 요인이라는 근거지만 Host CPU를 추가한 대조 시험 없이 유일한 병목이라고 단정하지 않는다. Host native transport 진행 처리 비용도 이 CPU 값에 포함된다. 고정 Host CPU 예산에서 process/replica 증가가 무조건 처리량 증가를 뜻하지 않는다.

64B echo의 200K **RPC/s**와 DSB의 **HTTP/s**는 다른 단위/작업이다. 이번 DSB에서는 외부 HTTP 요청당 약 3.8개 내부 RPC와 DB/cache/serialization 비용이 있다. 라우팅 결함을 수정한 사실과 end-to-end 처리량 한계는 구분해야 한다.

4-replica의 4,000 offered reference:

- 4 workers: 3,978.17 HTTP/s, max frontend p99 88.06 ms.
- 8 workers: 3,950.07 HTTP/s, max frontend p99 78.01 ms.
- 16 workers: 3,948.68 HTTP/s, max frontend p99 146.69 ms.

## 오류와 자원

27개 측정 trial의 wrk2 HTTP/socket/process 오류는 모두 없었다. 내부 gRPC error counter는 합계 2,231건 증가했으므로 전체 RPC 오류 0이라고 주장하지 않는다. 16-worker/4-replica에서 별도로 500ms 간격 추적한 error 증가분은 모두 각 wrk 종료 후 0–0.9초에 집중됐다. Load generator의 종료 시점 취소와 일치하는 패턴이나 이 counter는 status code를 기록하지 않는다.

| workers | replicas/service | DPA context | 사전 생성 threads | peak active threads | alloc/release | peak H2 owners |
|---:|---:|---:|---:|---:|---:|---:|
| 4 | 1 | 1 | 128 | 49 | 59/59 | 9 |
| 4 | 2 | 1 | 128 | 69 | 78/78 | 18 |
| 4 | 4 | 1 | 128 | 109 | 161/161 | 36 |
| 8 | 1 | 1 | 256 | 49 | 58/58 | 9 |
| 8 | 2 | 1 | 256 | 67 | 77/77 | 18 |
| 8 | 4 | 1 | 256 | 106 | 116/116 | 36 |
| 16 | 1 | 1 | 512 | 49 | 60/60 | 9 |
| 16 | 2 | 1 | 512 | 67 | 77/77 | 18 |
| 16 | 4 | 1 | 512 | 107 | 117/117 | 36 |

모든 endpoint의 동시 H2 owner는 최대 1개였다. 4 replicas × 9 services = 36 pools이며 worker 수만큼 복제되지 않았다. Weak directory의 retire/recreate가 있으므로 누적 owner 생성 수와 동시 pool 수는 다르다. DPA thread는 기존 per-flow 모델이며 context 공유/사전 thread pool 크기는 이번 routing 수정으로 바꾸지 않았다. w4-r4에는 36개 직접 probe와 재시작을 포함하므로 누적 allocation이 더 많다. 모든 구성에서 host/proxy 정상 stop, DPA allocation/release 일치, remaining=0. 실험용 Compose project도 종료했다. NIC/SF/EU partition 변경 및 재부팅은 수행하지 않았다.

## 빌드 및 회귀 검증

- 깨끗한 upstream HotelReservation에 patch + overlay 적용, Go race test(dmesh/dialer), 전체 cmd build 통과.
- Python topology 6 tests, Rust manifest 2 tests, dmesh-doca 14 tests 통과.
- H2 owner unit test: 두 routing stack의 pool 공유, 실제 owner thread에서 생성/call, 취소 전파, bounded admission 통과.
- DPU proxy 및 mock policy/destination release build 통과. Build hash는 JSON에 기록했다. 최종 binary 빌드 이후에는 Rust/Go 신규 코드의 whitespace formatting만 했다.
- 기존 manifest 없는 64B gRPC echo smoke: 1 Arm core, 2 connections, conn당 64 in-flight, 10초, 16,694.3 RPC/s, errors/reconnects=0, host/proxy 정상 종료. 짧은 호환성 검사이며 이전 16-core 성능 시험을 대체하지 않는다.
- root/submodule diff whitespace 검사 통과.

## 구현 범위와 제한

- Manifest schema v2, service VIP/replica address 분리, 기존 HTTP balancer 재사용. Explicit atomic manifest reload이며 실제 Kubernetes/Consul watcher는 추가하지 않았다.
- 공유 H2 key는 endpoint/고정 owner/H2 params/TLS identity다. 동일 worker 요청도 128-capacity owner queue를 이용하며 별도 H2 hop은 없다.
- 내부 readiness 대기는 3초. 자동 RPC retry는 추가하지 않았다. Body/trailers는 whole-body buffering 없이 전달한다.
- 제거 시 새 dispatch를 막고 cached client를 내려놓는다. 이미 전송한 RPC는 기존 취소/deadline lifecycle로 끝난다. 독립적인 강제 streaming-drain timer, 별도 endpoint-generation key, live worker migration은 없다.
- Mixed workload가 사용하지 않는 review/attractions는 직접 endpoint health로 확인했다. Hardware lifecycle 검증은 unary DSB와 정상 process restart다. 무제한 streaming drain, 강제 crash/slot 재사용 race, 매우 느린 replica의 latency-adaptive 회피는 별도의 검증이 필요하다.
- 이전 audit의 4-core/4-replica 단일 trial은 약 5,602.54 HTTP/s였다. 이번 3회 중앙값 5,302 HTTP/s와 직접적인 성능 향상을 주장하지 않는다. 이전 run은 routing이 잘못되어 한 replica가 처리했고 단일 trial이다. 이번 변경의 확인된 이득은 올바른 replica 분산과 제한된 연결 수이며, 4-replica의 8/16 cores에서 약 6.2K HTTP/s를 관찰했다.

## Evidence

- [작은 JSON](2026-09-29_deathstarbench-replica-routing.json), [CSV](2026-09-29_deathstarbench-replica-routing.csv).
- 최종 실행 경로: DPU/Host `/tmp/dsb-routing-final-v2`, host source `/tmp/dsb-routing/hotel-final`.
- 로컬 raw archive에는 실행 controller, source changes, build/test 로그, topology, CPU samples, replica counters와 전체 wrk/proxy 로그를 포함한다. 대형 archive는 Git에 포함하지 않는다.
- 초기 A/B pilot은 compiler 활동과 겹쳤으므로 최종 성능 표에서 제외했다. 첫 final run은 probe의 DPUMESH_CONFIG 누락으로 load 측정 전에 중단했고, 수정 후 별도 v2 디렉터리에서 전부 재실행했다.

Raw archive: `2026-09-29_deathstarbench-replica-routing-raw.tar.gz` (로컬, Git 제외).
SHA-256: `72c8ff8e4455bdd7af7d2470864ec2b1a24f5dfc24772a2f1fdb2d4fa7a84c1d`.
최종 PID birth-time 재확인: DPU 40개, Host 226개 기록 process 모두 종료; 실험 Compose container 잔존 없음.
