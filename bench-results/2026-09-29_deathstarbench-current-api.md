# HotelReservation: current DPUMesh API port and hardware validation

2026-09-29 UTC. **구현 및 W=1/2/4 통합 검증 완료.**
후속 [replica audit](2026-09-29_deathstarbench-replica-audit.md)에서 현재 mock-policy 기반
DMA 경로가 서비스별 한 replica에 집중됨을 확인했다. 아래는 기능/lifecycle 검증 결과이며,
replica 부하 분산 구현 완료 또는 정상적인 replica scaling 결과를 뜻하지 않는다.

현재 `integrations/grpc/go`와 `libdpumesh.so.5`로 HotelReservation의 서비스 간 gRPC를
sharded + busy-poll + dpu-dma 경로에 연결했다. 재현 방법은
[통합 README](../integrations/deathstarbench/README.md), 반복별 데이터는
[JSON](2026-09-29_deathstarbench-current-api.json) / [CSV](2026-09-29_deathstarbench-current-api.csv)에 있다.

## 구현

- clean DeathStarBench `312855ac450bcd35550a482517fd47e093a26ac4`에 재적용 가능한 patch와 overlay.
  Host의 기존 dirty checkout과 구형 `hotelres-dmesh` 포팅은 보존했다.
- 공통 dialer, 9개 service listener, 10개 cmd lifecycle을 현재 API로 연결.
  프로세스당 channel/Comch/EQ 하나, 서비스당 VIP, replica별 Pod IP와 고유 Consul ID.
  모듈 replace는 prepare 단계에서 생성한다. 업무 RPC 정의/로직은 유지한다.
- 외부 HTTP, MongoDB/Memcached, Consul/Jaeger는 기존 transport. DMA TLS는 명시적으로 거절.
  tracing interceptor와 context를 전달하고, keepalive는 서버 기본 최소값에 맞춰 5분으로 지정.
- manifest가 worker/주소/CPU/자원 예산을 생성. 실행별 PID·birth·exe를 검사하여 TERM으로 정리.
  gRPC/HTTP drain 후 outbound와 listener를 닫고 CloseTransport 오류를 종료 코드로 보고.
- DPU backend registry의 slot 키를 `(acceptor owner, slot)`로 변경.
  다른 worker의 같은 slot 번호를 가진 backend가 잘못 삭제되는 것을 방지했다.
- 즉시 stack 재시작을 위해 포트 preflight를 Go listener와 같은 SO_REUSEADDR 조건으로 검사.

프로세스의 channel은 고정 worker 하나를 쓴다. VIP의 backend 선택은 proxy가 수행하지만,
HTTP/2 cache가 연결을 재사용하므로 **RPC별 replica round-robin/균등 분산을 보장하지 않는다.**
현재 mock policy의 VIP passthrough를 쓰는 bare-metal 구성이다.

## DPA polling 수정과 검증

초기 W1 실행에서 DPA fatal 및 native teardown 실패가 발생했다. 기존 descriptor-ring kernel은
idle/credit 대기까지 무한 polling하여 하나의 activation을 끝내지 않았다.
DOCA는 kernel 실행 시간 제한을 두며 무한 실행은 context fatal을 일으킬 수 있다고 명시한다.
[DOCA DPA 3.5 documentation](https://networking-docs.nvidia.com/doca/archive/3-5-0/doca-dpa).

실제 SDK getter는 `max_kernel_run_seconds=12`를 반환했다. 로그에서 첫 flow 시작부터
fatal 보고까지 약 120초였으므로 **getter 값과 장애 감지 시각을 같은 타이머로 단정하지 않는다.**
PC `0x40002280`은 당시 ELF의 ring control read 경로, RA는 producer DMA copy 경로였다.
무한 activation이 SDK 계약을 위반한다는 점을 고쳤으며, 재현 경로에서 이후 fatal은 없었다.

`DMA_POLL_ACTIVATION_BUDGET=65536`에 도달하면 consumer cursor와 누적 dma_submitted를
저장하고 `doca_dpa_dev_thread_retrigger()`로 즉시 재실행한다. Idle, credit 부족,
연속 descriptor 처리도 예산에 포함한다. 일반 reschedule은 completion 없는 ring에서
계속 잠들 수 있어 사용하지 않았다. Per-flow thread/EU 할당 모델은 그대로다.
실제 loop를 SDK substitute로 컴파일하는 회귀 테스트는 idle/credit wait, resume/wrap,
staging backpressure, 누적 종료 fence를 검사한다.

## 실제 검증

- Host native library/Go adapter/10개 service binary, DPU transport/device/proxy 빌드 성공.
- Native 15개 테스트 + header/ABI + polling 회귀 테스트 통과.
  Go adapter race tests, DSB adapter tests, Rust dmesh-doca 14개, topology Python 4개 통과.
- TCP와 DMA W1/W2/W4 각각 9개 health RPC, seeded user login,
  8개 frontend HTTP 경로(검색/추천/로그인/예약/review/attractions), 64개 동시 검색 통과.
  상태 초기화 후 HTTP JSON을 TCP 결과와 비교했고 목록 순서는 정규화했다.
- 이미 취소/만료된 context의 Canceled/DeadlineExceeded 반환을 검사했다.
  서버에서 실행 중인 느린 RPC 취소나 tracing 전파를 별도로 검증한 것은 아니다.
- W1: **185초 무트래픽 후** 같은 검증과 모든 부하 시험 통과. W2/W4도 전체 suite 정상.
- W4 replica 기능 시험: frontend/search/profile 각각 2개, 총 13개 프로세스.
  두 frontend에서 8개 HTTP 경로를 호출했다. 동일 DPU proxy/context를 유지하고 host stack을
  종료·재시작한 두 실행 모두 기능 검증 및 종료 성공. 이는 replica 균등 부하 분산 측정은 아니다.
- 최종 TCP 9회 + DMA 27회 부하 측정에서 HTTP/socket 오류 0.
  Traffic와 별도로 모든 host/proxy 종료 상태를 확인했다.
  사용한 isolated Compose 프로젝트 `dpumesh-current`도 종료했고 볼륨은 삭제하지 않았다.

## 자원 확인

DPU 로그의 pool assign/release를 owner worker별로 재구성했다. Probe/reconnect도 포함한
최대 동시 flow이며, 서로 다른 worker의 최대값은 같은 시각일 필요가 없다.

| 구성 | 생성한 shared DPA context | 선할당 DPA threads | worker별 peak active flow | 종료 후 남은 flow |
|---|---:|---:|---|---:|
| W1 | 1 | 32 | [28] | 0 |
| W2 | 1 | 64 | [13, 15] | 0 |
| W4 | 1 | 128 | [7, 11, 6, 4] | 0 |
| replicas | 1 | 128 | [13, 6, 7, 13] | 0 |

기본 manifest 예산은 W1 `[27]`, W2 `[12,15]`, W4 `[6,11,6,4]`이며 worker당
2개 probe/reconnect 여유를 별도로 남긴다. 실제 probe가 W0의 peak를 1개 늘렸다.
Replica 예산은 `[13,6,8,13]`. 모든 실행에서 assign/release 수가 일치했다.
Context 공유가 thread/EU 한도를 제거하지는 않는다. 별도의 EU partition 변경은 하지 않았다.

## 성능 조건 및 결과

서비스당 replica 1개 + frontend 1개, GOMAXPROCS=4, 서비스 CPU 0–11,
wrk2 CPU 12–15/4 threads/64 HTTP connections. DPU worker는 CPU 15부터 역순 pin.
Plaintext, tracing sampling 0, backend pool=1/max=2. Host PF `0b:00.1`, DPU PF `03:00.1`.
별도 Mongo 5.0/cache 프로젝트를 사용하고 구성마다 reservation/cache 상태를 초기화했다.
`mixed-workload_type_1`의 요청 로직은 유지하고 frontend URL만 변경했다.

Offered 500/2000/5000 RPS별 15초 × 3회. 아래는 각 지표의 반복별 중앙값이다.
CPU는 100%=logical core 1개이며 host는 서비스 10개 합계, DPU는 proxy process 합계다.
**wrk2의 10초 calibration 이후 latency 표본이 약 5초뿐인 짧은 통합 평가**다.
장시간 최대 지속 처리량이나 latency SLO를 확정하는 시험이 아니다.

| 구성 | offered RPS | achieved RPS | p99 ms | host CPU % | DPU CPU % |
|---|---:|---:|---:|---:|---:|
| TCP | 500 | 500.50 | 5.70 | 158.9 | — |
| TCP | 2,000 | 1,996.71 | 4.40 | 516.5 | — |
| TCP | 5,000 | 4,908.43 | 5.15 | 836.9 | — |
| DMA W1 | 500 | 500.34 | 12.80 | 501.1 | 99.96 |
| DMA W1 | 2,000 | 1,994.21 | 31.26 | 828.8 | 99.97 |
| DMA W1 | 5,000 | 2,474.19 | 7,590.00 | 783.8 | 99.96 |
| DMA W2 | 500 | 500.43 | 6.94 | 480.9 | 199.83 |
| DMA W2 | 2,000 | 1,996.14 | 15.46 | 850.7 | 199.18 |
| DMA W2 | 5,000 | 4,422.04 | 1,940.00 | 880.3 | 198.77 |
| DMA W4 | 500 | 500.44 | 5.68 | 463.3 | 399.72 |
| DMA W4 | 2,000 | 1,996.44 | 12.73 | 850.5 | 398.72 |
| DMA W4 | 5,000 | 4,978.29 | 62.17 | 940.3 | 397.86 |

W1/W2의 offered 5k는 처리량 부족과 초 단위 대기시간이 발생한 과부하 조건이다.
오류가 없다는 사실을 offered rate 충족으로 해석하지 않는다. W4는 약 4,978 RPS를
처리했지만 p99는 약 62ms로 direct TCP보다 높았다. Direct TCP에는 L7 proxy가 없으므로
동등한 mesh 구현끼리의 성능 비교가 아니다. 각 shard CPU는 약 99–100%였으나
busy-poll의 idle spin을 포함하므로 유효 연산만의 병목 증거는 아니다.

## 범위와 재현 증거

이번 포팅/실장 검증은 W1/2/4까지다. W8/12/16, host-dpa, Kubernetes control plane,
동적 worker 이동, DPA thread multiplexing, per-endpoint 장기 부하, GC 및 DB/cache CPU
프로파일은 포함하지 않았다. 큰 core 수의 이득을 검증하려면 frontend replica와 worker 배치를
별도 축으로 확장해야 한다. 전체 애플리케이션을 zero-copy로 바꾼 것으로 해석하지 않는다.

DPUMesh base `b60712998e8b0747fb62f2195716aeeeab6faffb`, proxy base
`c09915bfb38cbb8401b4d9136194923a6d23edd2` + 보존된 기존 변경 및 이번 working-tree 변경.
최종 binary hashes, image IDs, generated go.mod/go.sum/config, source diff와 새 파일은 raw bundle에 있다.
TCP와 최종 DMA의 10개 service 실행 파일 해시는 동일하다. DPA kernel 수정으로 native library와
probe는 재빌드했으며 TCP 기준에서는 해당 DMA device path를 사용하지 않는다.

원본 host 실행 경로: `/tmp/dsb-current/tcp-baseline`, `dma-watchdog-w{1,2,4}`,
`replicas-c`, `replicas-d`. DPU 실행 경로: `/tmp/dsb-port/dpu-watchdog-w{1,2,4}`,
`dpu-replicas-final`. 실패한 초기 실행 및 crash는 별도 보존하고 headline 수치에서 제외했다.
특히 수정 전 `dma-final-w1`의 fatal/종료 실패 데이터는 최종 DMA 결과에 섞지 않았다.

Raw archive: `bench-results/2026-09-29_deathstarbench-current-api-raw.tar.gz` (git ignored).

Archive SHA-256: `51d1119905f7a042549a7d95228444dac58d0fabe280e6b891ecdcf104844b6a`.
