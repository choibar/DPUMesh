# DeathStarBench Host CPU / r4 load generator (2026-09-29)

DSB HotelReservation 기준 커밋 `312855ac450bcd35550a482517fd47e093a26ac4` + 현재 DPUMesh 포팅.
Host jet1 CPU 0–15에 Go 서비스, Host에 DB/cache, DPU에는 dpu-dma sharded busy-poll proxy 16 workers, r4 CPU 0–3에 wrk2를 배치했다.
HTTP는 r4→Host Ethernet 직접 연결이다. 서비스당 replica 1/4, frontend 4, frontend당 HTTP 연결 64, GOMAXPROCS=4, tracing=0.
각 부하점 30초 × 3회의 중앙값. **4K/8K는 목표 부하이며 최대 처리량 탐색이 아니다.**
CPU 100%=1코어, 아래 CPU는 앱 프로세스 user+system 합계이며 DB/cache와 wrk는 제외.
p99는 frontend별 p99의 최댓값을 각 trial에서 구한 뒤 중앙값을 취했다. 전체 요청 histogram의 p99가 아니다.

| 조건 | Idle cores | 목표 HTTP/s | 실제 HTTP/s | Host cores | CPU µs/HTTP | p99 ms |
|---|---:|---:|---:|---:|---:|---:|
| tcp-r4 | 0.031 | 4000 | 3954 | 9.92 | 2509 | 14.27 |
| tcp-r4 | 0.031 | 8000 | 7903 | 13.06 | 1653 | 19.36 |
| dmesh-r4 | 9.913 | 4000 | 3952 | 14.79 | 3743 | 48.32 |
| dmesh-r4 | 9.913 | 8000 | 7811 | 15.12 | 1935 | 648.19 |
| tcp-r1 | 0.007 | 4000 | 3926 | 7.00 | 1782 | 13.53 |
| tcp-r1 | 0.007 | 8000 | 7904 | 11.37 | 1438 | 14.41 |
| dmesh-r1 | 2.542 | 4000 | 3953 | 11.66 | 2950 | 60.00 |
| dmesh-r1 | 2.542 | 8000 | 7802 | 12.05 | 1548 | 226.18 |
| spin0 | 9.918 | 4000 | 3953 | 12.24 | 3096 | 19.28 |
| spin0 | 9.918 | 8000 | 7845 | 12.66 | 1613 | 122.75 |
| spin0-tick1000 | 0.909 | 4000 | 3953 | 6.91 | 1749 | 26.45 |
| spin0-tick1000 | 0.909 | 8000 | 7901 | 10.27 | 1300 | 147.07 |

`spin0`과 `spin0-tick1000`은 R4이며 각각 `DPUMESH_SPIN_US=0`, 후자는 추가로 `DPUMESH_TICK_US=1000`인 진단 실험이다. 제품 기본값을 바꾸지 않은 상태의 결과다.

## 원인과 근거

- R4 기본 DMA는 idle에도 9.91코어, R1도 2.54코어를 사용한다. TCP는 각각 0.031/0.007코어다.
- `src/core/dmesh_core.c`: 1ms spin 동안 eventfd가 readable이고, 이후에도 live stripe의 fallback timer가 50µs마다 깨어난다. drain은 32 stripes를 검사한다.
- `src/core/carrier.c` / `src/transport/host/channel.c`: live flow마다 공유 Comch control PE를 반복 progress한다.
- `integrations/grpc/go/dmesh.go`: cgo ppoll 대기는 OS thread를 점유하며, net.Conn Read/Write에는 Go/native 버퍼 copy가 남아 있다. DMA 전송 자체의 zero-copy가 gRPC 앱 전체의 zero-copy를 의미하지 않는다.
- 별도 99Hz perf 진단의 기본 DMA 부하 self sample: drain_rev_rings_span 16.71%, native DPUMesh 전체 26.08%, libc 11.15%, kernel 28.46%. pthread lock/unlock은 합계 7.38%.
- DMA idle perf stat 20초에서 context switch 19,482,040회(약 974K/s wall time). perf가 출력하는 K/sec는 task-clock 분모이므로 이 값과 다르다.
- spin0/tick1000 진단의 native sample은 5.73%, drain은 1.10%로 감소했다. sample 비중은 on-CPU self time이며 대기시간 비중이 아니다.
- TCP 부하에서는 Go runtime 39.58%, kernel 35.56%, gRPC/H2 10.36%였다. TCP idle은 샘플이 없을 정도로 CPU 사용이 작았다.
- 36개 정규 trial 모두 wrk HTTP/socket 오류 0. 내부 RPC error counter는 일부 증가했다(세부 JSON). 종료 경계 취소 가능성이 있지만 개별 원인은 이 계측만으로 확정하지 않았다.
- load generator의 CPU 0–3 중 가장 바빴던 코어도 trial 평균 35.9% 미만, Host Ethernet TX는 31.4Mbps 미만/1Gbps로 부하 발생기나 외부 링크의 포화 증거는 없다.

## 해석과 한계

현재 Host 부하의 큰 원인은 polling이다. 1ms 고정 tick은 CPU를 줄이지만 p99가 TCP보다 나쁘므로 최종 해결책으로 확정하지 않았다. Go runtime에서 대기하고, idle polling 빈도와 공유 control PE 중복 progress를 줄이는 방향을 우선 검토한다.
TCP는 같은 Host의 loopback이고 DMA는 DPU를 왕복한다. protobuf/gRPC/H2 처리는 Host에 남으므로 no-mesh보다 Host CPU가 항상 작다는 보장은 없다.
CPU 변동이 커서 JSON/CSV에 min/max도 보존했다(예: TCP R4 4K CPU 8.23–9.98코어). DB/cache CPU는 별도 기록했다.
프로파일 trial은 처리량 통계에서 제외했다. Host/r4 clock offset은 +17.70ms, 최소 RTT 측정 불확도 ±0.113ms. CPU sample 주기는 0.5초이고 부하 구간은 30초다.
과거 local-wrk 결과와는 부하 발생 위치뿐 아니라 Host 앱 CPU mask가 12→16코어로 달라져, 향상을 r4 이동 한 요인에만 귀속할 수 없다.
초기 시도는 TCP user-1 bind 실패로 계측 전 중단했다. ephemeral range와 서비스 포트가 겹쳐 startup listener 포트를 임시 예약하도록 하네스를 수정했으며 sysctl은 변경하지 않았다.

## 보존 자료

이 보고서의 JSON/CSV, `/tmp/dsb-hostcpu-r4-v2` 및 `/tmp/dsb-hostcpu-r4-tuning`에 원본 로그/CPU samples/perf/실행 스크립트가 있다.
테스트 앱 186개, DPU 역할 프로세스 16개, r4 wrk 프로세스 180개의 PID birth 기록으로 종료를 확인했다.
DPU 네 실험 모두 shared context 1개, DPA thread 할당/반납 각각 57/57,116/116,115/115,115/115. 격리 Compose는 제거했으며 NIC/SF/EU 설정은 변경하지 않았다.
후속 이전 버전 비교/수정은 별도 regression 보고서로 기록한다.

후속 결과: [이전 구현 비교 및 성능 수정](2026-09-29_dsb-hotelres-performance-fix.md).
원본 archive: `2026-09-29_deathstarbench-hostcpu-r4-raw.tar.gz` (로컬, Git 제외).
SHA-256: `3d08c15464548da04f5b0545aea81394306d3cf02fa4b72deb8ab984e4d15d08`.
archive에서는 대용량 symbol filesystem만 제외했으며 perf 원본과 분석 결과를 포함한다.
