# bench-results

One Markdown file per benchmark run, written by the `bench` subagent.
Filename: `YYYY-MM-DD_HHMMSS_<label>.md`. Each file records the environment
(git SHA, binary timestamps, DOCA version, node/PCI/EU-partition state, the
exact env + harness command) and the result (throughput, p50/p99, per-core
busy%, proxy L7 metrics, raw wrk2/h2load lines), then a one-line verdict.
A run whose conditions were not written down is not a result.

## 2026-10-01: HotelReservation 8-worker peak

[8-worker 재측정](2026-10-01_dsb-hotelreservation-8worker-peak.md): 동일 frontend 8/Host 앱 29/r4 단일 wrk 설정.
상한 확인 2회 중앙값 **15,626.16 HTTP req/s**, 직전 16-worker의 **66.9%**.
최대 단일 관측 15,725.61 req/s. P99 초과로 상승·반복 중단, 내부 RPC 오류는 별도 기재.

## 2026-10-01: 최신 dispatcher의 HotelReservation 최대 처리량

[DSB 재측정](2026-10-01_dsb-hotelreservation-current-peak.md): 16 DPU workers, frontend 8,
replica당 backend 하나, r4 단일 wrk2(-t10 -c1000), busy-poll/dpu-dma.
1K씩 ramp 후 29K offered 3회 중앙값 **23,357.72 HTTP req/s**, P99 **4.81초**.
30K에서 P99 5.30초로 상승 중단. Host physical 약 15/16 cores 사용.
상한 확인의 외부 HTTP/socket 오류 0, 내부 RPC 오류 총 3건. 전체 계수와 cleanup 기록 포함.

## 2026-10-01: 단일 client 64 flows 추가 측정

[16 workers / 16 replicas / 64 flows](2026-10-01_grpc-go-single-client-64flows.md):
한 client process에서 channel 2개, flow당 in-flight 64개. Same-worker **209.93K RPC/s**,
cross-worker **154.13K RPC/s**. 32-flow 재측정 및 channel 수 대조군 포함 18회 모두 성공.
64 flows는 32 flows 대비 처리량 향상 없이 P99가 증가했다.

## 2026-10-01: 단일 client + server replica scaling

[Replica scaling 측정](2026-10-01_grpc-go-single-client-replica-scaling.md): client process 하나,
server replicas = DPU workers, replica당 backend connection 하나, flow당 in-flight 64개.
30개 조건 × 3회 성공. **16 workers / 16 replicas / 32 flows에서 159,301.6 RPC/s**,
동일 worker/flow 수의 single-backend 대비 **5.67배**. Server별 처리 건수와 client 성공 건수 일치,
769개 DPA flow 회수 및 종료 후 DPA processes=0 확인.

## 2026-09-30: 단일 gRPC-go process의 flow/worker scaling

[성능 측정](2026-09-30_grpc-go-single-process-flow-scaling.md): client process 하나,
flow당 in-flight RPC 64개, DPU 1/2/4/8/12/16 workers, worker당 client flow 최대 4개.
Backend connection 하나 공유. 30개 조건 × 3회 성공, 최고 중앙값 **33,044.2 RPC/s**
(12 workers / 16 flows). Worker별 CPU 및 반복 원본 포함, RPC 오류·재연결 0.

## 2026-09-29: gRPC-go 16 ARM core 재측정

[현재 build, dpu-dma W16/K4](2026-09-29_grpc-go-64b-w16-k4-current.md): 64B echo,
connection당 동시 RPC 64개. 3/3회 성공, 중앙값 **200,563.5 RPC/s**로 이전
200,568.6 RPC/s와 사실상 동일. Shared DPA context 1개, peak 128 flows,
정상 종료 및 종료 후 DPA processes=0 확인.

## 2026-09-29: HotelReservation current API port

[Replica 분산 결함 진단](2026-09-29_deathstarbench-replica-audit.md): 4 ARM / 서비스별
4 replicas에서 DMA 업무 RPC는 마지막 replica 하나에 집중되고 direct TCP는 균등 분산됨을
실제 RPC 계수로 확인했다. 아래의 기능 검증은 replica scaling 검증을 의미하지 않는다.

[구현 및 실제 검증 결과](2026-09-29_deathstarbench-current-api.md): sharded busy-poll
dpu-dma W=1/2/4와 direct TCP, 36회 mixed-workload 측정. 185초 idle 후 재개,
13-process replica 구성의 동일 proxy 재사용 및 정상 종료를 확인했다.
W1/W2의 offered 5k RPS는 과부하 조건이며 최대 처리량으로 해석하지 않는다.

## 2026-09-25–26: gRPC-go 64B echo와 DPA 자원 검증

이번 기록은 보고서, 반복별 CSV, 그림과 작은 결과 JSON만 저장소에 포함한다.
실패와 미완료 조건도 결과의 일부이며, 초기화 실패를 0 RPC/s로 집계하지 않는다.

| 보고서 | 조건 | 상태와 해석 |
| --- | --- | --- |
| [1 ARM core, 두 reverse mode](2026-09-25_grpc-go-64b-1core.md) | conn=1–4, **전체 동시 RPC 64개** | 24회 유효 측정. 중단 실행은 제외했고, 비정상 중단 후 native QP 정리 지연은 미해결로 기록했다. |
| [1 ARM core, dpu-dma P4/C256](2026-09-26_grpc-go-64b-1core-p4-c256.md) | conn=4, **connection당 동시 RPC 64개** | PASS, 3/3회. 앞선 총 동시 요청 64개 조건과 구분한다. |
| [dpu-dma ARM scaling](2026-09-26_grpc-go-64b-arm-scaling.md) | W=1/2/4/8/12/16, K=1/2/4 | INCOMPLETE, 51/54회. 기존 EU partition 상태에서 W16/K4는 preflight 실패했다. |
| [dpu-dma W16/K4 재실험](2026-09-26_grpc-go-64b-w16-k4-no-eu-partitions.md) | EU partition 0개, DPU에 전체 190 EU 가용 | 3/3회 성공, 중앙값 200,568.6 RPC/s. 기존 suite와 EU 배치가 다르다. |
| [host-dpa ARM scaling 및 모드 비교](2026-09-26_grpc-go-64b-host-dpa-arm-scaling.md) | W=1/2/4/8/12/16, K=1/2/4 | INCOMPLETE, 34/54회. W8/K2는 정상 종료 1회뿐이고 W12/16은 process 생성 실패했다. |
| [host-dpa PF0/PF1 분산](2026-09-26_grpc-go-64b-host-dpa-pf01.md) | W=9/12/16, K=1/2/4 | INCOMPLETE, 5/27회 유효. W9/K2는 teardown 실패, W9/K4는 2/5회만 정상 종료했다. PF 분산도 W12/16의 process 한도를 해결하지 못했다. |
| [DPA process 공유 한도](2026-09-26_dpa-process-limit.md) | DPU 1 + host 27, 슬롯 해제 후 양쪽에서 재생성 | 현재 장비의 동시 base process 합계 28개를 확인했다. Application DPA thread 없이 시험했다. |
| [DPU-local SF standalone 시험](2026-09-26_dpu-local-sf-dpa-test.md) | 기존 SF에 임시 EU 8개 배정 | Standalone `doca_dpa_start()` 실패. SF의 DPA 사용 불가나 28개 제한 검증으로 해석하지 않는다. |
| [DPU-local SF extended 시험](2026-09-26_dpu-local-sf-extended-test.md) | PF base → SF extension, 64B heap 할당·해제 | 성공 및 정리 확인. SF datapath/EU scheduling이나 28개 경계의 추가 생성 시험은 아니다. |

W는 DPU ARM worker 수, K는 worker당 client connection 수다. Scaling 실험은
connection당 동시 RPC 64개이며 backend pool도 K개다. Mode 비교는 EU 배치가
다른 실험 사이의 참조 비교다. Busy polling에 의한 CPU 100%만으로 ARM 연산
병목을 확정하지 않는다. 상세 조건·정상 종료 여부·artifact SHA는 각 보고서에 있다.

## 자료 보존

2026-09-29 [4-core CPU 미포화 조사](2026-09-29_grpc-go-4core-bottleneck.md)는
host cores/process 2/4/8/16 대조군과 DPU/host sampling, scheduler 시간, HTTP/2 옵션을 확인했다.
Host core 증설의 이득은 거의 없고 DPU의 non-runnable 대기가 관찰됐다. 종료 panic과 과도한 trace overhead로 실패한 진단 run도 구분해 기록했다.
보고서/CSV/JSON은 Git에 포함하고 raw archive 및 실패한 대용량 trace 디렉터리는 로컬에 보존한다.

2026-09-28 [4-core in-flight RPC sweep](2026-09-28_grpc-go-64b-4core-inflight.md)은
busy-poll, Driver 1개, connection 4개에서 connection당 동시 RPC 64–1,024개를 측정했다.
15회 모두 통과했지만 proxy CPU는 약 334%/400%, 물리 core별 약 87%에서 정체했다.
보고서/CSV/작은 JSON/PNG는 저장소에 포함하고 `2026-09-28_grpc-go-64b-4core-inflight-raw.tar.gz`는 로컬에 보존한다.

2026-09-28 [non-sharded busy-poll 성능 평가](2026-09-28_grpc-go-64b-unsharded-busy-poll.md)는
host client/server 각 1 process, Comch Driver 1개로 DPU ARM cores 1/2/4와
client connections 1/2/4를 측정했다. 27회 모두 정상 종료했고 64B echo 5,413,470개를 검증했다.
보고서/CSV/작은 JSON/PNG는 저장소에 포함하고 `2026-09-28_grpc-go-64b-unsharded-busy-poll-raw.tar.gz`는 로컬에 보존한다.

2026-09-28 후속 [non-sharded 검증](2026-09-28_grpc-go-unsharded-validation.md)은
현재 global DPA context 구현에서 dpu-dma gRPC-go를 실행한 기록이다.
Busy-poll 1/2 Drivers는 통과했으나 event 구성에서 DPA fatal을 관찰했다.
작은 결과 JSON은 저장소에 포함하고 `2026-09-28_grpc-go-unsharded-validation-raw.tar.gz`는 로컬에만 보존한다.

보고서의 상대 링크는 Git에 포함된 파일을 가리킨다. 아래 원본 파일은 이 작업에서
삭제하거나 수정하지 않고 **로컬에만 보존하며 Git에는 포함하지 않는다**.

- 대형 결과 JSON: `2026-09-26_grpc-go-64b-arm-scaling.json`,
  `2026-09-26_grpc-go-64b-w16-k4-no-eu-partitions.json`,
  `2026-09-26_grpc-go-64b-host-dpa-arm-scaling.json`,
  `2026-09-26_grpc-go-64b-host-dpa-pf01.json`,
  `2026-09-26_dpa-process-limit.json`.
- 로그·실행 스크립트 archive:
  `2026-09-26_grpc-go-64b-host-dpa-arm-scaling-raw.tar.gz`,
  `2026-09-26_grpc-go-64b-host-dpa-pf01-raw.tar.gz`,
  `2026-09-26_dpa-process-limit-raw.tar.gz`,
  `2026-09-26_dpu-local-sf-dpa-test-raw.tar.gz`,
  `2026-09-26_dpu-local-sf-extended-test-raw.tar.gz`.
- W16/K4 재실험의 로컬 evidence 디렉터리:
  `2026-09-26_grpc-go-64b-w16-k4-no-eu-partitions-evidence/`.
- 과거 비교에 사용한 로컬 보고서:
  `2026-09-10_grpc-echo-1core-selective-h2-AB.md`.

위 경로는 모두 이 `bench-results/` 디렉터리 기준이다. 저장소 checkout만으로는
전체 원본 로그를 받을 수 없다. 작은 JSON 네 개는 1-core 두 실험과 SF 두 실험의
결과를 위해 포함한다. CSV는 저장소용으로 CRLF 줄바꿈만 LF로 정규화했고,
파싱한 모든 cell 값이 원본과 동일함을 확인했다. PNG와 작은 JSON의 내용은 그대로다.

보고서의 `/tmp/...`와 실행 명령은 **실험 당시의 로컬 경로**다. 공개 자료 정리 시점에
이전 1-core/scaling/process-limit 실험의 `/tmp` 디렉터리는 없었고, 위 archive와
결과 파일이 남아 있었다. SF 두 시험의 `/tmp` 디렉터리는 존재했지만 임시 경로의
지속성을 보장하지 않는다. 원본이 없는 경로를 클릭 가능한 저장소 링크로 표시하지 않는다.

2026-09-29 [DSB replica routing 구현/검증](2026-09-29_deathstarbench-replica-routing.md):
service VIP/endpoint 분리, per-RPC 분산, worker별 routing과 endpoint H2 공유를 구현했다.
단일 연결 분산·제거/복구·process 재시작 및 4/8/16 workers × 1/2/4 replicas 측정이다.
작은 JSON/CSV/보고서는 Git 대상이며 raw archive는 로컬에만 보존한다.

2026-09-29 [DSB Host CPU 및 r4 부하 발생기 분석](2026-09-29_deathstarbench-hostcpu-r4.md):
TCP/DMA replica 1·4 비교, idle CPU, perf, polling 설정 대조 실험이다.
4K/8K는 목표 부하이며 최대 처리량 측정이 아니다.

2026-09-29 [DSB 이전 구현 비교 및 성능 수정](2026-09-29_dsb-hotelres-performance-fix.md):
9월 3일과 같은 replica/workload 조건을 복원하고 Host polling 및 DPU worker 배치
회귀를 분리했다. 최종 14K/18K 반복 측정, 24K 과부하 진단, 실제 연결 lifecycle
검증과 CPU 분석을 포함한다. MD/JSON/CSV는 Git 대상, 원본 archive는 로컬 보존이다.

2026-09-29 [DSB DPU Arm core 사용률 및 남은 병목](2026-09-29_dsb-dpu-arm-bottleneck.md):
물리 코어 busy와 proxy worker CPU를 구분하고, 현재 바이너리의 idle/load perf로
frontend/search의 L7 처리 집중과 busy-poll 고정 비용을 확인했다. 진단 INFO 로그의
tracing 영향을 별도 표시하고 기존 warn 설정으로 재측정했다.

2026-09-29 [DSB event-driven / busy-poll 비교](2026-09-29_dsb-event-vs-busy.md):
PROGRESS_ALL notification 처리와 RX credit wake를 수정하고 동일 바이너리의
16-worker DSB를 r4 부하로 비교했다. Event 모드는 idle CPU를 크게 낮췄지만
18K 부하 처리량이 15.25% 감소했다. geoindex 공유 캐시 race 수정 및
연결 재생성 검증을 포함하며, raw archive는 로컬에 보존한다.

2026-09-29 [DSB frontend 8 replicas busy/event 비교](2026-09-29_dsb-frontend8-event-vs-busy.md):
동일 바이너리와 총 wrk 동시성을 유지하고 frontend를 4→8개로 늘렸다.
18K 목표 부하에서 busy 17.61K/event 16.61K req/s로 개선됐으며,
event의 busy 대비 처리량 격차는 15.25%에서 5.71%로 줄었다.
코어별 사용률과 lifecycle 검증을 포함한다.

2026-09-29 [DSB 단일 wrk / 1,000 connections / NodePort 비교](2026-09-29_dsb-single-wrk-nodeport.md):
r4의 wrk 1 process, 10 threads, 20초, 10.8.8.2 경로를 사용했다.
측정용 NodePort가 native DPUMesh frontend 8개로 TCP 연결을 분산하는 것을
확인하고 busy/event를 비교했다. 18K 처리량은 비슷하고 24K에서 event가
5.72% 낮았다. 저부하 timeout 3건과 전체 원본 로그를 포함한다.

2026-09-29 [DSB peak throughput 및 병목](2026-09-29_dsb-peak-throughput-bottleneck.md):
단일 wrk/NodePort/FE8/16 DPU workers에서 24–40K 부하를 3회씩 반복하고
60초 재확인과 Host/DPU perf를 수집했다. 기본 peak busy 23.57K/event 22.59K req/s,
Profile worker 배치 대조 실험으로 event 23.45K(+3.83%)를 확인했다.
과부하 지연, 코어별 사용률, 내부 RPC 오류 및 원본 archive를 함께 기록한다.

2026-09-29 [DSB busy-poll 1K 단위 부하 증가](2026-09-29_dsb-busy-load-ramp.md):
단일 wrk `-t10 -c1000 -d20s`/NodePort/FE8/16 DPU workers에서
1K부터 1K씩 올려 28K의 P99 5,150ms에서 중단했다. 직전 27K는
실제 23,323 req/s, P99 4,280ms였다. 전 부하 결과와 저부하 timeout을 기록한다.

2026-10-03 [gRPC-go worker-local lazy backend 검증 및 성능](2026-10-03_grpc-worker-local.md):
단일 client process, 64B echo, flow당 64 in-flight RPC로
1/2/4/8 DPU workers × replicas 1/W × client flows/worker 1/2/4를 측정했다.
최종 동일 빌드 63회에서 RPC 오류 0, 약 9,400만 요청 전달의 worker/thread 불일치 0.
측정 범위 최고 중앙값은 8 workers/1 replica/16 flows의 59.29K RPC/s였다.
초기 backend 생성 timeout 수정, server 재시작/한도 초과 검증과 CPU/latency를 함께 기록한다.

Additional flow-scaling and follow-up results (original experiment dates):

- [2026-09-30_grpc-go-single-process-flow-balancing](2026-09-30_grpc-go-single-process-flow-balancing.md)
- [2026-09-30_grpc-go-single-process-flow-scaling](2026-09-30_grpc-go-single-process-flow-scaling.md)
- [2026-10-01_dsb-hotelreservation-8worker-peak](2026-10-01_dsb-hotelreservation-8worker-peak.md)
- [2026-10-01_dsb-hotelreservation-current-peak](2026-10-01_dsb-hotelreservation-current-peak.md)
- [2026-10-01_grpc-go-single-client-64flows](2026-10-01_grpc-go-single-client-64flows.md)
- [2026-10-01_grpc-go-single-client-replica-scaling](2026-10-01_grpc-go-single-client-replica-scaling.md)
- [2026-10-03_grpc-onecore-comparison](2026-10-03_grpc-onecore-comparison.json)
- [2026-10-05_grpc-direct-replica-1core](2026-10-05_grpc-direct-replica-1core.json)
- [2026-10-05_grpc-multiprocess-w16-k4-recheck](2026-10-05_grpc-multiprocess-w16-k4-recheck.json)
