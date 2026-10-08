# DSB: single wrk / 10 threads / 1,000 connections / NodePort — 2026-09-29

요청한 단일 wrk 구성으로 측정을 완료했다. **NodePort를 통해 DPUMesh frontend 8개로 1,000개 TCP 연결이 분산되는 것을 실측으로 확인했다.** 18K 목표 부하의 처리량은 busy 17,626 / event 17,623 req/s로 거의 같았다. 그러나 p99는 56.45 / 229.25ms로 차이가 남는다. 24K에서는 busy 22,914 / event 21,603 req/s로 event가 **5.72% 낮고**, DPU proxy CPU는 15.25 / 13.06코어로 **14.37% 적었다**. 이는 3회 관측 중앙값이며 통계적 동등성 검정은 아니다.

14K·18K·24K 정규 trial에서는 HTTP non-2xx와 socket error가 모두 0이었다. 저부하 6K에서는 busy recovery, event warmup/recovery에 각각 timeout 1건이 기록됐다. 이 구간을 결과에서 제외하지 않았다. 원인별 socket 로그와 내부 RPC counter를 함께 보존했으며, 전체 실행을 무오류라고 표현하지 않는다.

## 조건 및 경로

- r4에서 wrk 프로세스 **1개**: `-D exp -t10 -c1000 -d20s -L`, CPU 0–9. 사용자 요청의 mixed-workload_type_1.lua를 사용하며 URL만 실제 NodePort로 치환했다. 모든 정규 측정의 duration은 20초다.
- `10.8.8.1 (r4 ens23f0np0) → 10.8.8.2 (jet1 ens4f0np0):30253 → native DPUMesh frontend 8개(15000–15007) → DPU/backend`. 외부 HTTP/TCP 경로와 내부 DPUMesh gRPC 경로를 구분한다.
- 측정용 selectorless NodePort Service `test-bench/dsb-single-20260929`와 EndpointSlice 8개를 사용했다. Backend port가 서로 달라 slice를 분리했다. 기존 `frontend-hr-hotelres:30396` 및 기존 Kubernetes Pod 8개는 이 실험에 사용하지 않았다. SO_REUSEPORT 및 별도 사용자 공간 LB는 추가하지 않았다.
- Service는 sessionAffinity=None / externalTrafficPolicy=Cluster다. TCP 연결 단위로 endpoint가 선택되고 keep-alive 연결 내 요청은 같은 frontend에 간다. [Kubernetes Service 문서](https://kubernetes.io/docs/concepts/services-networking/service/)의 selector 없는 Service 및 NodePort 방식이다.
- 실험 전 jet1 ens4f0np0에 IPv4가 없었다. r4의 기존 정적 이웃 항목이 그 NIC MAC을 가리키는 것을 확인하고 10.8.8.2/24만 임시 추가했다. 양방향 ping/NodePort HTTP 응답을 검증했다. OVS flow, 링크 상태, EU/SF/sysctl은 변경하지 않았다.
- DPU 16 sharded workers, dpu-dma. Frontend workers 14/15/12/13/10/11/5/4, search 6–9. Host CPU 0–15 / GOMAXPROCS=4 / 앱 29개. Backend replicas와 Host/DPU 바이너리는 직전 frontend-8 실험과 동일하다.
- Busy → event 순서로 새 서비스 프로세스를 띄우고 모드 사이 DB/cache를 초기화했다. Idle 및 6K warmup 각 20초, 14K/18K/24K 각각 20초×3회, 6K recovery 20초. 측정 중 추가 profiling/빌드는 하지 않았다.
- 이전 frontend-8 실험은 wrk 8 processes / 총 16 threads / 256 connections / 30초 / 147.46.78.187 직접 접근이다. 여러 조건이 동시에 바뀌었으므로 이전 대비 차이를 NodePort 하나의 효과로 해석할 수 없다.
- 처리량은 wrk가 출력한 전체 실행 평균이다. p99는 이제 단일 wrk의 합산 histogram이며, 이전의 frontend별 p99 최댓값과 정의가 다르다. wrk2의 startup/calibration을 포함하는 20초 실행이라는 한계가 있다.

## 측정 결과 (3회 중앙값)

| 모드 | 목표 HTTP/s | 실제 HTTP/s | 반복 범위 | p99 ms | Host 앱 cores | DPU proxy cores |
|---|---:|---:|---|---:|---:|---:|
| busy | 14000 | 13702 | 13508–13703 | 34.08 | 12.09 | 15.30 |
| busy | 18000 | 17626 | 17565–17706 | 56.45 | 12.73 | 15.26 |
| busy | 24000 | 22914 | 22481–22924 | 2820.00 | 13.65 | 15.25 |
| event | 14000 | 13693 | 13637–13710 | 41.73 | 12.13 | 12.24 |
| event | 18000 | 17623 | 17455–17697 | 229.25 | 12.67 | 12.80 |
| event | 24000 | 21603 | 21596–21697 | 4530.00 | 13.37 | 13.06 |

## 실제 연결 분산 (18K 첫 trial 중 snapshot)

| Frontend port | Busy 연결 수 | Event 연결 수 |
|---:|---:|---:|
| 15000 | 119 | 130 |
| 15001 | 126 | 117 |
| 15002 | 120 | 140 |
| 15003 | 129 | 99 |
| 15004 | 130 | 131 |
| 15005 | 129 | 121 |
| 15006 | 129 | 136 |
| 15007 | 118 | 126 |

두 모드 모두 총 1,000개 연결이 8개 frontend에 도달했고, r4 프로세스 snapshot에서 wrk 1개와 10 worker threads + main thread를 확인했다. 연결 배분은 매 실행의 source port와 Service 선택에 따라 달라진다.

## CPU와 오류 / 수명 검증

| CPU | Worker | Busy physical % | Event physical % |
|---:|---:|---:|---:|
| 0 | 15 | 98.66 | 95.98 |
| 1 | 14 | 98.85 | 83.67 |
| 2 | 13 | 98.66 | 97.56 |
| 3 | 12 | 98.72 | 99.37 |
| 4 | 11 | 98.74 | 98.62 |
| 5 | 10 | 98.84 | 98.64 |
| 6 | 9 | 98.98 | 86.30 |
| 7 | 8 | 99.15 | 85.72 |
| 8 | 7 | 98.96 | 85.88 |
| 9 | 6 | 98.94 | 85.95 |
| 10 | 5 | 98.57 | 96.19 |
| 11 | 4 | 98.62 | 95.51 |
| 12 | 3 | 99.46 | 37.61 |
| 13 | 2 | 98.94 | 59.17 |
| 14 | 1 | 98.99 | 65.26 |
| 15 | 0 | 98.90 | 76.58 |

CPU는 user+system 프로세스 시간 합계(1 core=100%)다. Physical busy에는 커널/다른 작업도 포함되며 idle+iowait를 제외한다.

- busy: 모든 trial HTTP/socket 성공=False; Host 29 processes exit 0 / proxy exit 0. DPA: {"contexts": 1, "pool_assign": 115, "pool_release": 115}.
- 내부 RPC error counter (busy): idle=0, warmup=1, r14000-1=17, r14000-2=4, r14000-3=0, r18000-1=17, r18000-2=7, r18000-3=9, r24000-1=106, r24000-2=222, r24000-3=139, recovery=0. 외부 HTTP 성공과 구분한다. 기존 counter로 status code는 구분할 수 없다.
- busy: idle DPU proxy 15.75 cores; recovery 5846 HTTP/s, p99 17.55ms.
- busy: r4 wrk 평균 CPU 사용량의 trial 최댓값 1.34 cores / 할당 10 cores.
- event: 모든 trial HTTP/socket 성공=False; Host 29 processes exit 0 / proxy exit 0. DPA: {"contexts": 1, "pool_assign": 115, "pool_release": 115}.
- 내부 RPC error counter (event): idle=0, warmup=0, r14000-1=9, r14000-2=7, r14000-3=6, r18000-1=26, r18000-2=64, r18000-3=28, r24000-1=95, r24000-2=75, r24000-3=82, recovery=1. 외부 HTTP 성공과 구분한다. 기존 counter로 status code는 구분할 수 없다.
- event: idle DPU proxy 0.12 cores; recovery 5893 HTTP/s, p99 18.70ms.
- event: r4 wrk 평균 CPU 사용량의 trial 최댓값 1.30 cores / 할당 10 cores.
- 기록 PID birth/리소스 UID를 확인해 실험 프로세스, Compose 컨테이너, 측정용 NodePort와 EndpointSlice만 정리했다. 기존 frontend NodePort 30396은 유지했다.

## 실제 명령 (18K)

```sh
taskset -c 0-9 /tmp/dsb-regression-20260929/wrk -D exp -t10 -c1000 -d20s -R 18000 -L -s /tmp/dsb-single-20260929/event/r18000-1/nodeport.lua http://10.8.8.2:30253
```

Lua 파일은 workload 원본에서 base URL만 http://10.8.8.2:30253로 바꾼 사본이다. NodePort와 임시 IP는 실험 종료 시 정리하므로 재실행 시 새 서비스와 해당 주소 설정이 필요하다.

## 원본 자료

2026-09-29_dsb-single-wrk-nodeport-raw.tar.gz

SHA-256: `2e596cc85e2de6dc75593900d904d5b0fb21e234e0e4b870ad0ce364956391a9`
