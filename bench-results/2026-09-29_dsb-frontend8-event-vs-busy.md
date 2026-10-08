# DSB frontend 8 replicas: busy-poll / event — 2026-09-29

Frontend를 8개로 늘리니 **event 모드의 처리량이 개선됐지만 busy-poll과 같아지지는 않았다.** 18K 목표 부하 중앙값은 busy 17,612 req/s, event 16,606 req/s였다. 4-replica 대비 각각 **+4.85% / +16.65%**이며, event의 busy 대비 격차는 **−15.25% → −5.71%**로 줄었다. 이때 DPU proxy CPU는 busy 15.29코어, event 12.98코어다.

24K 과부하에서는 busy 18,128 req/s, event 16,578 req/s였다. 목표 부하만 낮아서 비슷한 처리량이 나온 것은 아니며, 두 모드의 포화 처리량에는 여전히 차이가 있다. 이 부하는 각 1회 측정이므로 반복 중앙값과 구분한다.

## 비교 조건

- Frontend만 4 → 8 replicas로 늘렸다. Backend replicas, Host/DPU binaries, DPU 16 sharded workers, dpu-dma, Host CPU 0–15/GOMAXPROCS=4를 유지했다. Host 앱 프로세스는 25 → 29개다.
- 기존 frontend worker 14/15/12/13을 유지하고 10/11/5/4를 추가했다. Search는 계속 worker 6–9다. Frontend와 Search를 같은 worker에 추가 배치하지 않았다. 물리 CPU는 worker i → CPU 15−i다.
- Backend replicas: reservation/rate/search 각 4, profile/geo/recommendation 각 2, user/review/attractions 각 1. Worker flow 예산은 최대 11개로, 32-slot 한도 안이다.
- r4 CPU 0–3에서 wrk2의 총 256 connections / 16 threads를 유지했다. Frontend당 설정은 기존 -t4 -c64에서 -t2 -c32로 바뀌었고 -D exp는 동일하다. 부하 발생기 프로세스 수는 4 → 8개다.
- Busy → event 순서. 각 모드 전 새 서비스 실행, 모드 사이 DB/cache 초기화. Idle 20초, 6K warmup 35초, 14K/18K 각 30초×3회, 24K 30초×1회, recovery 6K 20초. 정규 측정 중 profiling/빌드는 하지 않았다.
- 4-replica baseline은 직전 실험 결과다. 동일 바이너리 해시를 재확인했지만, 두 replica 구성을 무작위로 교차 실행한 실험은 아니다.
- CPU cores는 프로세스 user+system 시간 합계이며 1 core=100%. p99는 trial별 frontend p99 중 최댓값의 중앙값이며, 전체 요청을 합친 histogram의 p99가 아니다. 8 replicas에서는 최댓값을 취하는 frontend 수도 증가한다.

## 처리량 / CPU / 지연시간

| FE | 모드 | 목표 HTTP/s | 실제 HTTP/s | Host 앱 cores | DPU proxy cores | p99 ms | 반복 |
|---:|---|---:|---:|---:|---:|---:|---:|
| 4 | busy | 14000 | 13989 | 11.73 | 15.24 | 89.47 | 3 |
| 4 | busy | 18000 | 16797 | 12.06 | 15.18 | 3450.00 | 3 |
| 4 | busy | 24000 | 16986 | 12.11 | 15.19 | 9740.00 | 1 |
| 4 | event | 14000 | 13744 | 11.51 | 10.72 | 1630.00 | 3 |
| 4 | event | 18000 | 14235 | 11.59 | 10.79 | 7610.00 | 3 |
| 4 | event | 24000 | 14257 | 11.64 | 10.84 | 13030.00 | 1 |
| 8 | busy | 14000 | 13892 | 12.14 | 15.12 | 323.07 | 3 |
| 8 | busy | 18000 | 17612 | 12.51 | 15.29 | 2510.00 | 3 |
| 8 | busy | 24000 | 18128 | 12.55 | 15.13 | 9450.00 | 1 |
| 8 | event | 14000 | 13907 | 12.15 | 12.38 | 171.01 | 3 |
| 8 | event | 18000 | 16606 | 12.45 | 12.98 | 5190.00 | 3 |
| 8 | event | 24000 | 16578 | 12.46 | 12.84 | 10800.00 | 1 |

## Frontend 4 → 8 변화

| 모드 | 부하 | 처리량 변화 | 8 FE 처리량 범위 |
|---|---:|---:|---|
| busy | 14000 | -0.69% | 13864–13918 |
| busy | 18000 | +4.85% | 17581–17806 |
| busy | 24000 | +6.72% | 18128–18128 |
| event | 14000 | +1.18% | 13888–13917 |
| event | 18000 | +16.65% | 16506–16712 |
| event | 24000 | +16.28% | 16578–16578 |

## Idle / recovery

| 모드 | Idle Host cores | Idle DPU proxy cores | Recovery HTTP/s | Recovery p99 ms |
|---|---:|---:|---:|---:|
| busy | 0.63 | 15.80 | 5968 | 37.25 |
| event | 0.64 | 0.12 | 5966 | 24.40 |

## 18K 물리 CPU busy (3회 평균)

| CPU | Worker | Frontend 배치 | Busy % | Event % |
|---:|---:|---|---:|---:|
| 0 | 15 | yes | 98.75 | 95.08 |
| 1 | 14 | yes | 98.87 | 83.80 |
| 2 | 13 | yes | 98.78 | 95.76 |
| 3 | 12 | yes | 98.78 | 95.26 |
| 4 | 11 | yes | 98.78 | 98.80 |
| 5 | 10 | yes | 98.88 | 98.53 |
| 6 | 9 |  | 99.04 | 86.76 |
| 7 | 8 |  | 99.14 | 97.19 |
| 8 | 7 |  | 99.06 | 87.90 |
| 9 | 6 |  | 99.04 | 87.68 |
| 10 | 5 | yes | 98.64 | 94.53 |
| 11 | 4 | yes | 98.65 | 95.12 |
| 12 | 3 |  | 99.49 | 37.44 |
| 13 | 2 |  | 99.03 | 67.99 |
| 14 | 1 |  | 98.96 | 73.72 |
| 15 | 0 |  | 99.06 | 67.87 |

Frontend 추가로 기존 코어 0–3의 집중이 줄었고, event의 전체 proxy CPU는 이전 10.79 → 12.98코어로 늘면서 더 많은 요청을 처리했다. Event에서도 16코어 모두가 100% busy인 것은 아니다. 이번에는 profile과 frontend가 함께 배치된 코어 4/5(worker 11/10)가 98.80%/98.53%로 포화에 가까웠다. 특정 작업의 병목이라고 확정하려면 별도 profiling이 필요하다. Host 앱 CPU도 4-replica event의 11.59 → 12.45코어로 늘었다.

18K p99는 busy 3.45 → 2.51초, event 7.61 → 5.19초로 개선됐지만 여전히 포화 부하의 큰 대기시간이 남는다. 14K에서는 두 모드의 처리량이 약 13.9K로 비슷했고 event의 p99가 171ms, busy가 323ms였다. 목표 부하와 지연시간 조건을 함께 보고 판단해야 한다.

## 검증 / 오류 / 정리

- busy: RPC 사전 검증 통과; 모든 trial의 HTTP/socket 성공=True. Host 29 processes exit 0, proxy exit 0. DPA lifecycle: {"contexts": 1, "pool_assign": 115, "pool_release": 115}.
- 내부 RPC error counter (busy): idle=0, warmup=2, r14000-1=62, r14000-2=27, r14000-3=21, r18000-1=49, r18000-2=44, r18000-3=15, r24000-1=0, recovery=5. 외부 HTTP/socket 성공과 구분한다. Counter는 status code를 수집하지 않는다.
- busy r4 wrk 합계 CPU 사용량 최대 0.92 cores / 할당 4 cores.
- event: RPC 사전 검증 통과; 모든 trial의 HTTP/socket 성공=True. Host 29 processes exit 0, proxy exit 0. DPA lifecycle: {"contexts": 1, "pool_assign": 115, "pool_release": 115}.
- 내부 RPC error counter (event): idle=0, warmup=2, r14000-1=69, r14000-2=41, r14000-3=29, r18000-1=0, r18000-2=0, r18000-3=2, r24000-1=0, recovery=7. 외부 HTTP/socket 성공과 구분한다. Counter는 status code를 수집하지 않는다.
- event r4 wrk 합계 CPU 사용량 최대 1.03 cores / 할당 4 cores.
- 기록된 PID birth를 확인해 Host/DPU 프로세스와 r4 wrk, 해당 Compose 컨테이너가 모두 종료된 것을 확인했다. NIC/SF/EU/sysctl은 변경하지 않았다.
- Event 구현은 앞선 실험과 같다. PE notification 및 TX/RX credit wakeup을 사용하며 private DMA/setup 진행을 위한 1ms safety timer를 유지한다. Geo 공유 캐시 race 수정도 두 모드 모두 동일하다.

## 재현 설정

```sh
python3 integrations/deathstarbench/topology.py RUN_DIR \
  --workers 16 --cpus 0-15 --pci 0b:00.1 \
  --replicas reservation:4,rate:4,search:4,profile:2,geo:2,recommendation:2 \
  --frontends 8 --frontend-workers 14,15,12,13,10,11,5,4 \
  --audit-rpcs --poll-mode event
DSB_PROXY_POLL_MODE=event DSB_ROUTES_FILE=TOPOLOGY_ON_DPU \
  bash integrations/deathstarbench/proxy-start.sh 16 PROXY_RUN_DIR
```

Topology 생성은 Host에서, proxy 실행은 DPU에서 한다. 측정 스크립트 전체와 설정은 raw archive에 포함한다.

## 원본 자료

2026-09-29_dsb-frontend8-event-vs-busy-raw.tar.gz

SHA-256: `e387f15d90dddeea6dfd63bb80ddd84cb67233cf1e5eedf75c2b6933f353a074`
