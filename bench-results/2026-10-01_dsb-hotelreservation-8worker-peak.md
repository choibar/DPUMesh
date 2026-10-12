# HotelReservation: 8 DPU workers peak throughput

2026-10-01. Offered **18,000 HTTP req/s**, 20초 × 2회에서 실제 처리량 중앙값 **15,626.16 HTTP req/s** (범위 15,575.79–15,676.53). P99 중앙값 **4,965.00ms**.

[직전 16-worker 결과](2026-10-01_dsb-hotelreservation-current-peak.md)의 23,357.72 req/s 대비 **66.9%**다. 전체 시도에서 최대 단일 관측값은 15,725.61 req/s (r19000)였다.

18K offered 첫 실행의 P99는 4,670ms였고 두 번째는 5,260ms였다. 두 번째가 5초 기준을 넘어서 **세 번째 반복은 하지 않았다**. 따라서 2회 중앙값이며, P99 중앙값 4,965ms만 보고 모든 반복이 기준을 만족했다고 해석하면 안 된다. 최고 단일 처리량 15,725.61 req/s 역시 P99 5,890ms와 내부 RPC 오류 28건을 포함한 HTTP 완료 처리량이다.

## 같은 설정과 변경점

- DPU-dma, sharded busy-poll, least-flows. DPU data workers만 16→8로 변경했다. Worker i는 ARM CPU15−i에 pin한다.
- Frontend 8, search/rate/reservation 각각 4, geo/profile/recommendation 각각 2, user/review/attractions 각각 1: Host process 총 29개. Host CPU0–15 공유, process별 GOMAXPROCS=4. Replica당 backend connection 하나. 동일 Host 앱/native library/proxy 바이너리를 사용했다.
- 8-worker 범위에 맞춰 Comch 접속점의 worker 번호를 조정했다. 실제 data-path owner는 dispatcher가 결정한다. 기존 frontend 접속점 번호는 modulo 8로 바꿨고 replica 수는 유지했다.
- r4 wrk 한 process, `-D exp -t10 -c1000 -d20s -L`, CPU0–9. 10.8.8.2의 새 임시 NodePort로 frontend 8개에 연결을 분산한다. 이전과 같은 mixed-workload_type_1 Lua이며 base URL만 변경했다.
- 별도 동일 이미지 Compose DB/cache를 초기화하고 모든 기능 및 TCP baseline 응답 일치를 확인했다. 5K offered 20초 warmup 후 10K부터 1K씩 상승했다. P99>5000ms에서 상승을 멈추고 그 이하에서 처리량이 가장 높았던 부하를 추가 2회 확인한다. 반복에서도 P99>5000ms이면 더 반복하지 않는다. 이전 16-worker 실험은 1K부터 시작했으므로 전체 warmup/ramp 이력은 다르다.
- Peak는 이 부하 범위·20초 관측의 완료 처리량이다. Offered rate를 지속적으로 소화한다거나 장시간 latency SLO를 보장한다는 뜻은 아니다.

## 측정값

| Trial | Offered req/s | 실제 req/s | P99 ms | HTTP/socket 정상 | 내부 RPC 오류 |
|---|---:|---:|---:|---|---:|
| warmup | 5000 | 4790.77 | 15.14 | False | 0 |
| r10000 | 10000 | 9734.88 | 31.39 | True | 0 |
| r11000 | 11000 | 10752.85 | 37.25 | True | 5 |
| r12000 | 12000 | 11733.39 | 46.53 | True | 9 |
| r13000 | 13000 | 12653.03 | 60.61 | True | 18 |
| r14000 | 14000 | 13622.48 | 92.42 | True | 34 |
| r15000 | 15000 | 14659.47 | 171.01 | True | 71 |
| r16000 | 16000 | 15434.88 | 1560.00 | True | 205 |
| r17000 | 17000 | 15595.87 | 3560.00 | True | 135 |
| r18000 | 18000 | 15676.53 | 4670.00 | True | 66 |
| r19000 | 19000 | 15725.61 | 5890.00 | True | 28 |
| confirm-r18000-2 | 18000 | 15575.79 | 5260.00 | True | 58 |

상승 중단: offered 19,000, P99 5,890.00ms.

확인 2회 외부 HTTP/socket 정상=True, 내부 RPC 오류 합계=124. HTTP 2xx만으로 내부 RPC 무오류를 가정하지 않는다. 전체 계측 자료는 JSON에 보관했다.

## 종료 및 원본

Host 앱 29개 exit 0, proxy 정상 종료, DPA thread 할당/회수 **98/98**, 종료 후 DPA processes=0. 임시 Compose/NodePort/EndpointSlice/10.8.8.2 IP를 정리했고 기존 frontend Service UID/spec는 유지됐다.

[JSON](2026-10-01_dsb-hotelreservation-8worker-peak.json), [CSV](2026-10-01_dsb-hotelreservation-8worker-peak.csv). Raw root: `/tmp/dsb-w8-peak-20261001`. Source와 바이너리 SHA-256, 실제 command, raw wrk 출력 및 CPU 계측 자료를 보존했다.

[Raw archive](2026-10-01_dsb-hotelreservation-8worker-peak-raw.tar.gz), SHA-256: `6398c962145d91cb8f33c3b5ae233e2cbab5279ea9ce6e8ae6ebbd688a98e8cd`.
