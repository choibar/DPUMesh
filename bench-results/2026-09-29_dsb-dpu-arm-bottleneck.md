# DSB DPU Arm core 사용률 및 남은 병목 — 2026-09-29

**물리 Arm core 16개는 모두 거의 100% busy였다. 다만 16개가 모두 유효 요청 처리로 포화됐다는 뜻은 아니다.** 기존 최종 18K 부하 3회에서 각 코어 평균 busy는 98.89–99.71%다. 앞선 보고서의 91–98%는 개별 proxy worker CPU 값이며 물리 코어 전체의 busy와 다른 지표다.

## 코어별 확인

기존 최종 구성: Host 25프로세스, DPU-DMA, sharded 16 workers, busy-poll, frontend workers 14/15/12/13, search workers 6/7/8/9. r4 wrk2 mixed workload, offered 18K, 30초 × 3회. 아래는 trial 평균의 산술평균이다.

| 물리 코어 | worker | 코어 busy % | worker CPU % | 코어 user % | 코어 system % |
|---:|---:|---:|---:|---:|---:|
| 0 | 15 | 98.91 | 95.43 | 89.14 | 9.66 |
| 1 | 14 | 98.95 | 94.90 | 88.17 | 10.78 |
| 2 | 13 | 98.91 | 95.41 | 89.25 | 9.66 |
| 3 | 12 | 98.96 | 95.34 | 89.77 | 9.19 |
| 4 | 11 | 98.89 | 95.75 | 77.68 | 21.21 |
| 5 | 10 | 99.16 | 91.34 | 73.42 | 25.74 |
| 6 | 9 | 99.22 | 96.07 | 83.60 | 15.62 |
| 7 | 8 | 99.22 | 95.89 | 83.14 | 16.07 |
| 8 | 7 | 99.18 | 96.27 | 83.95 | 15.22 |
| 9 | 6 | 99.20 | 96.53 | 83.40 | 15.80 |
| 10 | 5 | 99.52 | 97.67 | 74.90 | 24.63 |
| 11 | 4 | 99.71 | 98.26 | 76.88 | 22.83 |
| 12 | 3 | 99.57 | 97.31 | 77.61 | 21.96 |
| 13 | 2 | 99.26 | 96.26 | 78.96 | 20.29 |
| 14 | 1 | 99.11 | 96.39 | 80.21 | 18.90 |
| 15 | 0 | 99.19 | 96.26 | 79.73 | 19.45 |

busy는 `/proc/stat`의 idle+iowait를 제외한 값이다. worker CPU는 해당 스레드들의 user+system 시간을 wall time으로 나눈 값이다. 코어 busy에는 다른 작업과 커널 실행도 포함되므로 두 숫자를 같은 지표로 해석하지 않는다. 차이의 개별 프로세스별 원인까지 이 샘플로 분해하지 않았다.

무부하에서도 16코어 busy는 **99.95–100.00%**였다. 따라서 busy-poll에서 util=100%만으로 요청 처리 병목을 판단하면 안 된다.

## 현재 바이너리 추가 프로파일

동일 바이너리/replica/worker 배치에서 fresh isolated stack으로 idle 20초, warmup 6K 35초, offered 18K 45초를 실행했다. DPU proxy에 cycles 49Hz perf를 idle 12초 및 load 30초 수집했다. 아래는 기존과 같은 `LINKERD2_PROXY_LOG=warn` 실행의 worker별 self cycles 구성비다. syscall은 kernel 열 및 별도의 wrapper 함수에 나뉘므로 kernel 값만으로 총 syscall 비용이라고 해석하지 않는다.

| worker | 주된 프로세스 | HTTP/H2·proxy % | driver/control % | kernel % | tracing % |
|---|---|---:|---:|---:|---:|
| 0 | geo-0, reservation-1 | 16.2 | 15.8 | 18.2 | 0.5 |
| 1 | geo-1, reservation-2 | 15.0 | 15.2 | 21.7 | 0.6 |
| 2 | rate-0, reservation-3 | 13.4 | 19.4 | 19.5 | 0.5 |
| 3 | rate-1, review-0 | 5.5 | 26.8 | 23.6 | 0.5 |
| 4 | rate-2, attractions-0 | 7.6 | 22.8 | 21.7 | 0.4 |
| 5 | rate-3 | 6.4 | 20.1 | 22.5 | 0.5 |
| 6 | search-0 | 31.0 | 10.9 | 14.3 | 1.8 |
| 7 | search-1 | 30.0 | 9.9 | 14.3 | 1.5 |
| 8 | search-2 | 31.4 | 10.8 | 14.4 | 1.7 |
| 9 | search-3 | 31.9 | 10.3 | 14.3 | 1.5 |
| 10 | profile-0 | 13.1 | 19.1 | 21.6 | 0.2 |
| 11 | profile-1 | 16.1 | 15.2 | 20.7 | 0.3 |
| 12 | recommendation-0, frontend-2 | 49.7 | 1.0 | 7.9 | 2.5 |
| 13 | recommendation-1, frontend-3 | 50.6 | 1.0 | 9.5 | 2.1 |
| 14 | user-0, frontend-0 | 47.0 | 4.8 | 9.4 | 2.0 |
| 15 | reservation-0, frontend-1 | 50.0 | 1.2 | 8.8 | 2.5 |

이번 warn 진단은 16,575 req/s, Host 앱 11.97코어였으며 물리 코어 busy 범위는 98.48–99.49%였다. perf 및 45초 부하가 포함된 단일 진단이므로 이전 30초 비계측 3회 중앙값 17,060을 대체하는 결과로 쓰지 않는다.

HTTP/H2·proxy는 h2/http 및 Linkerd HTTP/stack/routing 관련 resolved 함수들의 self cycles 합계다. driver/control에는 `Driver::run`, `sessions_advance` 등이 들어가며 이를 전부 빈 polling으로 분류하지 않는다. unresolved 6.6–18.8%에는 vdso, libc와 DOCA SDK의 stripped 주소가 포함된다. raw CSV에 DSO와 symbol을 보존했다.

## 병목 판단

1. **가장 강한 근거는 DPU source worker의 L7 작업 집중이다.** frontend workers 12–15의 HTTP/H2·proxy 비중은 47–51%, search workers 6–9는 30–32%다. 반면 worker 3–5는 5–8%다. 단일 process channel에 들어온 요청의 수신 H2와 routing/policy 작업은 source worker에 남고, backend endpoint H2는 별도 owner worker가 처리한다. per-RPC replica 선택이 source worker CPU까지 분산해 주지는 않는다.
2. **busy-poll 고정 비용이 덜 바쁜 worker를 가린다.** worker 3은 driver/control 26.8%, kernel 23.6%이며 `sessions_advance`, `Driver::run`, `el0_svc`, `epoll_pwait`가 상위다. `Driver::run`은 busy-poll에서도 `yield_now()`로 Tokio에 양보한다. 현재-thread scheduler의 `park_yield()`는 timeout=0으로 IO driver를 poll하므로, busy-poll이어도 epoll/syscall 비용이 남는다. idle 100%와 이 프로파일은 코어가 덜 바빠 보이지 않는 이유를 설명한다.
3. **전체 Host CPU와 부하 발생기 용량 소진이 주원인이라는 근거는 약하다.** 기존 최종 Host 앱은 약 12.1/16코어, whole-host busy는 약 13.4/16이었다. 기존 r4 단일 코어 평균은 최대 21.74%, Ethernet TX는 최대 약 56.7Mbps/1Gbps였다. 다만 Host의 특정 poller/lock/단일 스레드 한계까지 총 CPU 값만으로 배제할 수는 없다.
4. **현재 증거의 한계.** source worker 배치만 바꿔 13.4K→16.8K가 된 이전 대조 결과와 이번 worker별 profile이 같은 방향이다. 따라서 다음 조사/최적화 우선순위는 source worker별 요청 처리 분산과 busy driver/scheduler 고정 비용이다. 특정 함수 하나를 제거했을 때의 향상이나 DMA/Comch credit stall의 부재까지 증명한 것은 아니다. 이 단계에서는 제품 코드를 변경하지 않았다.

## 계측 영향과 정리

처음에는 `warn,linkerd_app::dmesh=info`로 per-worker datapath stats를 켰다. 이 실행은 13,311 req/s로 낮아졌고 frontend의 tracing enter/exit 및 lock 비용이 커졌다. 이를 현재 warn 구성의 병목으로 오인하지 않도록 같은 바이너리로 warn 설정을 재측정했다. warn에서 tracing 비중은 frontend 약 2–3%이며 첫 INFO 진단의 큰 tracing 비용을 baseline 원인으로 사용하지 않았다. 두 진단은 별도 시작한 단일 실행이므로 정확한 인과 효과 크기 추정은 아니다.

INFO 진단에서는 DMA software pending/drop snapshot이 모두 0이었다. 하지만 처리량이 낮아진 계측 구성에서 나온 값이고 hardware in-flight/credit 전체를 나타내지 않으므로, 이를 17K에서 DMA 병목이 없다는 증거로 쓰지 않는다.

각 진단은 native close로 정상 종료했고 격리 Compose를 제거했다. NIC/SF/EU 설정을 변경하지 않았다. 원본은 `/tmp/dsb-regression-20260929/armdiag*`, `dpu-armdiag*`, `host-results/armdiag*`에 있다. 이전 비계측 코어 raw samples는 [성능 수정 보고서](2026-09-29_dsb-hotelres-performance-fix.md)의 archive에 보존돼 있다.

관련 코드: `linkerd2-proxy/linkerd/doca/src/driver.rs`의 busy-poll/yield, `linkerd2-proxy/linkerd/app/outbound/src/http/dmesh_pool.rs`의 endpoint-owner runtime dispatch. 최종 분석 JSON에는 idle/load worker별 상위 함수와 category 규칙/측정 조건을 포함한다.

원본 archive: `2026-09-29_dsb-dpu-arm-bottleneck-raw.tar.gz` (로컬, Git 제외). SHA-256: `c0ac0d5bfff9e51bf0cecabe73a1e08c5d4a21144c123b902428f481d6d8543e`.
테스트 Host 50개, DPU 역할 8개, r4 wrk 16개의 PID birth를 대조해 모두 종료됨을 확인했다.
