# Host idle wake: 50 µs tick → nap, linger, DOORBELL — 2026-09-30

판정: **idle host CPU가 1.74 core에서 0.01 core로, 부하 중 페이지당 host CPU가 23–39% 줄었다. 처리량은 같다.**
- Go 어댑터를 netpoller로 기다리게 바꾸자 Go 서버의 RPC 1개 왕복 지연이 100–250 µs 줄었다.
- DPU proxy의 L7 처리(RPC당 약 0.8 ms)가 정하는 RPS에는 영향이 없다.

설계: [docs/2026-09-30_host-wait-doorbell-plan.md](../docs/2026-09-30_host-wait-doorbell-plan.md).

## 바꾼 것

- **core:** 빈 EQ가 다시 poll하는 시점을 단계별로 정한다.
  1. 일을 한 직후에는 one-shot timer로 10 µs부터 두 배씩 늘려 100 µs까지 nap한다.
  2. 이어서 마지막 일(송신 포함) 뒤 linger(1 ms) 동안 100 µs 간격으로 poll한다.
  3. 그 뒤 잠든다.
  - custody가 남아 있으면 잠들지 않는다.
  - 잠든 EQ는 송신이 깨운다.
- **session protocol v2:** 잠들기 전 host가 push flow별로 다음에 볼 descriptor seq를 담아 ARM을 보낸다.
  - DPU는 이미 보냈으면 즉시, 아니면 다음 descriptor 완료 때 DOORBELL을 보낸다.
  - control PE의 notification fd가 모든 EQ를 깨운다.
- **host Comch control PE:** `PROGRESS_ALL`로 바꿨다. selective 모드에서는 알림이 켜진 뒤 clear 전까지 새 이벤트가 전달되지 않아, 동기 flow open이 READY를 5초 기다리다 실패했다.
- **Comch 송신 완료 로그:** INFO에서 DEBUG로 낮췄다. ARM마다 한 줄씩 남아 측정 중 수만 줄이 됐다.
- **Go 어댑터:** C 안에서 OS thread가 `ppoll`하던 대기를 Go netpoller로 옮겼다.

## Online Boutique (native, 짧은 측정)

같은 DPU proxy 조합에서 host 라이브러리, Go 서비스, DPU proxy를 수정 전과 후로 바꿔 끼우며 번갈아 쟀다.
- 한 회 구성: idle 15초 → Locust 32명으로 warm-up 15초 → 8명과 128명 각 15초 → health m1(대상마다 1+3초).
- host CPU: 서비스 10개 process CPU 시간의 합을 실제 요청 수로 나눴다.

| 실행 | idle cores | idle csw/s | 8명 req/s, p50 | 8명 cores, ms/page | 128명 req/s, p50 | 128명 cores, ms/page |
|---|---:|---:|---:|---:|---:|---:|
| 수정 전 `q-before-native` | 1.741 | 207,226 | 199, 37 ms | 3.71, 22.8 | 579, 200 ms | 5.29, 11.2 |
| 수정 후 `q-after-native` | 0.012 | 142 | 193, 38 ms | 2.18, 13.8 | 568, 210 ms | 3.96, 8.6 |
| + Go netpoller `q-gonet-native` | 0.007 | 197 | 197, 37 ms | 2.31, 14.4 | 577, 210 ms | 4.28, 9.1 |

health m1, p50 / p99 µs:

| 서비스 | 수정 전 | 수정 후 | + Go netpoller |
|---|---:|---:|---:|
| productcatalog (Go) | 1,151 / 1,980 | 1,312 / 2,479 | 1,071 / 1,897 |
| shipping (Go) | 1,173 / 1,954 | 1,143 / 2,208 | 1,054 / 1,990 |
| checkout (Go) | 1,222 / 1,937 | 1,236 / 2,330 | 1,053 / 1,902 |
| currency (Node) | 1,615 / 2,277 | 1,527 / 2,143 | 1,688 / 2,458 |
| payment (Node) | 1,579 / 2,270 | 1,516 / 2,286 | 1,535 / 2,362 |
| cart (.NET) | 1,250 / 1,870 | 1,285 / 2,112 | 1,268 / 2,111 |
| ad (Java) | 1,711 / 2,758 | 2,041 / 3,036 | 1,855 / 2,841 |
| email (Python) | 2,003 / 2,803 | 1,739 / 2,753 | 1,718 / 2,546 |
| recommendation (Python) | 1,639 / 2,518 | 1,824 / 2,808 | 1,786 / 2,753 |

- **Go:** C 루프 대기에서는 Go 서버 p99가 두 쌍 모두 150–500 µs 늘었다. netpoller로 수정 전 수준 이하가 됐다.
- **다른 언어:** 대상마다 4초씩 한 번 잰 값이다. Node, Java, Python의 ±300 µs는 회차 사이 편차 안이다.
- **대기 카운터 (수정 후 실행, 서비스 10개 합):**
  - ARM 78,872회에 DOORBELL 78,862회
  - backstop wake 4,662회 가운데 일을 찾은 것 0회
  - custody 때문에 잠들지 못한 경우 105회

## 긴 측정 1쌍 (참고)

idle 30초, Locust 8/32/128명 각 20초, health m1과 m64로 쟀다.

| | 수정 전 `before-native-r1` | 수정 후 `after-native-r1` |
|---|---:|---:|
| idle cores | 1.93 | 0.01 |
| 8/32/128명 req/s | 196/407/575 | 193/403/570 |
| 8/32/128명 ms/page | 21.4 / 13.6 / 10.4 | 14.5 / 10.5 / 8.0 |

- 수정 후 값에는 Comch 송신마다 남던 INFO 로그 비용이 포함돼 있다.
- linger 0 실행도 CPU와 m1이 1 ms와 같은 범위였다. 이 실행 역시 로그 비용이 포함돼 있다.
- DPU ERR/crash/panic은 모든 실행에서 0이다.

## TCP와의 비교

오전의 같은 장비 측정(`2026-09-30_online-boutique-fixes.csv`)과 비교했다.

| | page/s | ms/page |
|---|---:|---:|
| TCP 8명 | 662 | 10.2 |
| TCP 32명 | 1,376 | 8.6 |
| DPUMesh 수정 후 128명 | 568–570 | 8.0–8.6 |

- 처리량이 비슷한 조건에서 DPUMesh의 페이지당 host CPU는 TCP보다 낮다.
- 사용자 8명에서는 페이지당 context switch가 TCP의 약 3배다(512 대 160). 수신 하나가 DPUMesh 쪽 thread(Go poller, C++ reactor)에서 runtime thread로 건너가는 비용이다.
  - Node, Java, .NET, Python reactor thread를 합치면 약 1.1 ms/page, 전체의 약 8%다.
  - JVM JIT가 첫 구간에서 0.56 ms/page를 쓴다.

## 실장비 회귀 시험

수정 후 바이너리로 `hw-regression.sh`를 DPU busy poll 1과 0 모두 통과했다.
- gRPC: close/reopen 40회, 1 MiB+1 payload.
- stream: 3 MiB까지 byte마다 비교, 동시 stream.
- backstop이 일을 찾은 경우는 모든 process에서 0이다.

stream smoke 왕복 p50, µs:

| 크기 | busy poll 1: 수정 전 | busy poll 1: 수정 후 | busy poll 0: 수정 전 | busy poll 0: 수정 후 |
|---:|---:|---:|---:|---:|
| 64 B | 49.9 | 62.7 | 100.4 | 137.4 |
| 128 B | 50.0 | 62.2 | 4,249 | 1,126 |
| 8064 B | 89.6 | 118.4 | 4,239 | 1,345 |
| 1 MiB | 2,720 | 2,647 | 5,750 | 7,181 |
| 3 MiB | 8,263 | 8,337 | 13,413 | 10,080 |

- 약 50 µs에 오는 작은 ping-pong은 12–29 µs 느려졌다. 응답이 10→20→40 µs nap 사이에 떨어지기 때문이다.
- Online Boutique RPC는 약 1 ms 왕복이라 linger 구간에서 받는다.

## 조건

- **장비:**
  - host: rapids4, Xeon Gold 6554S, 36 core
  - DPU: BF-3 03:00.0/94:00.0, DOCA 3.1.0105
  - DPU proxy: worker 1개, busy poll 1, 로그 warn, DPA EU base 64
  - reverse 경로: `dpu-dma`
  - 03:00.1에서는 다른 DPA job이 계속 실행 중이었다.
- **host 라이브러리 `libdpumesh.so.5`:**
  - 수정 전 `719e13b2…`
  - 수정 후 `0183e4a0…`
- **Go frontend:**
  - 수정 전 `3297c694…`
  - netpoller `f9dd39d7…`
- **DPU proxy:**
  - 수정 전 `f3cf095b…`
  - 수정 후 `feffccde…`
- **CSV:** [2026-09-30_host-idle-wake.csv](2026-09-30_host-idle-wake.csv) (`summarize.py` 출력)
- **원자료:** `build/hw-ab-2026-09-30/idle-wake/`, 로컬 전용. 실행별 `results/`, 교대 스크립트, hw-regression 로그가 있다.

## 재현

fork checkout에서 실행한다. 수정 전과 후는 host 라이브러리, Go 서비스, DPU proxy를 함께 바꿔 끼운다. session protocol이 v1과 v2로 달라 섞으면 HELLO에서 실패한다.

```sh
export DPUMESH_ROOT=$HOME/DPUMesh-online-boutique
IDLE_SEC=15 USERS_LIST="8 128" LDUR=15 M1_WARM=1s M1_DUR=3s SKIP_M64=1 \
  bash dpumesh/bench/perf.sh native <tag>
DPUMESH_WAIT_STATS=<dir> bash dpumesh/bench/hw-regression.sh
```
