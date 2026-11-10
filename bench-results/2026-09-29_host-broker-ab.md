# Host broker 1단계 before/after — 2026-09-29

**Verdict:** broker 구조 자체(IPC, memfd, 등록 프로세스 분리)의 데이터 경로 비용은 측정되지 않았다.

- 처음에는 4-thread 8 KiB echo가 −3.6~−6.6%였다.
- `perf`로 추적한 원인은 DPU driver 코어의 동기식 rd_pos 갱신(`doca_dpa_h2d_memcpy`)이었다.
- `shim.c`에서 이를 64 KiB 단위로 묶었다. 그 뒤로는 모든 케이스가 before와 −2.6~+2.0% 안에 들고, 4-thread echo는 before와 after 모두 **14.3 → 20.3–20.8 Gbps(약 +42%)** 가 됐다.
- 남은 차이는 4-thread echo의 −2.6%(범위 겹침)와 RTT p50 +4.8%(겹치지 않음)다.

## 조건

| 항목 | 값 |
|---|---|
| Host | rapids4, Xeon Gold 6554S, 36 core, SMT 없음. BlueField `94:00.0`은 NUMA node 1(CPU 18–35) |
| DPU | BF-3 `03:00.0` / rep `94:00.0`, `dpumesh_dpu`(`DMESH_MODE=sink/echo`, `DMESH_BUSY_POLL=1`, server `DPUMeshBR`, 실행마다 재시작). `03:00.1` 인스턴스는 건드리지 않음 |
| DPU 바이너리 | 기존: sha256 `e7f9493a…`(main과 같은 DPU 소스). 수정: `377a0593…`(`shim.c` rd_pos 묶음 + `object.h`/`dpa.c`) |
| DOCA / kernel | 3.1.0105 양쪽, host 5.15.0-191, DPU 5.15.0-1074-bluefield |
| before | main `b607129` worktree. `libdpumesh.so.5` `522a8b29…`, `dpumesh_host` `30bf0c93…`. `DPUMESH_PCI_ADDR=94:00.0`(app이 직접 device open) |
| after | `feature/doca-broker` 작업 트리. 최종 `libdpumesh.so.5` `5ef70d2d…`, `dpumesh_broker` `f646a92d…`, `dpumesh_host` `fd579a9c…`. broker `--pci 94:00.0`, CPU 30 고정. app은 `DPUMESH_BROKER`만 설정 |
| 부하 | `dpumesh_host` CPU 20–27 고정, 10 s. 케이스마다 before/after를 번갈아 3회. conn 케이스는 한 channel에서 `dmesh_create_qp`/`dmesh_destroy_qp` 100회 |
| 집계 | host `HOST_BENCH_DONE`, DPU 초당 `TOTAL` 줄의 중앙값(첫·끝 줄 제외). 표는 3회의 median [min–max] |

## 최종 결과 (수정 DPU)

| case | metric | before | after | Δ median |
|---|---|---|---|---|
| sink 1 thread 8 KiB | Gbps (DPU recv) | 9.81 [9.81–9.81] | 9.86 [9.84–9.86] | +0.5% |
| sink 1 thread 64 B | msg/s | 8,589,947 [7,425,854–8,618,977] | 8,758,379 [8,737,625–8,764,146] | +2.0% |
| echo 1 thread 8 KiB | Gbps | 9.75 [9.74–9.75] | 9.79 [9.79–9.80] | +0.4% |
| | RTT p50 / p99 µs | 213 / 226 | 212 / 225 | −0.5% / −0.4% |
| echo 4 thread 8 KiB | Gbps | 20.78 [20.54–20.82] | 20.25 [20.09–20.78] | −2.6% |
| | RTT p50 / p99 µs | 376 / 462 | 394 / 470 | +4.8% / +1.7% |
| echo 1 thread 64 B ping-pong | msg/s | 76,742 [76,610–76,794] | 77,370 [76,989–77,409] | +0.8% |
| | RTT p50 / p99 µs | 11 / 12 | 11 / 13 | 0 / +1 µs |
| conn open / close | p50 ms | 211.7 / 71.6 | 211.8 / 71.9 | +0.0% / +0.4% |
| channel create / destroy | ms | 104.8 / 21.6 | 93.5 / 16.7 | −11% / −23% |

## 수정 전 결과 (기존 DPU)

| case | before | after | Δ |
|---|---|---|---|
| sink 8 KiB | 9.78 Gbps | 9.82 Gbps | +0.4% |
| sink 64 B | 8.62 M msg/s | 8.77 M msg/s | +1.8% |
| echo 1 thread 8 KiB | 8.37 Gbps | 8.30 Gbps | −0.8% |
| **echo 4 thread 8 KiB** | **14.30 Gbps** | **13.35 Gbps** | **−6.6%**(다른 두 세트는 −4.9%, −3.6%) |
| ping-pong 64 B | 75.2 k msg/s | 75.7 k msg/s | +0.7% |

두 세트의 반복별 값은 [CSV](2026-09-29_host-broker-ab.csv)에 있다(`dpu` 열: `old`, `rx_wm_batch`).

## 원인 추적

### 1. host 쪽 분리

모든 행은 echo 4 thread 8 KiB이고 기존 DPU를 썼다. 실험 변형은 broker 없이 app이 device를 직접 여는 scratch 빌드다.

| 변형 | Gbps |
|---|---|
| 직접 open + memfd 메모리 | 14.15–14.59 |
| 직접 open + private 메모리(새 코드) | 14.26–14.36 |
| 직접 open + poll마다 하던 Comch PE progress 제거 | 13.35–13.84 |
| 위 + poll마다 1 µs pause | 16.53–16.78 |

- broker, memfd, 리팩터링은 원인이 아니다.
- 원인은 in-process 경로가 poll마다 하던 idle Comch PE progress(1 thread 66 ns, 4 thread 경합 약 560 ns)가 빠진 것이었다.
- 교차 EQ drain, `stripe_lock` 공유와 패딩, 연결별 DMA line 읽기 빈도, NUMA, 다른 코어의 잡음 thread(ALU·공유/전용 xchg·pause)는 모두 차이를 만들지 않았다.

### 2. perf (root)

**host:**
- after는 cycle의 71%를 `drain_rev_rings_span`에서 썼다(before는 29%).
- IPC는 0.47(before 0.81), context switch는 1/13이었다.
- 하지만 host 쪽 경합을 줄이는 변형들은 처리량을 올리지 못했다.

**DPU driver(1 ARM core, busy poll):**
- 세 변형 모두 100% 사용이었고, 함수 분포도 비슷했다.
- vdso `clock_gettime` 샘플(약 25%)은 전부 다음 경로에서 나왔다: `dmesh_doca_conn_rx_watermark` → `doca_dpa_h2d_memcpy` → `flexio_memcpy` → `poll_host_cq_infinite`.

**DPU 계수기(측정용 빌드):**

| 변형 | Gbps | ticks/s | events/tick | watermark memcpy/s | tick당 | 1회 | 코어 점유 |
|---|---|---|---|---|---|---|---|
| before | 13.77 | 98 k | 9.0 | 218 k | 2.28 | 1.64 µs | 35.7% |
| after | 13.42 | 92 k | 8.9 | 238 k | 2.52 | 1.61 µs | 38.4% |
| host 1 µs pause | 16.22 | 116 k | 9.2 | 164 k | 1.47 | 1.71 µs | 28.0% |

- tick당 처리량은 같다.
- rd_pos 동기 memcpy는 "tick마다 새 데이터가 있던 flow 수"만큼 불린다. 그래서 host의 post 리듬이 매끄러울수록(after) 메시지당 호출과 DPU 코어 사용이 늘었다.

### 3. 수정과 확인

- `shim.c`의 `dmesh_doca_conn_rx_watermark`는 이제 소비가 64 KiB(`DMESH_RX_WM_BATCH`) 전진했을 때만 memcpy를 한다.
- 연결별 마지막 값은 `struct dmesh_conn`의 `rx_wm_published`에 두고, DPA thread arg를 만들 때(`rd_pos = 0`) 0으로 맞춘다.
- host-dpa 경로의 `CHANNEL_RD_POS_BATCH`와 같은 방식이다.
- DPA는 1 MiB staging의 여유가 3×8064 B 미만일 때만 멈춘다. 64 KiB 지연은 그 시점을 그만큼만 앞당기고, 다 비운 소비자를 멈추게 하지 않는다(`_Static_assert`).
- 측정용 빌드로 확인한 결과: memcpy 코어 점유가 7.7%로 줄었고, 4-thread echo는 before 20.4–20.6, after 20.4–20.7, pause 변형 20.6–20.7 Gbps였다. host 리듬의 영향이 거의 사라졌다.

## 장애 시나리오 (echo 2 thread, 수정 DPU)

| 시나리오 | 결과 |
|---|---|
| app SIGKILL | broker child가 소켓 종료를 보고 flow 2개를 DPU와 닫은 뒤 `done (clean)`. 이어서 새 client의 open/close 정상 |
| broker child SIGKILL | app이 `Broker connection lost`를 로그하고(liveness 검사 20 ms 주기) 두 worker 모두 에러 이벤트로 종료. DPU도 flow 2개를 정리. 새 client 정상 |
| broker 데몬 SIGTERM | child가 death signal로 SIGTERM을 받아 정리하고 `done (clean)`. app 종료, 소켓 파일 제거 |

## 재현과 자료

- `make lib broker`; `dpumesh_broker --listen <sock> --pci 94:00.0`.
- app 환경: `DPUMESH_BROKER=<sock> DPUMESH_SERVER=DPUMeshBR DPUMESH_POD_IP=10.99.0.1`.
- DPU: `DMESH_DEV_PCI=03:00.0 DMESH_REP_PCI=94:00.0 DMESH_SERVER_NAME=DPUMeshBR DMESH_MODE=<sink|echo> DMESH_BUSY_POLL=1 dpumesh_dpu`.
- 하네스, 측정 프로그램, 실험 패치, 전체 로그는 **로컬 보존** `2026-09-29_host-broker-ab-raw.tar.gz`(Git 미포함)에 있다.
