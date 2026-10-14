# DPA flow 다중화 (A안) — 2026-10-13

상태: **구현됨, 기본값은 꺼짐(`DMESH_DPA_MUX` 미설정).** 2026-10-14 jet2-dpu PF1에서 smoke와 A/B 측정을 통과했다. 사전 측정은 [`2026-10-13_dpa-mux-probe.md`](2026-10-13_dpa-mux-probe.md).

지금 구조는 DPU에서 forward 연결마다 DPA thread를 하나씩 EU에 올린다. 그래서 EU 190개가 flow 수의 상한이 되고, Online Boutique 배치(frontend 수, replica 라우팅)를 묶었다. A안은 worker마다 DPA thread를 몇 개만 두고, 그 thread가 여러 연결의 forward ring을 번갈아 처리한다. host, descriptor 형식, custody 의미, Linkerd는 바꾸지 않는다.

첫 구현(2026-10-13)은 ring 칸을 메시지(RING_ADD/ADD_ACK, RING_DEL/DEL_ACK)로 열고 닫았다. 지금은 ARM이 thread local storage(TLS)에 직접 쓰고, DPA는 칸의 상태 필드로 답한다. 제어 메시지가 없어져 DPA 쪽 메시지 수신과 ACK 전송이 빠졌다.

## 켜는 법

| 변수 | 뜻 |
|---|---|
| `DMESH_DPA_MUX=n` | worker당 mux thread 수(1–16). thread 하나가 ring 최대 8개(`DMESH_MUX_RINGS`)를 맡는다. 미설정이나 0이면 지금처럼 연결마다 thread |
| `DMESH_DPA_MUX_STALL_MS` | 가장 오래된 진행 중 copy가 이보다 오래되면 그 thread를 멈춘 것으로 보고 ERR 로그(기본 100) |
| `DMESH_DPA_MUX_STATS=1` | thread별 초당 한 줄: 바퀴 수, ARM credit 부족, 진행 중 copy 수, 오류 CQE |
| `DMESH_DPA_PIN_EU_BASE=b` | 측정용. mux thread i를 EU `b + 64w + i`에 고정 |

session으로 다중화된 연결(지금의 host 라이브러리)에만 적용한다. 예전 방식의 연결과 host-dpa 역방향은 지금처럼 연결마다 thread를 쓴다.

## 구조

- **DPA 상태(TLS, `struct dpa_mux_ctx`, 약 3.6 KB):** 128 B 헤더(핸들, FIFO 위치, 카운터, stop), copy FIFO 128칸, ring 칸 8개(`struct dpa_mux_ring`, 각 192 B). 모든 필드는 64비트라서 ARM과 DPA 컴파일러가 같은 배치를 본다(`common/dpa_common.h`).
- **ring 칸 하나는 cache line 세 개다.** 필드마다 쓰는 쪽이 하나이고, ARM 필드와 DPA 필드는 다른 line에 있다.
  - line 0(ARM, 칸을 채울 때만): `buf_arr`, 두 mmap, staging 주소와 크기, ring 크기, TX 풀 범위.
  - line 1(ARM): `slot`, `generation`, `cmd`, `rx_consumed_pos`.
  - line 2(DPA): `pos`, `submit`, `complete`, `completed_bytes`, `inflight`, `failed`, `state`, `error`.
- **칸 열기:** `AWAIT_METADATA`에서 mmap import, `buf_arr`, staging까지는 지금과 같다. 그 뒤 thread를 run하는 대신, ring이 가장 적은 thread의 빈 칸을 고른다.
  1. ARM이 `cmd = NONE`을 쓴다. cmd가 NONE인 칸은 DPA가 건드리지 않는다.
  2. 칸 전체(192 B)를 쓴다. line 2는 0이고 `state = IDLE`이다.
  3. 마지막에 `cmd = OPEN`을 쓴다. 각 `h2d_memcpy`는 다음 것이 시작하기 전에 끝난다.
  - DPA는 다음 바퀴에 OPEN과 IDLE을 보고 ACTIVE로 바꾼다. 연결은 OPEN을 쓰자마자 `RUNNING`으로 가고, 확인을 기다리지 않는다. 새 연결 상태 값은 없다(Rust driver는 모르는 상태 값을 `Free`로 읽는다).
- **칸 닫기:** ARM이 `cmd = CLOSE`를 쓴다. DPA는 그 ring의 새 copy를 멈추고, 진행 중 copy가 0이 되면 `state = CLOSED`로 바꾼다.
  - ARM은 칸을 `d2h_memcpy`로 읽어, CLOSED이고 `받은 DMA_DONE 수 + failed == submit`일 때 끝난 것으로 본다. consumer PE에 아직 남은 메시지가 있을 수 있어서 수를 맞춘다.
  - 그다음에 지금처럼 DMA task, `buf_arr`, mmap, staging을 해제하고 칸을 반환한다.
  - 1초 안에 끝나지 않으면 지금의 quiesce 시간 초과와 같이 다루고, 칸은 남겨 둔다.
- **데이터 경로(`run_dma_mux`, `device/dpa_kernel.c`):** 한 바퀴마다 CQE를 처리하고, 칸마다 cmd를 보고, ACTIVE인 ring마다 descriptor를 최대 32개 복사한다.
  - ring마다 `get_external_ptr`로 ctrl을 다시 구한다. 미리 구해 둔 포인터는 mmap을 바꿀 때 두 배 느리다.
  - staging 공간은 ring별로 확인하므로, 공간이 없는 ring만 건너뛴다.
  - 바퀴 전체를 멈추는 것은 공용 자원인 ARM credit과 FIFO뿐이다.
  - 20 ms 돌고 retrigger한다. descriptor 쓰기는 completion을 만들지 않아서 쉬지 않고 다시 돈다.
- **CQE와 연결 맞추기:** producer 작업은 copy뿐이고, copy마다 CQE가 하나씩 생긴다. FIFO에 제출 순서대로 `(ring, 길이)`를 넣고 CQE마다 맨 앞을 꺼낸다. 연결의 `consumer_head`(custody ACK)는 지금처럼 그 연결의 copy가 끝난 뒤에만 오른다.
- **메시지(16 B, `struct dmesh_mux_msg`):** DMA_DONE 하나뿐이고, copy의 즉시 데이터로 간다. `{ring, slot, generation, pos, length}`를 싣는다. ARM은 ring 칸으로 연결을 찾고, generation이 다르면 버린다.
- **검사:** descriptor 크기, `producer_tail`이 거꾸로 가거나 ring보다 앞서는지에 더해, `addr`가 그 연결의 TX 풀 범위 안인지도 본다. 어긋나거나 copy가 실패하면 그 ring만 `state = ERROR`가 되고, 오류 값은 host ring의 `ctrl.error`에도 남는다.
- **점검(`dmesh_dpa_mux_watch`, 1초마다):** thread마다 TLS 전체를 읽는다.
  - ring이 ERROR면 그 연결을 EIO로 닫는다. 메시지 방식보다 알아채는 데 최대 1초 늦다.
  - heartbeat(바퀴 수)가 그대로면 "is not running", 가장 오래된 copy가 `DMESH_DPA_MUX_STALL_MS`를 넘으면 "is stalled" ERR 로그를 남긴다.
  - 첫 점검은 thread를 모두 띄우고 1초 뒤에 한다. 그 전에 하면 아직 돌지 않은 thread를 멈췄다고 잘못 알린다.

## 알려진 한계

- **죽은 프로세스의 메모리를 복사하면 thread가 영구히 막힌다.** probe 실험 2b에서, copy가 오류도 완료도 없이 끝나지 않았고 같은 producer의 뒤 작업도 전부 멈췄다. A안에서는 그 thread의 연결(최대 8개, 다른 프로세스 것 포함)이 함께 멈춘다.
  - 생기는 조건: 프로세스가 비정상으로 끝나는 순간, 그 ring에 아직 복사하지 않은 descriptor가 남아 있을 때.
  - 지금은 `dmesh_dpa_mux_watch`가 ERR 로그로 알리기만 한다.
  - 해결은 broker 병합 때 한다. broker는 클라이언트가 죽어도 DPU가 flow를 닫을 때까지 메모리를 놓지 않는다(`feature/doca-broker`, `channel_broker.c`). 그때 직접 경로와 broker crash를 어떻게 다룰지(감지 후 복구, 또는 프로세스별로 thread를 나누기)도 함께 정한다.
- **ring 오류는 1초 점검에서 처리한다.** 그 사이 그 연결은 멈춰 있고, 같은 thread의 다른 ring은 계속 돈다.
- **벤치마크 안내:** mux를 켠 채로 측정 중 host 프로세스를 `kill -9` 하지 않는다. DPU 쪽은 지금처럼 실행마다 다시 띄운다.
- **worker 종료:** mux thread 정리(`release_mux`)는 연결이 모두 떠난 뒤에만 한다. 막힌 thread는 정리하지 못할 수 있다.

## 측정 (2026-10-14, jet2-dpu PF1)

`dpumesh_dpu`(echo) + `dpumesh_host`, 연결 16개, 64B echo, `BENCH_WINDOW=4096`, 10초. host 코어 16–31, DPU ARM 코어 8–9, EU 96번부터. 각 구성 2회, 같은 날 연달아 측정했다.

| 구성 | 쓰는 EU | msg/s | RTT p50 | RTT p99 |
|---|---:|---:|---:|---:|
| 연결마다 thread | 16 | 4.01M | 108–110 µs | 284–291 µs |
| `DMESH_DPA_MUX=2` (thread당 ring 8개) | 2 | 3.72–4.01M | 116–119 µs | 280–297 µs |
| `DMESH_DPA_MUX=8` (thread당 ring 2개) | 8 | 4.60–4.63M | 102 µs | 253 µs |

- ring 8개를 한 thread에 맡기면 EU를 16개에서 2개로 줄이고, 처리량 0–7%, p50 약 8 µs를 내준다. p99는 거의 같다.
- thread당 ring 2개는 연결마다 thread보다 처리량이 15–16% 높고, p50은 6–8 µs, p99는 30 µs 넘게 낮다.
- 연결마다 thread 경로는 이번 변경과 무관한데, 2026-10-13(4.19–4.21M, p99 203–208 µs)보다 처리량이 5% 낮고 p99가 약 80 µs 높았다. 날마다 차이가 있으니 같은 날 값끼리 비교한다.
- 메시지 방식(2026-10-13)과 비교해도, 연결마다 thread 대비 비율은 비슷하다. mux 2는 그때 2–9% 낮았고 지금 0–7% 낮다. mux 8은 그때 11–12% 높았고 지금 15–16% 높다.
- DPU의 forward DMA 완료 수(10초)는 처리량에 비례했다. 연결마다 thread 197만, mux 2 180–194만, mux 8 223–225만이다.
- 8 KiB echo, 연결 4개, mux 2: 방향마다 12.0 Gbps(메시지 방식 10.9 Gbps, 각 1회).
- 같은 DPU 프로세스에 host를 두 번 연결(64B 연결 16개 × 2회)해, mux 2와 mux 8에서 각각 연결 32번을 열고 닫았다. 실패와 오류 로그는 0이었다.
- host 코어를 8개만 줬을 때(thread 16개가 코어를 나눠 씀)는 1.8M 안팎으로 떨어지고 실행마다 흔들렸다. 비교 측정은 host 코어를 넉넉히 준다.

## 다음 측정

1. 대용량 연결 4개를 thread 1개에 모을 때와 나눌 때(probe 문서의 실험 3).
2. 연결 1000번 열고 닫기. Rust proxy에서 CLIENT 연결을 닫을 때의 segfault가 사라지는지도 본다.
3. staging이 꽉 찬 ring이 같은 thread의 다른 ring을 막지 않는지.
4. flow 200–300개.
5. OB 16코어 재측정(frontend 14–16개, 모든 replica로 round-robin).
