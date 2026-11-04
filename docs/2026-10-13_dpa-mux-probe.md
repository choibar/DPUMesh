# DPA 다중화 사전 측정 (probe) — 2026-10-13

상태: **실험 1·2a·2b 완료(2026-10-13, PF1).** 결과는 맨 아래.

A안(EU thread 하나가 여러 연결의 forward ring을 번갈아 처리)을 구현하기 전에 두 가지를 잰다.

1. **폴링 비용**: thread 하나가 ring R개의 `producer_tail`을 읽는 한 바퀴 시간. R = 1, 2, 4, 8, 16. thread당 ring 수와 worker당 thread 수(E)를 정한다.
2. **장애 범위**: host 프로세스가 copy 도중 죽을 때
   - (2a) 지금 구조에서 무슨 일이 생기는지
   - (2b) 같은 producer로 다른 연결의 copy가 계속 되는지. A안에서 오류 하나가 thread의 연결 전부를 멈추는지가 여기서 갈린다.

대용량 연결 4개를 thread 하나에 몰 때의 대역폭(실험 3)은 mux kernel이 있어야 하므로 구현 첫 단계에서 잰다.

## 코드

| 파일 | 내용 |
|---|---|
| `src/transport/common/dpa_probe.h` | probe 상태(`struct mux_probe_state`, 모두 64비트 필드), 모드, POLL 변형 |
| `src/transport/device/benchmarks/mux_probe.c` | DPA entry `run_mux_probe`. activation마다 POLL 1000 pass 또는 FAULT copy 256개, 그 뒤 retrigger |
| `src/transport/dpu/mux_probe.{c,h}` | ARM 쪽: thread 생성, msgq 한 쌍, init RPC, 상태 복사, run, kick 메시지. 결과는 `d2h_memcpy`로 읽음 |
| `apps/dma_bench/dpu/dpumesh_dpu.c` | `DMESH_MUX_PROBE=poll\|fault`. 연결이 N개 RUNNING이 되면 probe를 시작하고 1초마다 보고 |

probe thread는 연결별 thread 옆에서 같은 DPA context로 돈다. 연결별 thread는 평소처럼 돌고, probe는 ring을 읽기만 하거나(POLL) 자기 scratch 버퍼로만 복사한다(FAULT). 테스트 도구라 probe는 프로세스가 끝날 때까지 정리하지 않는다.

POLL 변형. 한 pass는 host 메모리의 64비트 값 R개를 읽는다.

| 변형 | 내용 |
|---|---|
| `lookup` | ring마다 buf_arr에서 포인터를 다시 구하고, pass마다 window invalidate 1번. 지금 kernel이 쓰는 방식 |
| `cached` | 포인터를 한 번만 구해 둠 |
| `same_ring` | ring 0의 descriptor R줄. ring을 바꾸지 않을 때의 PCIe 읽기 비용 |
| `inv_each` | 읽을 때마다 invalidate |

## 빌드

host (jet2, 이 checkout, `feature/dpa-mux`):

```bash
cd ~/DPUMesh/src/transport && meson setup build && ninja -C build
cd ~/DPUMesh && make lib
cd ~/DPUMesh/apps/dma_bench && meson setup build && ninja -C build     # dpumesh_host
```

DPU (jet2-dpu). 기존 `~/DPUMesh*` 디렉터리는 건드리지 않고 새 디렉터리에 복사해 빌드한다.

```bash
# jet2에서
rsync -a --delete --exclude '.git' --exclude 'build/' --exclude '/linkerd2-proxy/target' \
      --exclude 'node_modules' ~/DPUMesh/ jukebox@192.168.100.2:~/DPUMesh-dpa-mux/
# DPU에서
cd ~/DPUMesh-dpa-mux/src/transport && meson setup build && ninja -C build
cd ~/DPUMesh-dpa-mux/apps/dma_bench && meson setup build && ninja -C build    # dpumesh_dpu
```

## 실행 전 확인

- DPU와 jet2에 다른 실험이 없는지 본다: `who`, `ps -eo user,args | grep -E "dpumesh|crypto_owner|linkerd|dmesh"`, PF별 사용 여부는 `rdma resource show`(열린 `ctx` 수).
- 다른 실험이 PF0을 쓰고 있으면 PF1(`03:00.1` / `0b:00.1`, 프로그램 기본값)만 쓰고, 그 실험이 쓰지 않는 ARM 코어와 DPA EU에 고정한다. 아래 명령은 2026-10-13에 쓴 값이다(다른 런타임이 코어 0,3,5,6,7,10,11,13,15와 EU 0–31을 사용).
- `dpumesh_dpu`는 root로 실행한다. 일반 사용자로는 host representor 목록을 만들지 못해 `dmesh_doca_init`이 실패한다(`devlink`도 Operation not permitted).
- 이 브랜치에는 broker가 없으므로 host 프로그램은 장치를 직접 연다.
- DPU 쪽 프로세스는 실행마다 새로 띄운다. 종료할 때는 이름이 아니라 PID로 한다. 다른 런타임도 같은 이름(`dpumesh_dpu`)을 쓴다.

## 실험 1: 폴링 비용

DPU:

```bash
cd ~/DPUMesh-dpa-mux/apps/dma_bench
sudo env DMESH_MODE=echo DMESH_MUX_PROBE=poll DMESH_MUX_PROBE_CONNS=16 \
     DMESH_DPA_PIN_EU_BASE=96 DMESH_MUX_PROBE_EU=170 taskset -c 8,9 ./build/dpumesh_dpu 2>&1 | tee ~/probe-poll.log
```

host (DPU 쪽이 뜬 뒤):

```bash
cd ~/DPUMesh/apps/dma_bench
DPUMESH_PCI_ADDR=0b:00.1 DPUMESH_POD_IP=10.0.0.1 BENCH_THREADS=16 BENCH_MODE=echo BENCH_SIZE=64 \
BENCH_WINDOW=64 BENCH_DURATION=60 taskset -c 30,31 nice -n 10 ./build/dpumesh_host
```

연결 16개가 RUNNING이 되면 `MUX_PROBE start`가 찍히고, 수 초 안에 표와 `MUX_PROBE done`이 나온다. 장비가 비어 있으면 부하를 바꿔(`BENCH_MODE=sink BENCH_SIZE=8192`, DPU도 `DMESH_MODE=sink`) 한 번 더 잰다.

읽는 법:

- `us/ring`은 ring을 하나 더 볼 때 늘어나는 시간이다.
- R=16의 `avg_us`가 한 바퀴 시간이고, 메시지 하나가 더 기다릴 수 있는 최대 시간이다.
- `lookup`과 `cached`의 차이는 포인터를 구하는 비용, `cached`와 `same_ring`의 차이는 ring을 바꾸는 비용이다.
- 맨 위 `cycles_per_us`가 비정상(예: 0이나 수만)이면 `__dpa_thread_time()` 단위 가정이 틀린 것이므로, cycle 열만 본다.

판단: R=16 한 바퀴가 10 µs 안쪽이면 thread당 ring 16개, E=4로 시작한다. 그보다 길면 thread당 ring 수를 줄이거나(E를 늘림), host가 게시할 때 올리는 프로세스별 요약 카운터(설계 D4)를 검토한다.

## 실험 2a: 지금 구조에서 host 프로세스가 죽을 때

DPU:

```bash
sudo env DMESH_MODE=echo DMESH_DPA_PIN_EU_BASE=96 taskset -c 8,9 ./build/dpumesh_dpu 2>&1 | tee ~/probe-kill-current.log
```

host 터미널 두 개:

```bash
# A
DPUMESH_PCI_ADDR=0b:00.1 DPUMESH_POD_IP=10.0.0.1 BENCH_THREADS=1 BENCH_MODE=echo BENCH_SIZE=8192 BENCH_WINDOW=65536 \
    BENCH_DURATION=40 taskset -c 30 ./build/dpumesh_host & A=$!
# B
DPUMESH_PCI_ADDR=0b:00.1 DPUMESH_POD_IP=10.0.0.2 BENCH_THREADS=1 BENCH_MODE=echo BENCH_SIZE=8192 BENCH_WINDOW=65536 \
    BENCH_DURATION=40 taskset -c 31 ./build/dpumesh_host
```

10초쯤 뒤 `kill -9 $A`. 볼 것:

- B의 `HOST_BENCH_RATE`가 계속 나오는지
- DPU 로그의 DPA 오류(`dma_error`, `DPA context`, `IO_FAILED`)
- `dpumesh_dpu`가 계속 살아 있는지

같은 실험을 `DMESH_NO_TEARDOWN=1`로 한 번 더 한다. 이때는 A가 죽어도 DPU가 A의 매핑을 지우지 않는다.

## 실험 2b: 같은 producer로 다른 연결의 copy를 계속할 수 있는가

DPU:

```bash
sudo env DMESH_MODE=echo DMESH_NO_TEARDOWN=1 DMESH_MUX_PROBE=fault DMESH_DPA_PIN_EU_BASE=96 DMESH_MUX_PROBE_EU=170 \
     taskset -c 8,9 ./build/dpumesh_dpu 2>&1 | tee ~/probe-fault.log
```

`DMESH_NO_TEARDOWN=1`은 A가 죽은 뒤에도 DPU가 A의 매핑을 지우지 않게 한다. 그래야 오류가 "host 메모리가 사라져서"인지만 볼 수 있다.

host에서 A를 먼저 띄우고(2a와 같은 명령), DPU에 `slot 0 running`이 보이면 B를 띄운다. probe는 slot 0(A), slot 1(B)로 시작하고(`MUX_PROBE start: mode=fault slots=0,1`), 1초마다 `MUX_PROBE fault ...` 줄을 찍는다. 두 slot의 `ok`가 늘어나는 것을 확인한 뒤 `kill -9 $A`.

probe는 CQE를 `DMESH_MUX_PROBE_TIMEOUT_US`(기본 100 ms) 동안 기다린다. 오지 않으면 그 출처를 `stalled_on`으로 적고, 다른 출처만 16번 더 복사한다(`then other source: ok/err/timeouts`). `arm_msgs`는 ARM에 실제로 도착한 완료 메시지 수를 출처별로 센 것이라, 늦게라도 끝난 copy가 어느 쪽 것인지 가린다. probe가 끝난 뒤에도 이 줄은 1초마다 계속 찍힌다.

| 관찰 | 뜻 | A안에서 할 일 |
|---|---|---|
| A의 `err`가 늘고 B의 `ok`는 계속 는다 | 오류가 copy 하나로 끝나고 producer는 산다 | 장애 범위는 그 연결뿐. 그 ring만 RING_ERROR로 끊는다 |
| A 오류 뒤 B도 `err`가 나거나 `stalled_on`이 찍힌다 | producer가 오류 상태로 간다 | 장애 범위는 thread 전체. thread를 다시 만들고 살아 있는 ring을 다시 등록하는 경로가 필요하다 (D3) |
| `state read failed or DPA context failed` | 오류가 DPA context 전체로 번진다 | 지금 구조도 같은 위험이라 A안이 더 나빠지지는 않는다. 별도 문제로 기록 |
| A도 오류 없이 `ok`가 계속 는다 | 죽은 프로세스의 메모리가 바로 막히지 않는다 | 이 경로로는 오류가 나지 않는다. 메모리가 실제로 해제·재사용되는 시점을 따로 확인 |

## 결과

### 실험 1: 폴링 비용 (2026-10-13 UTC 07:05–07:07, jet2 / jet2-dpu, PF1)

조건:

- DPU: `run-probe-poll.sh`, root로 실행. ARM 코어 8·9, pool thread EU 96–159(`DMESH_DPA_PIN_EU_BASE=96`), probe EU 170
- host: `taskset -c 30,31`, 연결 16개, 64B echo, `BENCH_WINDOW=64`(연결당 메시지 1개). DPU 기준 합계 약 26.5k msg/s
- PF0에서는 다른 실험의 런타임이 돌고 있었다(DPA EU 0–31, 같은 PCIe 링크). 값이 실제보다 약간 클 수 있으므로, `min_cyc`를 하한으로 함께 본다
- `cycles_per_us=1799.5` (DPA 1.8 GHz), pass 4000회씩

| 변형 | R=1 | R=2 | R=4 | R=8 | R=16 | ring당 |
|---|---:|---:|---:|---:|---:|---:|
| `lookup` (지금 kernel 방식) | 0.81 µs | 1.68 | 3.38 | 6.89 | **14.18** | 0.85–0.89 |
| `cached` | 0.73 | 2.49 | 6.04 | 13.20 | 27.51 | 1.25–1.72 |
| `same_ring` | 0.75 | 1.57 | 3.44 | 6.52 | 15.07 | 0.75–0.94 |
| `inv_each` | 0.78 | 2.50 | 6.08 | 13.28 | 27.68 | 1.25–1.73 |

`avg_us`, 한 pass(ring R개를 한 번씩 읽기)의 평균이다. 원자료(`MUX_PROBE` 줄)는 DPU의 `~/probe-poll-run1.log`.

읽은 것:

1. **host 메모리 64비트 한 번 읽기가 약 0.75 µs다.** R=1이 변형과 상관없이 0.73–0.81 µs이고, 최소값은 0.53–0.68 µs.
2. **지금 kernel 방식(`lookup`)은 ring 수에 정비례한다.** ring 하나에 약 0.87 µs씩 늘어서, R=8은 6.9 µs, R=16은 14.2 µs다. 폴링만 보면 descriptor 하나가 최대 한 바퀴만큼 더 기다린다. 지금은 연결마다 thread가 있어 약 0.8 µs다.
3. **ring마다 포인터를 다시 구하는 쪽이 오히려 빠르다.** 다른 ring(다른 mmap)의 포인터를 미리 구해 두고 읽으면(`cached`) ring당 1.7 µs로 두 배가 된다. 같은 ring 안의 줄을 읽을 때(`same_ring`)는 그렇지 않다. invalidate를 매번 해도(`inv_each`) 같으므로, 비용은 invalidate가 아니라 mmap을 바꾸는 데서 나온다. `doca_dpa_dev_buf_get_external_ptr`를 ring마다 부르면 이 비용이 없다. 따라서 mux kernel도 ring마다 이 함수를 불러야 한다.
4. **판단:** 기준(R=16 한 바퀴가 10 µs 안쪽)을 넘는다. thread당 ring 16개(E=4)는 폴링만으로 hop마다 최대 14 µs가 더해진다. OB의 hop당 지연(약 1 ms)에는 작지만, 64B echo 같은 마이크로벤치에는 크다.
   - thread당 ring을 **8개 이하**로 두면 한 바퀴가 7 µs 이하다. worker당 연결이 64개면 E=8, worker 14개면 EU 112개로, 여전히 flow 수와 무관하고 190 안에 든다.
   - 더 줄이려면, host가 게시할 때 올리는 프로세스별 요약 카운터를 둔다(설계 D4). 그러면 한 바퀴가 요약 1번 읽기(약 0.8 µs)와 바뀐 ring 읽기로 줄어든다. host 변경이 필요해 2단계로 둔다.

같은 칩의 다른 실험 때문에 대용량(8 KiB sink) 조건은 재지 않았다. 장비가 비면 대용량 조건과 반복 측정을 추가한다.

### 실험 2a: 지금 구조에서 host 프로세스가 죽을 때 (2026-10-13 UTC 07:58–08:00)

조건: PF1, `DMESH_MODE=echo`, host A·B 각각 연결 1개, 8 KiB echo(`BENCH_WINDOW=65536`). 두 연결 합계 약 7.8 Gbps에서 A를 `kill -9`. DPU 시계는 host보다 약 11.5초 늦어서, 로그 시각은 이만큼 보정해서 읽는다.

| | teardown 켬(기본) | `DMESH_NO_TEARDOWN=1` |
|---|---|---|
| DPU가 끊김을 본 시점 | 죽인 직후(같은 초 안) | 같음 |
| A의 DPA thread | 정상 quiesce, pool 반환. `dma_error` 없음 | 반환하지 않고 계속 돎. 죽은 ring을 12초 넘게 폴링해도 오류 없음 |
| B | 계속 송수신(1.5–2.2만 msg/s) | 같음 |
| DPU 프로세스, DPA context | 살아 있음, 오류 로그 없음 | 같음 |

죽는 순간 진행 중이던 copy는 정상으로 끝났다. 죽은 뒤에는 host가 새 descriptor를 올리지 않으므로, 연결별 thread는 죽은 메모리에 새 copy를 걸지 않았다.

### 실험 2b: 죽은 프로세스의 메모리를 같은 producer로 복사할 때 (3회)

probe thread 하나가 producer 하나로 A와 B의 TX 풀을 번갈아 4 KiB씩 복사하다가(약 4만 회/s, 오류 0), 중간에 A를 `kill -9` 했다.

- A가 죽은 직후, **A의 메모리를 읽는 copy가 끝나지 않았다.** 오류 CQE도, ARM 완료 메시지도 오지 않았다. 3회 모두 같았다.
- 그 뒤 **같은 producer로 살아 있는 B의 메모리를 16번 복사해도 하나도 끝나지 않았다**(`timeouts=16`).
- **40초를 더 지켜봐도 풀리지 않았다.** ARM 완료 메시지 수(A, B 모두 227281)가 그대로였다.
- DPA context 실패는 없었다. B 자신의 연결(연결 전용 thread와 producer)은 그동안 계속 정상으로 돌았다.

### 2a·2b가 A안에 주는 뜻

- 위험은 "오류"가 아니라 **producer가 조용히, 영구히 막히는 것**이다. 막힌 copy 뒤에 걸린 일은 다른 연결 것이어도 끝나지 않는다. A안에서는 한 프로세스가 죽을 때 같은 mux thread의 연결 전부가 멈출 수 있다.
- 이 일은 **죽은 뒤에 새 copy가 걸릴 때만** 생긴다. 예를 들어 Linkerd가 느려서 staging이 차 있으면 descriptor가 ring에 남아 있고, 프로세스가 죽은 뒤 staging이 비면서 그 descriptor를 복사하게 된다. 2a에서는 이런 상황이 없어서 아무 일도 없었다. 지금 구조에서도 같은 일이 생기면 그 연결의 thread가 막히고 quiesce가 1초 뒤 시간 초과로 끝나지만, 다른 연결에는 번지지 않는다.
- 설계에 넣을 것:
  1. **메모리를 프로세스보다 오래 살리기.** broker 경로처럼 TX 메모리를 앱이 아닌 broker가 소유하고, DPU가 close를 확인할 때까지 유지하면 이 경로가 생기지 않는다. broker 없이 장치를 직접 여는 경로(OB 측정이 이 방식)가 남는 위험이다.
  2. **멈춤 감지와 복구.** 가장 오래된 진행 중 copy가 일정 시간(예: 10 ms)을 넘으면 thread가 막힌 것으로 본다. producer가 막혀 있어 메시지를 보낼 수 없으므로, DPA는 TLS에 표시하고 ARM이 그것을 읽는다. ARM은 그 thread의 연결을 오류로 끝내고, 새 thread를 다른 EU에 띄워 살아 있는 연결을 옮긴다. 막힌 thread와 producer는 정리할 수 있는지 따로 확인해야 한다.
  3. **프로세스끼리 thread를 나눠 쓰지 않기(선택).** 그러면 막혀도 죽은 프로세스 자신의 연결만 영향을 받는다. 대신 thread 수가 (프로세스, worker) 쌍의 수만큼 늘어 EU 절약 효과가 줄어든다.
