# Host broker on grpc-all: direct vs broker (jet1) — 2026-10-06

**판정:** grpc-all(main + PR #7–#11) 위로 옮긴 broker는 데이터 경로 비용이 측정되지 않았다.
- 같은 바이너리에서 direct와 broker를 번갈아 잰 다섯 케이스가 모두 −3.0~+2.2% 안이다. broker 쪽이 같거나 조금 높다.
- direct 경로는 grpc-all 그대로다(아래 기준선).
- PR #7의 idle wake도 broker를 거쳐 동작한다. 매번 잠드는 최악 조건에서 놓친 wake는 0이다. 대신 잠든 앱을 깨우는 메시지마다 broker를 한 번 더 거친다.

설계: [docs/2026-09-29_host-broker-plan.md](../docs/2026-09-29_host-broker-plan.md) 6절. 이전 측정(old main, rapids4): [2026-09-29_host-broker-ab.md](2026-09-29_host-broker-ab.md).

## 조건

| 항목 | 값 |
|---|---|
| Host | jet1, Xeon 6515P 16 CPU(SMT 없음), Ubuntu 24.04, kernel 6.8.0-142, DOCA 3.5.0098 |
| DPU | BF-3, kernel 6.8.0-1030-bluefield-64k, DOCA 3.5.0098 |
| DPU 실행 | `dpumesh_dpu`(`bc66dd96…`): `DMESH_DEV_PCI=03:00.0 DMESH_REP_PCI=0b:00.0 DMESH_SERVER_NAME=DPUMeshBR DMESH_BUSY_POLL=1 DPUMESH_DPA_EU_BASE=64`, core 12 고정, 실행마다 재시작 |
| 같은 DPU에서 함께 돈 것 | PF1(`03:00.1`/`0b:00.1`)에 ~/DPUmesh 런타임이 떠 있었다(트래픽 없음, ARM 사용률 약 9%). PF0 측정과 DPA EU 범위가 겹치지 않는다 |
| host 빌드 | 이 브랜치: `libdpumesh.so.5` `e323d38d…`, `dpumesh_broker` `a47306b6…`, `dpumesh_host` `da9dc432…`. 기준선 grpc-all `8f6ec93`: `libdpumesh.so.5` `c2bd8adb…`, `dpumesh_host` `ab4b6268…` |
| direct | `DPUMESH_BROKER=off DPUMESH_PCI_ADDR=0b:00.0`: 앱이 device를 직접 연다 |
| broker | `dpumesh_broker --listen <sock> --pci 0b:00.0`(host core 12), 앱은 `DPUMESH_BROKER=<sock>`만 설정 |
| 부하 | `dpumesh_host`, host core 4–11 고정, `DPUMESH_REVERSE=dpu-dma`, 10 s. 케이스마다 direct와 broker를 번갈아 3회 |
| 집계 | host `HOST_BENCH_DONE`, DPU 초당 `TOTAL` 줄의 중앙값(첫·끝 줄 제외). 표는 3회의 median [min–max] |

## direct vs broker

| case | metric | direct | broker | Δ median |
|---|---|---|---|---|
| sink t1 8192B w262144 | Gbps (DPU recv) | 9.63 [9.63–9.64] | 9.66 [9.66–9.67] | +0.3% |
| sink t1 64B w262144 | msg/s (host) | 7,535,483 [7,497,502–7,547,159] | 7,698,626 [7,437,509–7,715,427] | +2.2% |
| sink t1 64B w262144 | DMA/s (DPU recv) | 117,670 [117,256–117,934] | 120,213 [116,251–120,614] | +2.2% |
| echo t1 8192B w262144 | Gbps (host rx) | 9.56 [9.56–9.57] | 9.59 [9.59–9.59] | +0.3% |
| echo t1 8192B w262144 | RTT p50 us | 210 [209–210] | 207 [206–210] | -1.4% |
| echo t1 8192B w262144 | RTT p99 us | 325 [324–326] | 326 [324–327] | +0.3% |
| echo t4 8192B w262144 | Gbps (host rx) | 19.6 [19.32–19.66] | 19.86 [19.59–20.13] | +1.3% |
| echo t4 8192B w262144 | RTT p50 us | 351 [351–356] | 352 [348–354] | +0.3% |
| echo t4 8192B w262144 | RTT p99 us | 568 [562–572] | 559 [557–567] | -1.6% |
| echo t1 64B w64 | msg/s (host rx) | 27,499 [25,619–27,573] | 27,841 [27,825–27,892] | +1.2% |
| echo t1 64B w64 | RTT p50 us | 33 [33–33] | 32 [32–32] | -3.0% |
| echo t1 64B w64 | RTT p99 us | 56 [55–56] | 55 [55–55] | -1.8% |

- `sink 64B`의 host msg/s는 앱이 보낸 메시지 수다. 라이브러리가 작은 post를 8064 B 단위로 묶으므로 DPU 수치는 DMA/s다.
- `echo 64B w64`는 메시지 1개만 오가는 ping-pong이다. 기본 nap/linger에서는 ARM이 0이다.

## direct 경로 기준선 (grpc-all vs 이 브랜치)

두 빌드 모두 direct 경로(`DPUMESH_PCI_ADDR=0b:00.0`)로, 번갈아 2회씩 쟀다. 표는 2회의 median [min–max]다.

| case | metric | grpc-all `8f6ec93` | 이 브랜치 (direct) | Δ median |
|---|---|---|---|---|
| sink t1 8192B w262144 | Gbps (DPU recv) | 9.64 [9.64–9.64] | 9.63 [9.63–9.63] | -0.1% |
| echo t4 8192B w262144 | Gbps (host rx) | 19.84 [19.81–19.87] | 19.98 [19.95–20.02] | +0.7% |
| echo t4 8192B w262144 | RTT p50 us | 350.5 [349–352] | 348.5 [348–349] | -0.6% |
| echo t4 8192B w262144 | RTT p99 us | 563.5 [563–564] | 561 [561–561] | -0.4% |
| echo t1 64B w64 | msg/s (host rx) | 27,540 [27,497–27,584] | 27,616 [27,569–27,663] | +0.3% |
| echo t1 64B w64 | RTT p50 us | 33 [33–33] | 33 [33–33] | +0.0% |
| echo t1 64B w64 | RTT p99 us | 55.5 [55–56] | 55.5 [55–56] | +0.0% |

구조체를 `channel_internal.h`로 옮기고 ARM 송신부를 나눈 것은 direct 경로에 차이를 만들지 않는다.

## idle wake

`DPUMESH_NAP_US=0 DPUMESH_LINGER_US=0`으로 빈 EQ가 곧바로 잠들게 했다. 64 B ping-pong(창 64 B)에서는 응답마다 ARM과 DOORBELL을 거친다. 5 s, 1회.

| 경로 | msg/s | RTT p50 / p99 µs | ARM / DOORBELL | backstop wake / 그중 일을 찾은 것 |
|---|---:|---:|---:|---:|
| direct | 40,872 | 16 / 81 | 193,529 / 193,529 | 0 / 0 |
| broker | 18,278 | 47 / 83 | 91,285 / 91,284 | 0 / 0 |

- broker도 wake를 놓치지 않는다. 남은 DOORBELL 1개는 끝날 때 대기 중이던 ARM이다.
- 잠든 앱을 깨우는 메시지마다 broker가 한 번 더 깨어난다(DOORBELL → broker → eventfd → 앱). 이 최악 조건에서는 응답마다 약 30 µs가 늘었다.
- 기본 설정(nap 10–100 µs, linger 1 ms)에서는 연속 트래픽이 잠들지 않는다. 위 표의 64 B ping-pong도 ARM이 0이고 두 경로가 같다. 비용은 1 ms 넘게 쉰 뒤의 첫 메시지에만 든다.

## 선택 규칙과 장애 시나리오

| 확인 | 결과 |
|---|---|
| `DPUMESH_BROKER` 없음, 기본 소켓 없음 | direct로 열고 정상 실행 |
| `DPUMESH_BROKER` 없음, `/run/dpumesh/broker.sock`에 broker | broker client로 열고 정상 실행. broker SIGTERM 뒤 소켓 제거됨 |
| 앱 SIGKILL (echo 2 thread) | broker child가 flow 2개를 DPU와 닫고 `done (clean)`. 다음 client 정상 |
| broker child SIGKILL | 앱이 `Broker connection lost`를 남기고 두 worker가 에러 이벤트로 종료(`failed=2`). DPU도 flow 2개를 닫음. 다음 client 정상 |
| broker 데몬 SIGTERM | child가 정리하고 `done (clean)`, 소켓 제거. 앱은 에러로 종료 |

## 재현

```sh
make lib broker && ninja -C src/transport/build && ninja -C apps/dma_bench/build   # host
# DPU: src/transport, apps/dma_bench 를 meson으로 빌드 (linkerd2-proxy/linkerd/doca/src/shim.c 필요)
DMESH_DEV_PCI=03:00.0 DMESH_REP_PCI=0b:00.0 DMESH_SERVER_NAME=DPUMeshBR DMESH_MODE=<sink|echo> \
  DMESH_BUSY_POLL=1 DPUMESH_DPA_EU_BASE=64 taskset -c 12 apps/dma_bench/build/dpumesh_dpu        # DPU, root
build/bin/dpumesh_broker --listen /tmp/broker.sock --pci 0b:00.0                                 # host (broker)
DPUMESH_BROKER=/tmp/broker.sock DPUMESH_SERVER=DPUMeshBR DPUMESH_POD_IP=10.99.0.1 \
  DPUMESH_REVERSE=dpu-dma BENCH_MODE=echo BENCH_THREADS=4 taskset -c 4-11 apps/dma_bench/build/dpumesh_host
```

direct는 `DPUMESH_BROKER=off DPUMESH_PCI_ADDR=0b:00.0`로 실행한다. 회차별 값은 [CSV](2026-10-06_host-broker-grpc-ab.csv)에 있다(`path` 열: `direct`, `broker`, 기준선은 `grpcall`, `branch`).
