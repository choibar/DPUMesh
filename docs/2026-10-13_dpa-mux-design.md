# DPA pod 다중화 — forward ring을 pod 단위 DPA thread로

상태: **구현됨, 기본값은 꺼짐(`DMESH_DPA_MUX` 미설정).** host 단위 시험, DPA kernel 모의 시험(`tests/dpa_poll_test.py`), jet2-dpu PF1의 마이크로벤치와 Online Boutique 측정을 통과했다(아래 "측정"). 사전 측정은 [`2026-10-13_dpa-mux-probe.md`](2026-10-13_dpa-mux-probe.md).

연결마다 DPA thread를 하나씩 EU에 올리면 EU 190개가 flow 수의 상한이 된다. 다중화는 DPA thread 하나가 여러 연결의 forward ring을 번갈아 처리하게 한다. 이때 thread 하나는 **한 host session(= pod 하나의 프로세스)의 ring만** 맡는다. 그래서 한 pod의 ring은 같은 pod의 copy 뒤에서만 기다리고, 다른 pod의 부하가 그 pod의 지연이나 처리량을 가져가지 않는다. 끝나지 않는 copy가 thread를 막아도 멈추는 것은 그 pod의 flow뿐이다.

thread는 ring마다 host 메모리의 ring ctrl을 읽는 대신, session이 내보낸 **tail 표**를 읽는다. 표는 flow 칸마다 u32 하나이고 16칸이 cache line 하나에 들어가므로, PCIe 읽기 한 번으로 ring 16개에 새 descriptor가 있는지 안다. 새 descriptor가 있는 ring만 방문한다. descriptor 형식, custody 의미, Linkerd는 바꾸지 않는다.

## 켜는 법

| 변수 | 뜻 |
|---|---|
| `DMESH_DPA_MUX=n` | session마다 DPA thread 하나. 프로세스 전체에서 동시에 최대 n개(1–128). OB 32 pod는 32개를 쓰므로 64면 넉넉하다. 미설정이나 0이면 연결마다 thread |
| `DMESH_DPA_MUX_STALL_MS` | 가장 오래된 진행 중 copy가 이보다 오래되면 그 thread를 멈춘 것으로 보고 ERR 로그(기본 100) |
| `DMESH_DPA_MUX_STATS=1` | thread별 초당 한 줄: 바퀴 수, tail line 읽기 수와 바퀴당 읽기, 방문한 ring 수와 바퀴당 방문, copy 수, credit 부족, FIFO 가득 참, 진행 중 copy, 오류 CQE와 메시지 |
| `DMESH_DPA_PIN_EU_BASE=b` | 측정용. thread i를 EU `b + i`에 고정 |

session으로 다중화된 연결(지금의 host 라이브러리)에만 적용한다. 예전 방식의 연결과 host-dpa 역방향은 연결마다 thread를 쓴다.

## 구조

- **pod와 ring.** pod는 host session 하나다. dispatcher가 붙인 session id와 epoch로 구분하고, dispatcher가 없는 DPU 프로세스(`dpumesh_dpu`)에서는 worker의 session으로 구분한다. session의 flow id는 1–32이므로 thread의 ring r은 flow r + 1이다. 한 pod의 flow는 여러 worker에 놓이고(백엔드 flow는 worker마다 하나), 그 flow들이 모두 같은 thread의 ring이다.
- **tail 표(TAILS).** host 라이브러리는 session을 열 때 128 B(cache line 두 개) 표를 만들어 PCI로 내보내고, HELLO_ACK 바로 뒤 첫 OPEN보다 먼저 `TAILS`(session protocol v9)로 보낸다. flow는 descriptor를 쓰고 ring의 `producer_tail`을 올린 뒤, 같은 값의 하위 32비트를 자기 칸 `tails[flow id - 1]`에 쓴다. flow를 열기 전에 칸을 0으로 둔다(같은 id의 이전 flow는 CLOSED까지 끝났으므로 DPA가 읽지 않는다). DPU는 표의 export를 session에 두고 flow마다 worker로 넘긴다. thread는 첫 flow 때 표를 import하고 line마다 buf 하나로 DPA에 맡긴다. ring ctrl의 `producer_tail`은 연결마다 thread에서 그대로 쓰고, `consumer_head`, `completed_bytes`, `error`는 다중화에서도 ring ctrl에 있다. host broker가 device를 가진 pod에서는 broker가 표를 memfd로 만들어 내보내고, 앱은 HELLO 응답으로 받은 그 memfd를 map해서 칸을 쓴다(`docs/2026-09-29_host-broker-plan.md` 8절).
- **바퀴(`run_dma_mux`, `device/dpa_kernel.c`).** CQE를 처리하고, `ctl_seq`가 바뀌었으면 명령을 다시 읽고, 닫히는 ring을 정리한 뒤, 열린 ring이 있는 tail line만 읽는다. 열린 ring마다 `tail − submit`(하위 32비트 차)가 0이면 건너뛰고, 아니면 descriptor를 최대 32개 복사한다. ring은 1024칸이라 차이는 정확하고, ring 크기보다 크면 그 ring만 `TAIL` 오류로 멈춘다. 다음 바퀴는 직전 바퀴가 마지막으로 서비스한 ring 다음부터, FIFO가 차서 멈췄으면 그 ring부터 시작한다. 그래서 일이 있는 ring은 열린 ring이 몇 개든 차례대로 첫 순서를 받는다. 20 ms 돌고 retrigger한다.
- **메시지.** thread의 producer 하나에 worker마다 consumer가 하나 있다. ring의 설정에 그 ring을 가진 worker의 consumer id가 있고, copy의 DMA_DONE은 그 worker로 간다. credit은 consumer마다이므로, receive가 비어 있는 worker는 자기 ring만 건너뛴다. 바퀴 전체를 멈추는 것은 FIFO(진행 중 copy 128개)가 가득 찼을 때뿐이다. completion은 producer 하나라 제출 순서대로 오고, FIFO 맨 앞이 그 copy다. 연결의 `consumer_head`(custody ACK)는 그 연결의 copy가 끝난 뒤에만 오른다.
- **DPA 상태(TLS, `struct dpa_mux_ctx`, 약 8.2 KB).** line마다 쓰는 쪽이 하나다.
  - 헤더 line(ARM): producer와 completion handle, tail 표 buf, `ctl_seq`, `stop`, 상태를 내보낼 ARM 메모리의 mmap handle과 주소.
  - `struct dpa_mux_status`(DPA): 바퀴 수, tail 읽기, 방문, copy, credit 부족, FIFO 가득 참, 오류 CQE의 line, FIFO 위치, 열린 ring과 닫히는 ring의 bitmask, 가장 오래된 copy의 나이, `stopped`의 line, ring마다 상태 line 하나(`pos`, `submit`, `complete`, `completed_bytes`, `inflight`, `failed`, `state`, `error`).
  - **상태 내보내기.** ARM은 DPA의 쪽을 thread의 local storage에서 `d2h_memcpy`로 읽지 않는다. thread마다 ARM 메모리에 `struct dpa_mux_status`를 하나 두고, DPA가 mmap external pointer와 window writeback으로 거기에 쓴다(host ring ctrl에 `consumer_head`를 쓰는 것과 같은 길). 앞의 두 line은 activation마다 끝에, 그리고 멈추기 직전에 쓰고, ring의 line은 그 ring이 ERROR나 CLOSED가 될 때 쓴다. ARM은 ring을 열 때 그 line을 0으로 둔다. 점검과 ring 닫기는 이 메모리만 읽으므로, 읽는 동안 worker가 멈추지 않는다.
  - FIFO 128칸, ring마다 설정 line 두 개(ARM): ring buf_arr, 두 mmap, staging, ring 크기, TX 풀 범위, slot, generation, consumer, `cmd`, `rx_consumed_pos`.
- **ring 열기.** mmap import, `buf_arr`, staging까지는 연결마다 thread와 같다. 그 뒤 session의 thread를 찾거나 새로 띄우고(표 import, thread, completion, notification, msgq, 이 worker의 consumer, TLS, run, notification RPC로 첫 실행), 이 worker에 그 session의 consumer가 없으면 만든다.
  1. ARM이 `cmd = NONE`을 쓴다. cmd가 NONE인 ring은 DPA가 건드리지 않는다.
  2. 설정 두 line을 쓰고, 상태 line을 0(`IDLE`)으로 쓴다.
  3. `cmd = OPEN`을 쓰고 `ctl_seq`를 올린다. 각 `h2d_memcpy`는 다음 것이 시작하기 전에 끝난다.
  - DPA는 다음 바퀴에 OPEN과 IDLE을 보고 ACTIVE로 바꾸고 열린 ring에 넣는다. 연결은 OPEN을 쓰자마자 `RUNNING`으로 간다.
- **ring 닫기.** ARM이 `cmd = CLOSE`를 쓰고 `ctl_seq`를 올린다. DPA는 그 ring을 열린 ring에서 빼고, 진행 중 copy가 0이 되면 `state = CLOSED`로 바꾸고 그 line을 내보낸다. ARM은 내보낸 line이 CLOSED이고 `받은 DMA_DONE 수 + failed == submit`일 때 끝난 것으로 본다. 그 worker의 마지막 ring이면 consumer를 그 worker의 PE에서 멈추고 없앤다. 그다음 DMA task, `buf_arr`, mmap, staging을 해제하고 ring을 돌려준다. session의 마지막 ring이면 thread를 멈추고(`stop`, 내보낸 `stopped`) 모든 객체를 없앤다. 실패한 해제는 점검이 다시 시도한다. 1초 안에 닫히지 않으면 quiesce 시간 초과와 같이 다루고, ring은 남겨 둔다.
- **consumer 정리.** worker의 consumer는 그 worker가 thread에 가진 마지막 ring이 닫히면 멈추고 없앤다. Comch consumer는 idle이 되는 순간 SDK 객체가 끝나고, 멈출 때 걸어 둔 receive가 flush되면서 이벤트를 하나 올린다. 그래서 `doca_ctx_stop` 앞에서 PE에 쌓인 이벤트를 읽고, stop 직후 PE를 progress하기 전에 한 번 더 읽는다(`dmesh_pe_take_events`). 이벤트가 남은 채 객체가 끝나면 다음 `doca_pe_request_notification`이 해제된 객체를 건드린다.
- **검사.** descriptor 크기, tail이 거꾸로 가거나 ring보다 앞서는지, `addr`가 그 연결의 TX 풀 범위 안인지 본다. 어긋나거나 copy가 실패하면 그 ring만 `state = ERROR`가 되고, 오류 값은 host ring의 `ctrl.error`에도 남는다.
- **점검(`dmesh_dpa_mux_watch`).** 모든 worker가 advance마다 부르고, 때가 된 첫 worker가 thread 하나를 점검한다(나머지는 lock을 얻지 못하면 바로 돌아간다). thread마다 약 1초에 한 번이다. 점검은 그 thread가 내보낸 상태를 ARM 메모리에서 읽는다.
  - ring이 ERROR면 그 ring을 가진 worker가 다음 advance에 그 연결을 EIO로 닫는다. 메시지 방식보다 알아채는 데 최대 1초 늦다.
  - 바퀴 수가 그대로면 "is not running", 가장 오래된 copy가 `DMESH_DPA_MUX_STALL_MS`를 넘으면 "is stalled" ERR 로그를 남긴다.

## 알려진 한계

- **죽은 프로세스의 메모리를 복사하면 thread가 영구히 막힌다.** probe 실험 2b에서, copy가 오류도 완료도 없이 끝나지 않았고 같은 producer의 뒤 작업도 전부 멈췄다. thread가 pod 하나만 맡으므로 함께 멈추는 것은 죽은 그 프로세스의 flow뿐이다. 그 thread를 되살리는 일은 broker 병합 때 한다(`feature/doca-broker`, `channel_broker.c`).
- **ring 오류는 1초 점검에서 처리한다.** 그 사이 그 연결은 멈춰 있고, 같은 thread의 다른 ring은 계속 돈다.
- **thread의 tail 표와 msgq는 첫 flow를 받은 worker의 device로 만든다.** 프로세스가 끝날 때 말고는 worker가 먼저 떠나지 않으므로 문제가 되지 않는다.
- **벤치마크 안내:** 다중화를 켠 채로 측정 중 host 프로세스를 `kill -9` 하지 않는다. DPU 쪽은 실행마다 다시 띄운다.

## 측정

2026-11-06, jet2(SMT off) ↔ jet2-dpu PF1. 마이크로벤치는 `dpumesh_host` ↔ `dpumesh_dpu`(echo), 64 B 메시지, 연결마다 창 4 KiB(포화) 또는 64 B(한 번에 하나), 실행마다 DPU 쪽을 새로 띄우고 각 구성을 2번씩 번갈아 돌렸다. "worker마다 thread"는 같은 날의 4fc2153(worker마다 thread n개, 새 flow는 ring이 가장 적은 thread로)이다. DPU 지표는 `DMESH_DPA_MUX_STATS=1`의 초당 줄의 중앙값이다. 자료는 `microservices-demo/mesh-bench/results/podmux-20261106-micro-fix2`(OB는 `dpumesh-20261106-smtoff-podfix`).

**공정성.** pod B(연결 1개, 메시지 하나씩 왕복)를 혼자, 다음에 pod A(연결 7개, 창 4 KiB) 옆에서 돌린다. worker마다 thread는 `DMESH_DPA_MUX=1`이라 A와 B의 ring이 한 thread에 있다.

| | B 혼자: 처리량 / p50 / p99 | A 옆의 B: 처리량 / p50 / p99 | A의 처리량 |
|---|---|---|---|
| worker마다 thread | 22.2–22.4k msg/s / 39 µs / 158–163 µs | 856 msg/s / 485 µs / 3,446–3,449 µs | 218.8k msg/s |
| pod마다 thread | 22.9–23.0k / 38 / 161 | 21.4–21.6k / 34 / 201–204 | 206.1k |

- worker마다 thread에서 B는 처리량이 1/26, p99가 21배가 된다. A의 ring 7개가 바퀴마다 각각 32개씩 복사하는 동안 B의 ring이 차례를 기다린다.
- pod마다 thread에서 B는 p50이 그대로, p99 +40 µs, 처리량 −6%다. 남은 차이는 같은 DPU worker와 DMA 엔진을 나눠 쓰는 몫이다. A는 6% 낮다.

**polling.** 연결 8개(한 프로세스), 포화.

| | 처리량 | RTT p50 / p99 | 바퀴/s | ring 확인 읽기/바퀴 | ring 확인 읽기/s |
|---|---|---|---|---|---|
| worker마다 thread(thread 1개, ring 8개) | 219.8–219.9k msg/s | 2.22–2.25 / 3.86 ms | 1,717–1,719 | 8 | 13,736–13,752 |
| pod마다 thread | 209.7–209.8k | 2.44 / 2.61–2.62 ms | 1,679–1,680 | 1 | 1,679–1,680 (−88%) |

- 바퀴마다 일이 있던 ring은 5.1개다. 일이 없으면 바퀴마다 1번 읽고 초당 약 38만 바퀴를 돈다. ring 16개도 1번이다.
- 처리량은 5% 낮고 p99는 32% 낮다.

**한 pod의 처리량은 thread 하나로 묶인다.** 연결 16개(한 프로세스)에서 worker마다 thread(thread 2개, 각 ring 8개)는 434.6k msg/s, pod마다 thread는 209.7k msg/s다. 한 pod가 thread 하나의 copy 속도(64 B에서 약 21만 msg/s)를 넘게 보내면 pod마다 thread가 느리다.

**Online Boutique.** SMT off, 32 pod, 2.3 GHz, uncore 고정, DPU worker 15개, `DMESH_DPA_MUX=64`(thread 32개). 500–6,000 req/s에서 drop이 없다. host busy core는 같은 날 sidecar 없는 구성보다 2–4% 적다.

| req/s | host / DPU busy core | p50 |
|---|---|---|
| 500 | 5.04 / 4.83 | 17.0 ms |
| 1,000 | 8.41 / 7.65 | 16.9 ms |
| 1,500 | 10.74 / 10.36 | 18.2 ms |
| 2,000 | 12.00 / 11.69 | 20.1 ms |
| 3,000 | 13.98 / 14.00 | 28.2 ms |
| 6,000 | 15.94 / 14.58 | 201.2 ms |

- 같은 시각 PF0에서 다른 프로세스가 DPA thread 약 1,040개를 잡고 있었다. worker마다 thread는 시작 때 만드는 thread 105개 중 일부가 `DOCA_ERROR_DRIVER`로 실패했고(mux worker 15개 중 6개), pod마다 thread는 pod가 올 때 32개만 만들어 모두 떴다.
