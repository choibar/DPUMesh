# DPA pod 다중화 — forward ring을 pod 단위 DPA thread로

상태: **구현됨, 기본값은 꺼짐(`DMESH_DPA_MUX` 미설정).** host 단위 시험과 DPA kernel 모의 시험(`tests/dpa_poll_test.py`)을 통과했다. 하드웨어 측정은 아래 "측정"의 계획대로 한다. 사전 측정은 [`2026-10-13_dpa-mux-probe.md`](2026-10-13_dpa-mux-probe.md).

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
- **tail 표(TAILS).** host 라이브러리는 session을 열 때 128 B(cache line 두 개) 표를 만들어 PCI로 내보내고, HELLO_ACK 바로 뒤 첫 OPEN보다 먼저 `TAILS`(session protocol v9)로 보낸다. flow는 descriptor를 쓰고 ring의 `producer_tail`을 올린 뒤, 같은 값의 하위 32비트를 자기 칸 `tails[flow id - 1]`에 쓴다. flow를 열기 전에 칸을 0으로 둔다(같은 id의 이전 flow는 CLOSED까지 끝났으므로 DPA가 읽지 않는다). DPU는 표의 export를 session에 두고 flow마다 worker로 넘긴다. thread는 첫 flow 때 표를 import하고 line마다 buf 하나로 DPA에 맡긴다. ring ctrl의 `producer_tail`은 연결마다 thread에서 그대로 쓰고, `consumer_head`, `completed_bytes`, `error`는 다중화에서도 ring ctrl에 있다.
- **바퀴(`run_dma_mux`, `device/dpa_kernel.c`).** CQE를 처리하고, `ctl_seq`가 바뀌었으면 명령을 다시 읽고, 닫히는 ring을 정리한 뒤, 열린 ring이 있는 tail line만 읽는다. 열린 ring마다 `tail − submit`(하위 32비트 차)가 0이면 건너뛰고, 아니면 descriptor를 최대 32개 복사한다. ring은 1024칸이라 차이는 정확하고, ring 크기보다 크면 그 ring만 `TAIL` 오류로 멈춘다. 시작 ring은 바퀴마다 하나씩 돈다. 20 ms 돌고 retrigger한다.
- **메시지.** thread의 producer 하나에 worker마다 consumer가 하나 있다. ring의 설정에 그 ring을 가진 worker의 consumer id가 있고, copy의 DMA_DONE은 그 worker로 간다. credit은 consumer마다이므로, receive가 비어 있는 worker는 자기 ring만 건너뛴다. 바퀴 전체를 멈추는 것은 FIFO(진행 중 copy 128개)가 가득 찼을 때뿐이다. completion은 producer 하나라 제출 순서대로 오고, FIFO 맨 앞이 그 copy다. 연결의 `consumer_head`(custody ACK)는 그 연결의 copy가 끝난 뒤에만 오른다.
- **DPA 상태(TLS, `struct dpa_mux_ctx`, 약 8.2 KB).** line마다 쓰는 쪽이 하나다.
  - 헤더 line 0(ARM): producer와 completion handle, tail 표 buf, `ctl_seq`, `stop`.
  - 헤더 line 1–2(DPA): 바퀴 수, tail 읽기, 방문, copy, credit 부족, FIFO 가득 참, 오류 CQE, FIFO 위치, 열린 ring과 닫히는 ring의 bitmask, `stopped`.
  - ring마다 상태 line 하나(DPA): `pos`, `submit`, `complete`, `completed_bytes`, `inflight`, `failed`, `state`, `error`. 점검은 헤더와 이 32줄을 한 번에 읽는다.
  - FIFO 128칸, ring마다 설정 line 두 개(ARM): ring buf_arr, 두 mmap, staging, ring 크기, TX 풀 범위, slot, generation, consumer, `cmd`, `rx_consumed_pos`.
- **ring 열기.** mmap import, `buf_arr`, staging까지는 연결마다 thread와 같다. 그 뒤 session의 thread를 찾거나 새로 띄우고(표 import, thread, completion, notification, msgq, 이 worker의 consumer, TLS, run, notification RPC로 첫 실행), 이 worker에 그 session의 consumer가 없으면 만든다.
  1. ARM이 `cmd = NONE`을 쓴다. cmd가 NONE인 ring은 DPA가 건드리지 않는다.
  2. 설정 두 line을 쓰고, 상태 line을 0(`IDLE`)으로 쓴다.
  3. `cmd = OPEN`을 쓰고 `ctl_seq`를 올린다. 각 `h2d_memcpy`는 다음 것이 시작하기 전에 끝난다.
  - DPA는 다음 바퀴에 OPEN과 IDLE을 보고 ACTIVE로 바꾸고 열린 ring에 넣는다. 연결은 OPEN을 쓰자마자 `RUNNING`으로 간다.
- **ring 닫기.** ARM이 `cmd = CLOSE`를 쓰고 `ctl_seq`를 올린다. DPA는 그 ring을 열린 ring에서 빼고, 진행 중 copy가 0이 되면 `state = CLOSED`로 바꾼다. ARM은 상태 line을 읽어, CLOSED이고 `받은 DMA_DONE 수 + failed == submit`일 때 끝난 것으로 본다. 그 worker의 마지막 ring이면 consumer를 그 worker의 PE에서 멈추고 없앤다. 그다음 DMA task, `buf_arr`, mmap, staging을 해제하고 ring을 돌려준다. session의 마지막 ring이면 thread를 멈추고(`stop`, `stopped`) 모든 객체를 없앤다. 실패한 해제는 점검이 다시 시도한다. 1초 안에 닫히지 않으면 quiesce 시간 초과와 같이 다루고, ring은 남겨 둔다.
- **검사.** descriptor 크기, tail이 거꾸로 가거나 ring보다 앞서는지, `addr`가 그 연결의 TX 풀 범위 안인지 본다. 어긋나거나 copy가 실패하면 그 ring만 `state = ERROR`가 되고, 오류 값은 host ring의 `ctrl.error`에도 남는다.
- **점검(`dmesh_dpa_mux_watch`).** 모든 worker가 advance마다 부르고, 때가 된 첫 worker가 thread 하나를 점검한다(나머지는 lock을 얻지 못하면 바로 돌아간다). thread마다 약 1초에 한 번이다. 점검은 동기 `d2h_memcpy` 하나(약 1 ms)이고 그동안 그 worker는 flow를 처리하지 않으므로, 점검을 한꺼번에 하지 않고 1초에 나눠 한다.
  - ring이 ERROR면 그 ring을 가진 worker가 다음 advance에 그 연결을 EIO로 닫는다. 메시지 방식보다 알아채는 데 최대 1초 늦다.
  - 바퀴 수가 그대로면 "is not running", 가장 오래된 copy가 `DMESH_DPA_MUX_STALL_MS`를 넘으면 "is stalled" ERR 로그를 남긴다.

## 알려진 한계

- **죽은 프로세스의 메모리를 복사하면 thread가 영구히 막힌다.** probe 실험 2b에서, copy가 오류도 완료도 없이 끝나지 않았고 같은 producer의 뒤 작업도 전부 멈췄다. thread가 pod 하나만 맡으므로 함께 멈추는 것은 죽은 그 프로세스의 flow뿐이다. 그 thread를 되살리는 일은 broker 병합 때 한다(`feature/doca-broker`, `channel_broker.c`).
- **ring 오류는 1초 점검에서 처리한다.** 그 사이 그 연결은 멈춰 있고, 같은 thread의 다른 ring은 계속 돈다.
- **thread의 tail 표와 msgq는 첫 flow를 받은 worker의 device로 만든다.** 프로세스가 끝날 때 말고는 worker가 먼저 떠나지 않으므로 문제가 되지 않는다.
- **벤치마크 안내:** 다중화를 켠 채로 측정 중 host 프로세스를 `kill -9` 하지 않는다. DPU 쪽은 실행마다 다시 띄운다.

## 측정

다음을 같은 날 연결마다 thread, worker마다 thread를 두는 다중화(thread 7개, thread당 ring 8개), pod 다중화로 비교한다.

1. 정확성: OB 32 pod로 연결 수백 개를 열고 닫기, 오류 로그와 "not running"이 없는지.
2. 공정성: host 프로세스 둘(하나는 64B echo를 창 4096으로 밀어 넣고, 하나는 창 64로 왕복 지연만 잰다). worker마다 thread를 두면 두 프로세스의 ring이 같은 thread에 놓일 수 있고, pod 다중화에서는 놓이지 않는다. 지연 쪽의 p50/p99를 본다.
3. polling: `DMESH_DPA_MUX_STATS=1`의 바퀴당 host 읽기. worker마다 thread를 두는 다중화는 바퀴마다 열린 ring 수만큼 ring ctrl을 읽고, pod 다중화는 tail line 1–2개를 읽는다.
