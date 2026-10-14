# Host 대기 정책과 dpu-dma doorbell — 2026-09-30

상태: **구현(미커밋), 실장비 검증 중.** 기준은 `feature/online-boutique`(main `b607129` 기반)의 현재 host 구조다.
앱 process가 DOCA device를 직접 열고, process마다 channel 하나와 공유 Comch control session 하나를 쓴다.
host broker(`feature/doca-broker`)는 이후 PR이며 이 설계의 범위가 아니다. broker가 들어올 때 바뀌는 곳은 10절에 적었다.

## 1. 문제

측정: `bench-results/2026-09-30_online-boutique-fixes.*`, 원자료 `build/hw-ab-2026-09-30/perf/after-eu64-r7`.

- **idle 비용.** 열린 flow가 하나라도 있으면 EQ가 50 µs tick으로 계속 깨어난다. process마다 초당 약 2만 번이다.
  - Online Boutique의 gRPC 연결과 server의 backend pool은 늘 열려 있으므로, tick은 사실상 멈추지 않는다.
  - 사용자 8명일 때 Go가 아닌 서비스의 DPUMesh reactor thread만 합쳐 약 0.9 core였다. host 전체 3.30 core의 약 30%다.
    - currency 0.27, cart 0.26, ad 0.13, recommendation 0.10, email 0.09, payment 0.09 core.
    - 이 thread들은 모두 초당 약 2만 번 context switch한다.
- **수신 지연.** dpu-dma 수신은 tick이 올 때까지 보이지 않는다. 수신 한 번에 평균 약 25 µs, 최대 50 µs가 더해진다.
  - DPUmesh는 고정 50 µs 대기가 25 µs보다 p50을 약 100 µs 늘린다고 측정했다(`~/DPUmesh/bench/report/data/doorbell-relay-20260901`).
- **정책이 흩어짐.**
  - spin window와 tick은 core에 있다.
  - main `8d5dd9b`는 Go 어댑터 안에서만 dpu-dma의 EQ fd를 우회하고 Go timer로 폴링한다.
  - 이 호스트(Go 1.27.1)에서 128 µs Go timer는 실제로 약 1.07 ms 뒤에 깨어난다.

이 설계가 **풀지 않는 것:** RPC 하나에 약 0.8 ms를 더하는 DPU proxy의 L7 처리. RPS 병목의 가장 큰 항이지만 DPU 쪽 문제다(11절).

## 2. 현재 구조

```text
 앱 runtime thread (JS main, Kestrel, Netty, grpcio, goroutine)
      ▲ 언어별 전달
 EQ 대기 루프: Go native.go / C++ dmesh_reactor.cc / preload dispatcher
      │ 약속: "EQ fd가 readable이면 dmesh_poll_eq()가 0을 줄 때까지 poll"
      ▼
 core  dmesh_poll_eq(): 비면 dpumesh_eq_arm() → 한 번 더 drain → 0이면 호출자가 잠듦
       EQ fd = epoll { notify eventfd, tick timerfd(50 µs 주기), tail timerfd,
                       spare stripe 묶음, 자기 stripe의 doorbell(host-dpa reverse PE) }
      ▼
 carrier  dmesh_native_stripe_arm(): 열린 slot이면 무조건 tick=1
      ▼
 channel  알림이 없는 세 가지
   (a) push 수신: conn->descs[seq]를 메모리에서 비교          channel_conn_rx_next
   (b) custody ACK: DPA가 쓴 ctrl->consumer_head를 읽음         channel_conn_consumed
   (c) control session: Comch PE를 progress해야 메시지가 들어옴  channel_dev_progress
       (PE의 notification fd는 어느 EQ에도 등록돼 있지 않음)
```

tick이 이 세 가지를 모두 떠맡고 있어서 멈출 수 없다.

## 3. 원칙

1. **대기 정책은 core 한 곳에만 둔다.** "언제 다시 poll할지"는 EQ fd가 readable이 되는 시점으로만 표현한다.
2. **어댑터 약속은 그대로 둔다.** Go(C 루프 또는 netpoller), C++ reactor, preload dispatcher는 수정하지 않는다.
   - 세 루프는 runtime마다 달라야 하는 얇은 glue다. 합칠 대상이 아니다. 앞서 "루프를 통합해야 한다"고 한 것은 이렇게 바로잡는다: 정책이 core에 있으면 충분하다.
3. **알림이 없는 세 가지 (a)(b)(c)에 각각 깨우는 수단을 준다.** 그러면 한가할 때 완전히 잠들 수 있다.

## 4. 대기 정책 (core)

EQ마다 상태 기계를 둔다. `dmesh_poll_eq()`가 비어서 `dpumesh_eq_arm()`에 들어올 때 다음 대기를 정한다.

```text
            일을 찾음(drain>0, 이벤트 반환, 이 EQ QP의 send)
   ┌───────────────────────────────────────────────┐
   ▼                                               │
 ACTIVE ──빈 poll──▶ NAP: one-shot timer = nap, nap *= 2 (10→20→40→80 µs, 상한 100 µs)
                      │ nap이 상한을 넘음
                      ▼
                    LINGER: 상한 간격 nap을 마지막 활동 후 linger까지 계속
                      │ linger 지남, custody 없음
                      ▼
                    ARMED: doorbell 켜기 → 한 번 더 drain → 잠듦 (backstop timer만)
```

- **timer:** `tick_fd`를 주기 timer에서 one-shot timer로 바꿔 nap과 backstop 둘 다에 쓴다.
  - timerfd는 hrtimer라 EQ fd 안에서 µs 단위로 정확히 깨어난다.
  - Go netpoller가 epoll을 ms 단위 timeout으로 기다려도, fd가 readable이 되면 바로 반환한다. 그래서 main 방식의 Go도 같은 정밀도를 얻는다.
- **custody가 남아 있으면 ARMED로 가지 않는다.** 소유 stripe에 custody ticket이나 FIN이 남아 있으면 상한 간격 nap을 계속한다.
  - custody는 DPA가 descriptor를 가져가는 즉시(수~수십 µs) 풀리므로 이 구간은 짧다.
  - 대안인 ACK doorbell은 11절에 적었다.
- **oversubscription 보호:** 살아 있는 EQ 수가 이 process가 쓸 수 있는 CPU 수(`sched_getaffinity`)보다 많으면 nap과 linger를 건너뛰고 바로 ARMED로 간다. DPUmesh와 같은 규칙이다.
- **assist 규칙:** 같은 channel의 다른 EQ가 방금 일을 처리했다면 ARMED로 가지 않고 상한 간격 nap을 유지한다. 다만 nap을 최소로 되돌리지는 않는다(DPUmesh와 같음).
- **TX tail timer(`tail_fd`)는 그대로 둔다.**
- **host-dpa:** 수신은 이미 reverse PE doorbell이 있으므로 ARMED에서 그 doorbell을 켠다. 지금과 같다. (b)(c)는 dpu-dma와 같이 처리한다.

| 환경변수 | 기본값 | 뜻 |
|---|---|---|
| `DPUMESH_NAP_US` | 10 | 첫 nap |
| `DPUMESH_NAP_CAP_US` | 100 | nap 상한 |
| `DPUMESH_LINGER_US` | 측정으로 결정 (후보 0 / 1000 / 10000) | 상한 nap을 유지하는 시간 |
| `DPUMESH_BACKSTOP_MS` | 200 | 잠든 EQ의 안전망. 이것으로 일을 찾으면 경고를 센다 |

`DPUMESH_SPIN_US`와 `DPUMESH_TICK_US`는 없앤다. 설정되어 있으면 한 번 경고한다.

**linger가 필요한 이유.** doorbell로 깨우는 것은 폴링보다 느리다.
- 우리 저장소의 9월 22일 실험에서 즉시 잠들기는 요청 간격 1 ms에서 RTT +80 µs, 10 ms에서 +250 µs였다. deep C-state에서 깨어나는 비용 때문이다(memory `push-doorbell-results`).
- 우리 L7 RPC 왕복은 약 1 ms다. linger가 0이면 요청 1개씩 주고받을 때 매 응답을 doorbell로 받게 된다.
- linger는 "한가할 때 CPU"와 "첫 응답 지연"을 맞바꾸는 손잡이다. 기본값은 8절 측정으로 정한다.

## 5. dpu-dma doorbell 프로토콜

### wire (session protocol version 1 → 2)

| 방향 | type | flow_id / generation | payload |
|---|---|---|---|
| host → DPU | `DMESH_SESSION_ARM = 9` | 0 / 0 | `epoch u64`, `n u32`, `n × {flow_id u32, generation u32, expected_seq u64}` (32 flow면 16 + 32×16 = 528 B, 최대 payload 2048 B 이내) |
| DPU → host | `DMESH_SESSION_DOORBELL = 10` | 0 / 0 | `epoch u64` |

- **version을 올린다.** 지금 DPU는 version 1만 받고, 모르는 type이나 `flow_id == 0`인 메시지가 오면 session을 끊는다. 2로 올리면 버전이 다른 조합은 HELLO에서 바로 실패한다.
- `dmesh_session_header_valid()`는 ARM과 DOORBELL에 한해 `flow_id == 0`을 허용한다.

### host (channel 계층)

ARMED로 들어갈 때 `channel_dev_arm(dev)`를 부른다. `session_lock` 안에서 다음을 한다.

1. 열린 push flow마다 `expected`(host가 다음에 볼 descriptor seq)를 스냅샷한다.
2. `epoch`를 올려 ARM을 보낸다.
   - 비동기로 보내고 완료를 기다리지 않는다. 9월 22일 실험에서 slot마다 완료를 기다리다 CPU 65%와 +80 µs를 잃었다.
   - 채널당 ARM은 동시에 하나만 둔다(`arm_outstanding`). DOORBELL을 받으면 해제된다.
3. control PE에 `doca_pe_request_notification()` → `doca_pe_progress()`를 호출한다. 요청 후 progress가 필요하다는 것은 DOCA 문서 순서이며, 9월 22일 실험에서 확인했다.
   - **구현에서 확인한 것:** control PE를 기본값 `DOCA_PE_EVENT_MODE_PROGRESS_SELECTIVE`로 두면 두 가지 문제가 생긴다.
     - 알림이 켜진 뒤 clear 전까지는 progress가 새 이벤트를 전달하지 않았다. DOORBELL로 깨어난 서버가 spare flow를 동기로 열 때 READY를 5초 동안 못 받았다.
     - clear를 progress 경로에 넣자, 처리 중인 send task가 있을 때 알림이 곧바로 다시 켜져 EQ가 초당 수십만 번 헛돌았다.
   - control PE는 context가 하나뿐이라 `PROGRESS_ALL`로 바꿨다(`comch_client.c`). 이 모드에서는 progress가 항상 전부 처리하고, 새 요청이 이전 알림을 지운다. clear는 EQ가 fd를 확인할 때만 한다.
4. core로 돌아가 한 번 더 drain한다. 비어 있으면 잠든다.

그 밖의 변경:
- control PE의 notification fd를 channel이 노출한다(`channel_dev_fd`). core는 이것을 spare 묶음처럼 모든 EQ의 epoll에 넣는다.
  - 그러면 DOORBELL, CLOSED, ERROR, READY 모두 잠든 EQ를 깨운다. 지금은 이것도 tick이 떠맡는다.
- 깨어나면 `doca_pe_clear_notification()`을 부르고, drain pass가 `channel_dev_progress()`로 메시지를 처리한다.
- `session_message()`에 DOORBELL case를 추가한다. `arm_outstanding = 0`만 하면 된다. 실제 일은 뒤따르는 drain이 한다.

### DPU (comch_server.c, dma.c)

- **ARM을 받으면:** payload의 각 flow에 대해 그 flow의 `push_seq`(host 메모리에 도착을 마친 마지막 descriptor seq)가 `expected_seq` 이상인지 본다.
  - 하나라도 그렇다면, host가 스냅샷 뒤에 도착한 것을 못 봤을 수 있으므로 **즉시 DOORBELL**을 보낸다.
  - 아니면 `session->armed_epoch = epoch`로 둔다.
- **`dmesh_dma_push_desc_done()`에서:** 그 flow의 session이 armed이면 `doorbell_due`를 세운다. FIN descriptor도 포함한다.
  - 실제 전송은 DMA callback이 아니라 driver가 매 루프 부르는 `dmesh_doca_ctrl_advance()`에서 한다.
  - 보내면서 disarm한다.
  - 보낼 epoch(pending)와 보낸 epoch(sent)를 따로 둔다. 송신이 실패하거나 `DOCA_ERROR_AGAIN`이면 다음 `advance()`에서 다시 보낸다(DPUmesh와 같음).
- proxy `shim.c`와 Rust driver는 바꿀 필요가 없을 것으로 본다. 이미 매 루프 `advance()`를 부르기 때문이다. 구현 때 확인한다.

### 깨움을 잃지 않는 이유

push descriptor 하나가 host 메모리에 도착을 마친 시점을 T, DPU가 ARM을 처리한 시점을 A라고 하자.

- **T < A:** 그 seq는 스냅샷의 `expected` 이상이거나(host가 아직 못 봄 → A에서 즉시 DOORBELL), 미만이다(host가 이미 봄).
- **T > A:** armed 상태이므로 `desc_done`에서 DOORBELL을 보낸다.
- **host가 ARM을 보낸 뒤 다시 drain에서 일을 찾은 경우:** DPU가 나중에 보내는 DOORBELL은 불필요한 깨움이 된다. 이런 깨움은 ARM 하나당 최대 1번으로 제한된다.
- **control 메시지:** 그 자체가 control PE fd를 깨운다.
- **DOORBELL과 descriptor 쓰기의 순서:** DOORBELL(Comch)과 descriptor 쓰기(DMA)가 host에 보이는 순서는 서로 보장되지 않을 수 있다.
  - 그래서 DOORBELL을 받으면 EQ를 ACTIVE로 되돌린다. 첫 drain이 descriptor를 못 봐도 뒤따르는 10 µs nap이 잡는다.
- **backstop(200 ms):** 위 논증을 벗어난 버그를 막는 안전망이다. 이것으로 일을 찾으면 센다. 회귀 시험의 통과 조건은 0회다.

## 6. 알림이 없는 세 가지의 처리

| 항목 | 바쁠 때 | 한가할 때 |
|---|---|---|
| (a) push 수신 | NAP / LINGER 폴링 | ARM → DOORBELL |
| (b) custody ACK | NAP / LINGER 폴링 | custody가 남아 있으면 ARMED로 가지 않음 |
| (c) control session | drain pass마다 progress | control PE notification fd |
| TX tail | `tail_fd` (변경 없음) | `tail_fd` |
| host-dpa 수신 | NAP / LINGER 폴링 | reverse PE doorbell (지금과 같음) |

## 7. 코드 변경

| 파일 | 변경 |
|---|---|
| `src/core/dmesh_core.c` | `dpumesh_eq_arm()`의 정책을 4절로 교체. EQ에 `nap_ns`, `last_activity_ns`, `state` 추가. `tick_fd`는 one-shot. spin 제거. control fd를 EQ epoll에 넣음. oversubscription 보호 |
| `src/core/native_transport.h`, `src/core/carrier.c` | `stripe_arm()`이 "tick 필요" 대신 "custody 남음"을 돌려줌. `dmesh_native_idle_arm()`, `dmesh_native_control_fd()`, `dmesh_native_control_clear()` 추가 |
| `src/transport/host/channel.{h,c}` | `channel_dev_fd()`, `channel_dev_arm()`, `channel_dev_clear()`. ARM 스냅샷과 전송. DOORBELL 처리 |
| `src/transport/common/session_protocol.h` | version 2, ARM/DOORBELL 타입과 payload encode/decode, `_Static_assert` |
| `src/transport/common/object.h`, `src/transport/dpu/comch_server.c` | session의 `armed_epoch`, `doorbell_due`. ARM 처리, `advance()`에서 전송 |
| `src/transport/dpu/dma.c` | `dmesh_dma_push_desc_done()`에서 `doorbell_due` 설정 |
| `tests/support/native_memory_transport.c` | 새 native 함수의 memory 구현 |
| `design/HOST.md` | Readiness 절을 이 정책으로 교체 |
| 어댑터(Go, C++, preload) | 없음. main 방식의 Go를 쓸 경우에만 dpu-dma의 Go timer 분기를 지우고, host-dpa와 같이 netpoller로 EQ fd를 기다리게 함 |

## 8. 시험과 측정

**단위 시험 (장비 없음)**
- core 정책(memory transport)
  - nap 순서 10/20/40/80 µs
  - LINGER 진입과 유지 시간
  - custody가 남아 있으면 ARMED로 가지 않음
  - ARMED 뒤 다시 drain
  - oversubscription 보호
  - `DPUMESH_SPIN_US` 경고
- `session_protocol` ARM/DOORBELL encode와 decode, 길이 검사
- `channel_session_test` 확장
  - ARM 스냅샷
  - 동시에 ARM 하나만
  - DOORBELL 뒤 `arm_outstanding` 해제
  - notification 요청 뒤 progress 순서
- `session_server_test` 확장
  - ARM 처리 시 `push_seq >= expected`이면 즉시 DOORBELL
  - armed 상태의 `desc_done` → 다음 `advance()`에서 DOORBELL 1회 후 disarm
  - generation이 다른 flow는 무시

**실장비 (dpu-dma, 수정 전후 교대)**
- idle: 서비스 10개를 띄우고 30초 동안 부하 없음.
  - process별 CPU와 context switch를 잰다.
  - 목표: 초당 깨어남이 backstop 수준(초당 약 5회) 이하.
- 요청 1개씩: health-bench m1로 9개 서비스를 잰다.
  - linger 0 / 1 / 10 ms를 각각 잰다.
  - 목표: 선택한 linger에서 p50이 수정 전보다 나쁘지 않음.
- Online Boutique: Locust 8 / 32 / 128명.
  - 목표: 사용자 8명에서 host CPU 약 0.9 core 감소, 처리량은 그대로 이상.
- `hw-regression.sh`: busy poll 1과 0 모두에서, backstop으로 일을 찾은 횟수 0.
- 계측: process별 nap / ARM / DOORBELL / 불필요한 DOORBELL / backstop 적중 카운터. `DPUMESH_CORE_TRACE` 또는 종료 시 통계로 남긴다.

## 9. DPUmesh와의 관계

DPUmesh(`~/DPUmesh`)의 reverse 경로 깨우기를 끝까지 따라가 봤다. 대기 정책은 거의 그대로 가져오고, doorbell을 켜는 방식은 다르게 간다.

### 가져오는 것

- **nap 정책:** 10 µs에서 시작해 두 배씩, 상한 100 µs를 넘으면 arm한다(`src/core/dmesh_core.c:790-863`).
  - 근거: 10→100 µs backoff가 고정 25 µs보다 나았다. conc32 p50 261 vs 316 µs, conc1 p50 136 µs. 응답이 초기 10–40 µs nap 안에 도착해서다(`bench/report/data/doorbell-relay-20260901/SUMMARY.md`).
- **순서:** arm한 뒤 한 번 더 drain하고, 그래도 비었을 때만 잠든다.
- **backstop 200 ms:** DPUmesh에서도 측정 근거 없이 이전 구현에서 이어받은 값이다. 우리는 적중 횟수를 세서 0이어야 통과로 둔다.
- **oversubscription 보호:** `n_live_eqs`(최대치가 아니라 현재 살아 있는 수)가 `sched_getaffinity`의 CPU 수보다 많으면 nap을 건너뛴다. CPU 수는 100 ms마다 다시 읽는다. 스레드가 많은 조건에서 nap 지연이 RTT에 그대로 더해져서 생긴 규칙이다.
- **assist 규칙:** 다른 EQ thread가 방금 일을 처리했으면, 자기 nap을 최소로 되돌리지 않고 상한 간격으로만 폴링한다. 바쁜 in-line 소비자를 매번 선점하지 않기 위해서다.
- **DPU 쪽 송신:** 보낼 epoch(pending)와 보낸 epoch(sent)를 따로 둔다. 송신이 실패하거나 `DOCA_ERROR_AGAIN`이면 다음 루프에서 다시 보낸다.
- **참고 수치:**
  - doorbell 한 번의 kernel wake chain 비용은 약 7 µs다.
  - nap 0(순수 doorbell)이면 broker가 초당 1.24만 번 깨어나 0.177 core를 썼다.
  - 부하 중 doorbell이 사라지자 DPU ARM CPU가 약 26% 줄었다.

### 다르게 가는 것: doorbell을 켜는 방식

| | DPUmesh | 이 설계 |
|---|---|---|
| host가 켜는 방법 | host 메모리의 reverse ring ctrl에 `arm_epoch`를 씀 (store 한 번) | Comch `ARM` 메시지에 flow별 `expected` 스냅샷을 담아 보냄 |
| DPU가 아는 방법 | **publish할 때마다** ctrl 64 B를 DMA로 읽음. 이 읽기는 flow control용 `consumer_head` 읽기를 겸함 | ARM을 받을 때만 처리. push 경로에 읽기가 늘지 않음 |
| 깨움을 잃지 않는 근거 | Dekker 패턴(host: epoch 쓰기 → 다시 확인 / DPU: 쓰기 → ctrl 읽기). host 쪽에 StoreLoad fence가 필요 | DPU가 `push_seq`와 스냅샷을 비교 (5절). host 메모리 순서에 기대지 않음 |
| 부하 중 DPU 비용 | publish마다 DMA 읽기 1회 | 없음 (host가 arm하지 않음) |
| 한가할 때 비용 | 없음 (DPU는 publish할 때만 읽음) | idle로 넘어갈 때마다 ARM 1회, DOORBELL 1회 |

**선택 이유:**
- 우리 push 경로는 publish 때마다 ctrl을 읽지 않는다. 읽기를 넣으면 DPU ARM에 일이 늘고, 그 ARM이 지금 처리량 병목이다(1 core).
- 우리 cursor pull(`dmesh_dma_pull_cursor`)은 16 batch마다이거나 흐름이 막혔을 때만 읽는다.
- 그래서 비용을 idle 전환 때만 내는 ARM 메시지 쪽을 택한다.
- DPUmesh 방식(host `dmesh_push_cursor`의 `pad`를 `arm_epoch`로 쓰고, push 완료마다 cursor를 읽음)은 대안으로 남긴다. ARM/DOORBELL 메시지 수가 문제가 되면 다시 본다.

### 다른 점

- **ACK:** DPUmesh는 TX custody ACK도 reverse ring의 entry(`DMESH_REV_ENTRY_TX_ACK`)로 받아서 같은 doorbell을 탄다. 우리 ACK는 DPA가 쓰는 `consumer_head`라 doorbell이 없다. 그래서 6절처럼 custody가 남아 있으면 잠들지 않는다.
- **깨우는 경로의 thread 수:** DPUmesh는 broker를 거친다. DPU worker → DPU main → Comch → broker thread → eventfd → drain shard → EQ eventfd → 앱 thread로, host thread 3개를 지난다.
  - 이 설계에서는 control PE fd가 앱의 EQ epoll에 바로 들어 있어 host thread 하나로 끝난다.
  - broker PR에서는 여기에 hop이 하나 늘어난다.

## 10. broker PR에서 바뀌는 곳

- control PE와 DOORBELL 수신이 broker로 옮겨간다. 앱에는 broker가 넘기는 eventfd 하나가 `channel_dev_fd()` 자리에 들어간다.
- ARM 전송도 broker가 맡는다. 앱은 공유 메모리에 스냅샷을 쓰고 broker를 깨운다.
- core의 정책(4절)과 어댑터는 바뀌지 않는다. channel 계층 뒤에 숨겨 둔 것이 이것 때문이다.

## 11. 범위 밖 / 후속

- **DPU L7 처리 비용 (RPC당 약 0.8 ms):** RPS의 가장 큰 병목. 별도 profiling이 필요하다(DPU `perf`에 root 필요).
- **DPU push 파이프라인:** 지금은 flow당 8 KiB batch 하나를 data DMA → descriptor DMA로 직렬 처리한다. DPUmesh는 여러 batch를 동시에 처리하고 descriptor 여러 개를 한 번에 publish한다.
  - 이 설계의 doorbell은 "armed 중 아무 descriptor 완료"에 반응하므로, 파이프라인이 바뀌어도 그대로 유효하다.
- **ACK doorbell:** DPU ARM은 DPA의 DMA_COMPLETED로 forward 소비를 안다. 하지만 DPA가 host의 `consumer_head`를 쓰는 시점과 순서가 보장되지 않는다. 먼저 6절의 "custody가 남으면 잠들지 않음"으로 가고, 측정에서 문제가 보이면 다시 본다.
