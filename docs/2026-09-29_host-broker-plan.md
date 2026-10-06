# Host broker: DOCA device 소유권을 broker로 옮기기 — 2026-09-29

상태: **1단계 구현 완료.** 처음 구현은 브랜치 `feature/doca-broker`(main `b607129` 기준)였고, 지금은
`feature/grpc-all`(main + PR #7–#11) 위로 옮긴 `feature/doca-broker-grpc`에 있다. 옮기면서 direct 경로를 남겼고,
PR #7의 idle wake를 broker를 거쳐 전달하게 했다(6절).
측정: [bench-results/2026-09-29_host-broker-ab.md](../bench-results/2026-09-29_host-broker-ab.md). 처음에는 4-thread 8 KiB echo가
−4~7%였다. perf로 추적한 원인은 DPU 쪽 동기 rd_pos 갱신이었고(5절), `shim.c`에서 이를 묶어 모든 케이스를 before와 같게 맞췄다.
같은 수정은 grpc-all의 submodule에 이미 들어 있다(`DMESH_RX_WM_BATCH`). grpc-all 위의 측정은
[bench-results/2026-10-06_host-broker-grpc-ab.md](../bench-results/2026-10-06_host-broker-grpc-ab.md).

## 1. 목표와 범위

`~/DPUmesh`의 per-Pod broker처럼 **host broker 프로세스가 DOCA device, Comch session, 등록 메모리(`doca_mmap`)를 소유**한다. 앱(`libdpumesh`)은 broker가 넘겨준 memfd를 mmap해 데이터 경로를 공유 메모리로만 사용한다. 앱은 DOCA fd를 갖지 않는다.

Pod 인증 방식은 아직 정하지 않았다. 그래서 인증·격리 계층은 가져오지 않고, **소유권 구조만** 최소한으로 옮긴다.

**성능 조건:** 데이터 경로 성능은 before/after가 같아야 한다. 같은 DPU 바이너리로 main 빌드와 번갈아 측정해 확인한다.

## 2. DPUmesh에서 가져오는 것 / 가져오지 않는 것

| DPUmesh | 이 repo |
|---|---|
| `src/broker/dmesh_brokerlink.c`: SCM_RIGHTS 송수신, HELLO magic/version, ERROR 응답, memfd seal 검사, 소켓 소유자 검사 | `src/transport/host/broker_ipc.{h,c}` |
| `src/broker/dmesh_ipc.h`: 고정 크기 wire struct와 `_Static_assert` | 같은 패턴. 메시지는 이 repo의 channel 연산에 맞게 새로 정의 |
| `doca/buffer.c`: memfd 할당(`memfd_create` → `ftruncate` → `MAP_SHARED` → shrink/grow/seal) | `channel.c`의 등록 메모리와 forward ring(broker 소유 모드) |
| `dmesh_broker_run`: client 소켓과 PE를 같이 기다리는 루프, HUP이면 정리 | `channel_broker.c`의 server 루프 |
| `init_broker_datapath`: fd 수신 → mmap | `channel_broker.c`의 client 경로 |

가져오지 않는 것:
- dpumeshd, device plugin, controller
- registration challenge/assertion relay
- broker supervisor(PID/mount/cgroup/net namespace, launch barrier), pivot_root, uid drop, seccomp
- RESOLVE relay: 이 repo는 서비스 해석을 host에서 DNS/env로 한다.
- REV_DOORBELL eventfd: 처음 구현 때 이 repo의 push 경로에는 doorbell이 없었다. PR #7의 ARM/DOORBELL이 생긴 뒤에는 6절의 방식으로 전달한다.

## 3. 구조

```text
앱 프로세스 (libdpumesh)                          dpumesh_broker --listen <sock>  (관리자가 기동)
  carrier.c ── channel.h                           accept마다 fork → child 1개 = 앱 1개
    control: Unix socket 요청/응답 ───────────────→  child: channel.c를 소유 모드로 실행
    data:    memfd mmap (TX, RX, flow별 ring) ←fd──    (DOCA device, Comch session, doca_mmap 등록)
    flow 상태: 공유 status page 읽기          ←────     PE progress → CLOSED/error를 status page에 기록
```

- **경계는 `src/transport/host/channel.h`**이다. control 함수(dev/session/mem/conn open·close·progress)는 IPC로 가고, hot path(`channel_conn_post`/`consumed`/`rx_next`/`rx_consumed`)는 push 모드에서 원래 메모리 접근뿐이므로 공유 메모리 위에서 그대로 동작한다.
- **권한:** 권한 없는 앱이 더 큰 권한의 프로세스를 만들 수는 없다. 권한은 관리자가 미리 띄운 `dpumesh_broker` 부모에서 오고, fork한 child는 그 권한을 물려받는다. dpumeshd에서 "broker 띄우기"만 남긴 형태이며, 인증은 이후 accept 지점에 끼운다.
  - 참고로 이 host는 `/dev/infiniband/uverbs*`가 `0666`이고 memlock이 unlimited라서, bare-metal에서는 일반 사용자도 device를 연다. 실제로 막히는 곳은 컨테이너(pod) 안이다.
- **PCI 주소:** broker를 쓰면 broker 설정에서만 받고, 앱에는 `DPUMESH_PCI_ADDR`가 필요 없다. direct 경로(6절)에서는 앱이 `DPUMESH_PCI_ADDR`로 device를 연다.
- **flow identity:** server 이름, pod IP, workload label은 인증이 정해질 때까지 앱이 보내고 broker는 그대로 전달한다.
- **forward descriptor 주소:** DPU에는 등록한 프로세스(broker)의 VA가 전달된다. 앱이 올리는 `desc.addr`는 `channel_conn_post`에서 `앱 VA − 앱 base + broker base`로 바꾼다. DPU, DPA kernel, proxy는 바꾸지 않는다.
  - DPUmesh처럼 offset과 kernel bounds check로 바꾸는 작업은 비신뢰 앱을 다룰 인증 단계에서 함께 한다.
- **host-dpa reverse**는 hot path에 DOCA 호출이 있다(reverse PE completion, `doca_dpa_h2d_memcpy`로 쓰는 `rd_pos`). 그래서 1단계에서는 옮기지 않는다. `DPUMESH_REVERSE=host-dpa`는 2단계까지 임시로 direct 경로를 유지한다.

## 4. 단계

### 1단계: push(dpu-dma) reverse

1. `channel.c`의 등록 메모리와 forward ring에 memfd 할당을 추가한다(broker 소유 모드일 때만).
2. `broker_ipc`를 이식하고, 장비 없이 도는 테스트를 만든다(socketpair로 fd 전달, seal, 메시지 검증).
3. `channel_broker.c`(client와 server)와 `dpumesh_broker` 실행 파일(listen+fork)을 만든다.
4. carrier는 broker 모드에서 `DPUMESH_PCI_ADDR`를 요구하지 않는다.
5. 장비에서 측정한다: `dpumesh_host` ↔ `dpumesh_dpu`, main 빌드와 번갈아 실행한다.
6. 결과를 `bench-results/`에 기록한다.

### 2단계: host-dpa를 broker로

먼저 kernel 변경을 시도한다. host DPA thread가 completion을 Comch msgq 대신 공유 메모리 슬롯 ring(`{seq,pos,len}`)에 쓰고, `rd_pos`는 host 메모리 page에서 폴링한다. 핵심 확인 사항은 "데이터가 먼저 도착하고 completion은 그 뒤"라는 순서 보장이다. 안 되면 broker relay로 간다. 완료되면 direct 경로를 제거하고 `libdpumesh`에서 DOCA 의존을 뺀다.

### 후속

- accept 시점의 pod 인증
- descriptor offset과 bounds check
- broker 격리
- 여러 앱이 쓰는 단일 broker의 DPA runtime 공유: 칩 전체 DPA process가 동시 28개로 제한되므로 필요하다(`bench-results/2026-09-26_dpa-process-limit.md`, `docs/2026-09-26_dpa-process-sharing-plan.md` §7).

## 5. 알려진 trade-off

- **Poll cadence와 DPU rd_pos:** in-process 경로는 carrier가 poll할 때마다 Comch PE를 progress했다(1 thread 66 ns, 4 thread 경합 약 560 ns). broker client는 대신 status page를 읽으므로 host의 post 리듬이 더 매끄러워진다.
  - DPU driver 코어는 tick마다 새 데이터가 있던 flow마다 `doca_dpa_h2d_memcpy`(약 1.6 µs)로 rd_pos를 동기 갱신했다. 그래서 리듬이 매끄러울수록 메시지당 비용이 늘었고, 이 비용이 코어의 28–38%를 차지했다.
  - 수정: `shim.c`가 64 KiB를 소비할 때마다만 rd_pos를 보낸다. host의 `CHANNEL_RD_POS_BATCH`와 같은 방식이다. 수정 후에는 리듬과 무관하게 before와 같고, 4-thread echo는 14.3 → 20.4 Gbps가 됐다.

- control 요청은 channel당 소켓 하나로 직렬화된다. conn open이 DPU READY를 기다리는 동안 같은 channel의 다른 close는 잠깐 기다린다.
- conn open/close마다 IPC 왕복과 ring memfd mmap이 한 번씩 추가된다. 데이터 경로에는 추가 hop이 없다.
- broker가 죽으면 그 앱의 channel은 실패로 처리한다. 재접속과 epoch는 후속 작업이다.

## 6. grpc-all 위로 옮기기 — 2026-10-06

**direct 경로를 남긴다.** dpu-dma channel은 다음 경우에만 broker client가 된다(`channel_dev_open`).

- `DPUMESH_BROKER`가 소켓을 가리킬 때
- `DPUMESH_BROKER`가 없고 `/run/dpumesh/broker.sock`이 있을 때

`DPUMESH_BROKER=off`, broker 소켓이 없는 host, host-dpa 경로에서는 앱이 예전처럼 device를 직접 연다. 지정한 broker에 연결하지 못하면 direct로 넘어가지 않고 실패한다.

**idle wake를 broker를 거쳐 전달한다.** PR #7에서 EQ는 잠들기 전에 ARM을 보내고, DOORBELL이 control PE의 fd를 깨운다. broker client에는 control PE가 없다. 그래서 요청 소켓 옆에 다음 경로를 둔다.

- HELLO 응답이 arm page memfd와 eventfd 2개(wake, kick)를 함께 넘긴다(broker IPC v2).
- 앱의 `channel_dev_arm`은 push flow마다 다음에 읽을 descriptor를 arm page(seqlock)에 쓰고 kick을 올린다. broker는 in-process 경로와 같은 송신부(`channel_dev_send_arm`)로 DPU에 ARM을 보낸다.
- DOORBELL은 broker가 PE를 progress하는 어느 곳에서든 wake eventfd를 올린다. flow 상태가 바뀌어도 올린다. arm page를 끝내 일관되게 읽지 못하거나 ARM 송신이 실패하면 앱을 깨워 다시 poll하게 한다.
- ARM은 요청 소켓을 거치지 않으므로, 같은 channel에서 conn open이 DPU READY를 기다리는 동안에도 막히지 않는다.

**비용.**

- 잠든 앱을 깨우는 첫 메시지마다 broker를 한 번 더 거친다(DOORBELL → broker → eventfd → 앱).
- nap과 linger 구간(마지막 일 뒤 1 ms)에는 ARM을 보내지 않으므로, 연속된 트래픽에는 이 비용이 없다.

**뺀 것.** `629c12c`(DPU의 연결별 watermark)는 grpc-all에 같은 수정이 있어 가져오지 않았다.
