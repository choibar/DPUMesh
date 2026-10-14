# preload gather write — 2026-09-29

판정: **`writev`/`sendmsg`가 iov 원소별로 전송하던 것을 native reservation 하나로 모으자, 쪼개진 쓰기의 약 510 µs 지연이 사라졌다.** 단일 버퍼 쓰기의 동작은 변경 전과 같고, 회귀는 없었다.

- 변경 전 `shim_send_iov`는 iov 원소마다 따로 `dmesh_alloc`/`dmesh_post_send`를 호출했다.
- 그래서 idle 상태에서 첫 원소(예: HTTP/2 프레임 헤더 9B)는 바로 나갔다. 다음 원소는 앞선 단위가 아직 DPU 처리 중이라 busy tail이 되어, library의 500 µs 마감 시각까지 보류됐다(`src/core/dmesh_core.c` `dmesh_tx_after_commit`, `tx_arm_idle_tail`).
- 변경 후에는 iovec cursor를 따라 원소 경계를 넘어 한 reservation(최대 block 크기)을 채운다. 쓰기 호출 하나가 전송 쓰기 하나가 된다.

## 결과

같은 서버(preload `tcp_echo`), 같은 proxy, 연결 1개, 109B 왕복 2000회씩 교대로 쟀다.

| 측정 | 변경 전 | 변경 후 |
|---|---|---|
| `write(109)` p50 / p99 | 57, 61 µs / 130, 146 µs | 55–65 µs / 220–286 µs |
| `writev(9,100)` p50 / p99 | **569, 571 µs** / 829, 855 µs | **55–62 µs** / 206–294 µs |

`write(109)`의 p99 차이는 측정 묶음 사이의 편차이며, 이 경로의 코드는 바뀌지 않았다. 변경 후 묶음 안에서 `write`와 `writev`의 차이는 없었다.

## 회귀 확인: 변경 전과 후 preload 교대 실행

`examples/preload/tcp_client`로 `RUN 200 64 4`와 `RUN 50 65537 2`를 돌렸다. 서버와 client는 같은 preload 버전을 썼고, 실행마다 proxy와 서버를 새로 띄웠다. 결과 형식은 `성공/실패, p50, p99`이다.

| 실행 | 64B × 연결 4개 | 65537B × 연결 2개 | DOCA 에러 (서버 종료 전 / 후) |
|---|---|---|---|
| 변경 전 1 | 200/0, 76 µs, 675 ms | 50/0, 665 µs, 561 ms | 1 / 1 |
| 변경 후 1 | 150/50, 72 µs, 740 ms | 50/0, 629 µs, 560 ms | 0 / 2 |
| 변경 전 2 | 150/50, 69 µs, 742 ms | 50/0, 589 µs, 562 ms | 0 / 2 |
| 변경 후 2 | 150/50, 96 µs, 736 ms | 50/0, 609 µs, 561 ms | 0 / 2 |

- **연결 4개 중 1개 실패는 변경 전에도 있었다(기존 문제).** `DPUMESH_BACKEND_POOL=2`에서 새 연결 4개가 동시에 오면 비는 backend flow를 동기로 연다. flow 하나에 약 167 ms가 걸리고, 그 사이 한 연결이 client의 응답 대기 시간을 넘긴다. p99 0.56–0.74 s도 연결별 첫 메시지가 이 대기를 포함해서 생긴다.
- **서버 종료 후 DOCA 에러(`cqe with error syndrome 0x15`, `A message failed to send`)도 변경 전에도 있었다.** preload 서버를 SIGTERM으로 끝낸 뒤 proxy teardown에서 나오며, 변경 전과 후가 같다.
- 첫 측정 묶음에서 DMA task 에러(`LOCAL_QP_OPERATION_ERROR`)를 **1회** 관측했다. 이후 교대 실행 4회에서는 두 버전 모두 재현되지 않아 원인을 판단하지 않았다.

## 기능 검증

| 검증 | 결과 |
|---|---|
| `make test` | PASS. 새 테스트 2개 포함: iov `[3,0,5,2]`를 4B post 한도에서 4+4+2로 모아 쓰기, EAGAIN이 iovec cursor를 소비하지 않고 TX_READY 뒤 재시도가 전체를 한 번 순서대로 보내기 |
| 장비 `iovbench` (`write`/`writev` 교대) | 2회, 위 표 |
| 장비 `tcp_client` 변경 전과 후 교대 | 위 표 |

## 측정 조건

rapids4 host, BF-3 DPU(DOCA 3.1), DPU 쪽 `~/DPUMesh-online-boutique`(main `b607129` + proxy `c09915bf`), PCI `03:00.0`/`94:00.0`, `dpu-dma`. Proxy 설정은 `LINKERD2_PROXY_CORES=1`, `DMESH_BUSY_POLL=1`, `MOCK_OUTBOUND_OPAQUE=1`, `MOCK_POLICY_ECHO_TARGET=1`이다. 서버는 `DPUMESH_SERVICE=localhost:18086 DPUMESH_PORT=18086 DPUMESH_BACKEND_POOL=2 DPUMESH_BACKEND_MAX=4`로 띄웠다. 변경 전 preload는 `git show b607129:src/facade/dmesh_preload.c`를 Makefile의 preload 규칙대로 빌드했다. `iovbench` 소스와 로그는 host의 `build/e2e/go-adapter-rework-2026-09-29/`에 로컬로만 보존한다.
