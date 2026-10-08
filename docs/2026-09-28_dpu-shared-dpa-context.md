# DPU worker들의 DPA context 공유

DPU worker 초기화는 이제 OS process 안에서 같은 PF PCI 주소와 DPA app에 대해 하나의 base DPA context를 공유한다. `host-dpa`와 `dpu-dma` 모두 DPU 측에 적용한다. 다른 PF/app은 별도 context를 만든다. Host channel의 base/extended context 생성 방식은 바꾸지 않았다.

## 소유권과 동기화

- `common/dpa_runtime.c`가 context와 자체 `doca_dev_open()` reference를 소유한다. Worker의 Comch device reference와 수명을 분리한다.
- Worker마다 runtime lease 하나를 갖고, 기존 32-slot thread pool, PE, flow ring/MsgQ/completion/buffer를 유지한다. Thread/EU 요구량과 wire/public API는 그대로다.
- Registry 조회·초기화·마지막 해제와 DPA SDK 호출은 process-wide mutex로 직렬화한다. PF가 다른 runtime도 첫 구현에서는 같은 SDK mutex를 사용한다. 초기화 중복 방지를 위해 초기화 SDK 호출 중에도 mutex를 유지한다.
- `DMESH_DPA_CALL`은 개별 SDK 호출만 감싼다. PE progress, task callback, quiesce 반복 전체에서는 lock을 유지하지 않는다. Proxy의 RX watermark 쓰기도 같은 lock을 사용한다. 공유에 따른 lock 비용은 아직 벤치마킹하지 않았다.
- Shared context에 새로운 worker가 붙을 때 fatal 상태를 검사한다. Context failure는 해당 context의 모든 thread에 영향을 줄 수 있다. 자동 context 재생성/flow 복구는 구현하지 않았다.

## 종료·실패

`cleanup_objects()`는 checked DPA cleanup을 호출한다. 살아 있는 flow나 pool owner가 있으면 해제를 거절한다. Flow가 모두 정리된 뒤 유휴 thread와 부분 생성된 thread/argument까지 정리하고 runtime lease를 반환한다. 마지막 lease만 context stop/destroy와 runtime device close를 수행한다.

SDK 해제 실패 시 pointer와 lease를 보존한다. 마지막 해제가 시작된 context에는 신규 worker를 붙이지 않고 AGAIN을 반환한다. 재시도는 남은 단계부터 진행한다. 초기화 실패 중 SDK cleanup까지 실패하면 registry에 해당 자원을 보존하고, 다음 acquire가 먼저 cleanup을 재시도한다. Thread pool은 생성 성공 수와 thread start 성공 여부를 기록한다. 부분 생성에 실패한 slot은 cleanup 후 재생성하며, 준비되지 않은 thread를 flow에 넘기지 않는다.

Proxy shim은 cleanup 실패 시 worker를 free하지 않는다. 기존의 live-flow/session 보존 조건도 유지한다. Logger 초기화와 staging flow-control 설정은 `pthread_once`로 처리한다. 프로세스 간 공유나 별도 관리 thread는 추가하지 않았다.

## 프로그램 전역 상태

HPACK 실험 mode 3/4/5의 mutable static scratch는 공유 context에서 worker끼리 충돌하므로 thread별 DPA heap scratch로 옮겼다. 해당 mode에서만 64 KiB를 할당하고 checked thread teardown에서 회수한다. Kernel의 static assertion으로 scratch 크기를 검증한다. 정상 DMA 경로에는 이 추가 할당이 없다. Internal thread argument가 변경되므로 DPA kernel과 CPU library를 함께 rebuild해야 한다.

## 검증

- `make -j4 test`: native/mock 15개, C/C++ header 및 ABI 검사 통과.
- `dpa_runtime_test`: 16개 pthread의 동시 acquire가 하나의 context 생성으로 모임; PCI가 같고 handle이 다른 경우 공유; 다른 PF 분리; SDK 호출 상호 배제; 첫 worker 종료 후 peer 유지; fatal join 거절; stop/destroy/device-close 실패 재시도; 초기화 실패 자원 보존과 재시도.
- `dpa_cleanup_test`: partially initialized pool 정리, 살아 있는 owner 보호, thread/argument 해제 실패 보존, PE progress 동안 SDK mutex 미보유.
- Transport와 DPA kernel, DMA bench 앱, legacy hostlib 빌드 통과.
- Proxy `dmesh-doca` 단위 테스트 13개와 full release proxy(ThinLTO) 빌드 통과.
- 실제 DPU PF `0000:03:00.1`: 두 worker가 context 하나를 공유하고 각각 32개 thread를 준비. 첫 worker 정리 후 두 번째 worker의 DPA alloc/h2d/d2h/free 성공. 마지막 worker의 thread/context/device 정리 성공. Thread를 run하지 않았으며 NIC/SF/EU partition 변경 없이 수행했다.

로그: `/tmp/dpa-global-{tests,transport-build,app-build,legacy-hostlib-build,proxy-tests,proxy-build,smoke}.log`.

후속 [non-sharded gRPC-go 검증](../bench-results/2026-09-28_grpc-go-unsharded-validation.md)에서 dpu-dma, 4개 Tokio thread, 1/2 Drivers의 실제 64B echo를 실행했다. Busy-poll의 짧은 반복과 정상 teardown은 통과했지만 event 구성의 유휴 후 재연결에서 DPA device fatal이 두 번 발생해 전체 lifecycle 검증은 실패했다. 같은 context를 공유한 두 Driver 모두 영향을 받았다. 정확한 원인은 미확정이며 상세 실패/재실행 로그는 보고서에 기록했다.

이후 [busy-poll 성능 평가](../bench-results/2026-09-28_grpc-go-64b-unsharded-busy-poll.md)는 Driver 1개, DPU ARM cores 1/2/4, host client connections 1/2/4에서 27회 모두 traffic/정상 teardown을 통과했다. 측정 구간의 echo 5,413,470개를 검증했다. 이는 단일 Driver의 runtime core 확장 평가이며 여러 Driver의 context 공유 효과나 장시간 idle 문제 해결을 검증하지 않는다.

Context 공유 변경 전후의 성능 비교, host-dpa 실제 gRPC, 16-worker hardware 실행 및 HPACK 실행은 이번 검증에 포함하지 않는다. Host-dpa의 host context 수와 per-flow EU 제약은 여전히 남는다.

Hardware lifecycle test 재현(명시적으로 실행할 때만 장비 사용):

```sh
ninja -C src/transport/build
cc -O2 -g -DDOCA_ALLOW_EXPERIMENTAL_API \
  -I/opt/mellanox/doca/include -Isrc/transport/common \
  -Isrc/transport/dpu -Isrc/transport/host tests/dpa_context_smoke.c \
  -Wl,--start-group src/transport/build/libdmesh_common.a \
  src/transport/build/libdmesh_dpu.a src/transport/build/libdmesh_host.a \
  src/transport/build/device/dpa_kernel.a -Wl,--end-group \
  $(pkg-config --libs doca-common doca-comch doca-dma doca-dpa libflexio) \
  -pthread -o /tmp/dpa-global-smoke
sudo /tmp/dpa-global-smoke 0000:03:00.1
```
