# 4-core proxy CPU 미포화 원인 조사

2026-09-29 UTC. **Host의 CPU core 수 부족은 현재 처리량 한계를 설명하지 않는다. DPU worker는 CPU 배정을 기다리는 것이 아니라 non-runnable 상태로 상당 시간을 보낸다.** HTTP/2 공유 상태의 mutex 경합과 Tokio task 대기가 관찰됐지만, 약 33k RPC/s 한계를 하나의 lock/Driver/host transport로 완전히 분리하지는 못했다.

## 고정 조건과 대조군

DPU 4 ARM cores(CPU 12–15), busy-poll, sharded OFF, Driver 1개, dpu-dma. Host client/server 각 1 process, client connections 4개, backend pool 4개, connection당 256 concurrent RPC(전체 1,024), 64B echo. Warmup 3초, 측정 10초, 비프로파일링 조건마다 3회 반복했다. 동일 proxy/host library/Go binary를 사용했고 artifact SHA는 JSON에 기록했다.

Host core 수와 GOMAXPROCS를 함께 2/4/8/16으로 변경했다. 2/4/8은 client CPU 0부터, server CPU 8부터 각각 해당 개수로 배정했다. 16은 양쪽 process가 CPU 0–15 전체를 공유한다. **각각 독립된 16 core를 배정한 실험은 아니다.**

| Host cores / process | RPC/s 중앙값 | Proxy CPU 합계 | Host client CPU | Host server CPU |
| ---: | ---: | ---: | ---: | ---: |
| 2 | 32,470.4 | 334.3% | 125.8% | 127.3% |
| 4 | 33,605.2 | 335.1% | 168.0% | 183.3% |
| 8 | 33,342.0 | 334.7% | 200.6% | 247.9% |
| 16 (공유 CPU 집합) | 33,415.9 | 334.3% | 223.1% | 299.9% |
| 8 (마지막 재확인) | 33,422.3 | 335.7% | 202.4% | 246.2% |

CPU 100%는 core 하나다. Host core/GOMAXPROCS를 늘리면 host CPU 비용이 오히려 늘지만 처리량은 거의 같다. 2 cores/process에서도 기준 처리량의 약 97%를 유지한다. Host의 물리 CPU 용량 부족이라는 가설은 지지되지 않는다.

Thread별 CPU도 조사했다. Baseline 첫 run의 가장 바쁜 host OS thread는 client 약 26%, server 약 30%였다. Go goroutine은 OS thread 사이를 이동하므로 이 사실만으로 단일 goroutine의 직렬화를 배제할 수는 없다. Host CPU profile에는 native ring polling, gRPC control buffer, Go scheduler/lock 비용이 분산되어 있었다. **Host의 channel-wide mutex, single poller, flow-control 대기를 완전히 배제했다는 뜻은 아니다.**

## DPU가 실행하지 않은 시간

`/proc/PID/task/TID/schedstat`에서 runtime과 runqueue wait를 측정했다. Runqueue wait는 runnable하지만 CPU를 배정받지 못한 시간이고, 아래 non-runnable은 측정 wall time − runtime − runqueue wait의 근사치다.

Baseline 8-core host 조건의 3회 평균으로 DPU worker 하나당 10초 중:

- 실행 시간: 약 **8.37초**.
- Runnable 상태로 CPU 배정을 기다린 시간: 약 **4.3ms**.
- 나머지 non-runnable 시간: 약 **1.63초**.

마지막 재확인에서도 non-runnable 약 1.60초, runqueue wait 약 3.1ms로 재현됐다. 검사한 DPU cgroup 및 상위 cgroup의 `cpu.max`는 `max 100000`이고 CPU throttling 기록은 없다. 따라서 core affinity 부족, 다른 runnable thread에 의한 CPU 기아, CFS CPU quota로 약 16%가 빠지는 상황은 아니다.

가벼운 profile run에서 50ms 간격으로 `/proc/.../wchan`과 kernel stack을 읽었다. Worker의 `futex_wait_queue`와 일부 `ep_poll` 대기를 관찰했다. Wchan은 순간 샘플이며 상태 전환 중 `S:0` 같은 값도 나오므로 대기 원인별 wall-time 비율로 환산하지 않았다. Admin/SDK helper thread는 별도로 대부분 대기 상태였다.

현재 busy-poll은 **Driver future 하나의 PE progress loop**에 적용된다. 이 future는 매번 `yield_now()`로 Tokio에 실행권을 반환한다. 네 OS worker 모두가 별도 busy-poll loop를 갖는 구조가 아니다. Protocol task가 lock, I/O, 다른 task의 진행을 기다리면 추가 core는 쉬게 된다.

## HTTP/2 동기화 증거

현재 binary를 99Hz `cpu-clock`으로 sampling했다. DPU call graph는 DWARF, host는 frame pointer를 사용했다. 가벼운 profile에서 lost sample은 0이며 처리량은 32,187.2 RPC/s로 비프로파일링 baseline보다 약 3.5% 낮았다. 이 run은 종료 panic이 있어 lifecycle PASS로 판정하지 않는다.

DPU on-CPU profile:

- `std::sys::sync::mutex::futex::Mutex::lock_contended`: self **4.08%**, inclusive **11.44%**.
- `__arm64_sys_futex`: inclusive **7.11%**.
- Contended mutex의 caller에 Hyper HTTP/2 `H2Stream::poll`, `PipeToSendStream::poll`, client `ClientTask::poll`이 나타난다.
- H2 frame/header 처리, stream 상태 접근, task scheduling/wakeup에도 CPU가 분산된다.

**이 비율은 on-CPU sample 비율이다. Lock을 기다리며 잔 wall time 11.44%라는 뜻이 아니다.** Inclusive 항목끼리는 중첩되므로 합산하지 않는다. 최적화된 binary의 call graph에 일부 unresolved frame도 존재한다.

코드에서도 h2의 connection/stream 상태는 `Arc<Mutex<Inner>>`를 공유하고, frame send buffer에는 별도 Mutex가 있다. `reserve_capacity`, `poll_reset`, `is_end_stream`, `poll_data` 등에서도 해당 lock을 잡는다. Non-sharded runtime에서는 같은 HTTP/2 connection의 driver와 stream task가 여러 worker에서 실행되므로 실제 lock 경합이 생길 수 있다.

관련 코드:

- [Driver busy-poll 및 yield](../linkerd2-proxy/linkerd/doca/src/driver.rs)
- [h2 connection/stream 공유 상태와 lock](../linkerd2-proxy/vendor/h2/src/proto/streams/streams.rs)
- [Host channel-wide mutex와 poller](../integrations/grpc/go/dmesh.go)

이 증거는 HTTP/2 동기화가 비용과 대기의 일부임을 보여준다. **CPU가 비는 시간 전부 또는 최종 처리량 한계를 이 mutex 하나에 귀속할 근거는 아직 없다.**

## 옵션 대조 실험

Host 8 cores/process를 유지하고 기존 binary의 진단 옵션만 변경했다. 성능 측정 중 profiler는 실행하지 않았다.

| 조건 | RPC/s 중앙값 | Proxy CPU | 판단 |
| --- | ---: | ---: | --- |
| 기본 Hyper | 33,342.0 | 334.7% | 기준 |
| `DMESH_INLINE_STREAMS=1` | 32,477.5 | 321.4% | CPU 비용은 줄지만 처리량 개선 없음 |
| `DMESH_NGHTTP2=1` | 33,628.3 | 334.0% | 큰 처리량 개선 없음 |
| 기본 Hyper 재확인 | 33,422.3 | 335.7% | 기준 재현 |

두 옵션의 실제 환경 전달을 `/proc/PID/environ`에서 확인했다. 옵션을 켰다는 사실만으로 해당 경로의 모든 비용이 제거된 것으로 보지 않는다. 이 결과로 inbound stream task 생성 또는 server HTTP/2 engine 하나를 유일한 병목으로 단정할 수 없다. 모든 옵션 변경은 시험 process에만 적용했다.

## 별도로 관찰한 실패

1. `profile8` 및 `host16`은 모든 RPC 측정과 host native close를 통과했지만 proxy 종료 중 `queue/src/service.rs:73`의 `worker must set a failure if it exits prematurely` panic이 발생했다. Shared DPA context destroy와 process 종료는 완료됐다. 성능 구간과 종료 실패를 분리해 기록했으며 정상 lifecycle로 집계하지 않는다. 이 문제의 수정은 수행하지 않았다.
2. `inline_profile8`에 추가한 scheduler trace는 과도한 overhead를 유발했다. 약 1.14GB trace와 sample loss 37.25%, RPC deadline/cancellation 오류를 관찰했다. 이 run의 QPS/CPU/scheduler sample은 성능 또는 정상 대기 비율의 근거에서 제외했다. 상세 scheduler tracing을 중단했고, 이후 기본 구성 3회가 정상 복구된 것을 확인했다. 이 실패를 inline mode 자체의 안정성 문제로 판정하지 않는다.
3. 초기 perf report의 inline symbol resolution이 비정상적으로 느려 해당 분석 process만 종료하고 `--no-inline` report로 대체했다. 대조군의 DPU runqueue wait가 수 ms 수준인 것도 확인했다.

## 결론과 남은 범위

확인된 것은 **host core 수 부족이 아니라 DPU worker의 non-runnable 대기가 CPU 미포화의 직접적인 형태**라는 점이다. Busy-poll이 네 worker를 항상 실행시키는 구조도 아니다. HTTP/2 공유 lock 및 task 간 의존성이 관찰되며, 더 많은 client in-flight 요청이나 host core 추가로 이 대기가 사라지지 않는다.

다만 최종 33k RPC/s 한계를 host transport의 직렬화, 단일 Driver의 진행, HTTP/2 connection 단위 직렬화 중 하나로 완전히 분리하지는 못했다. 다음에 구조를 바꾼다면 connection별 task locality와 shared state 접근을 계측하고, 동일 총 connection 수에서 여러 channel/Driver로 분리하는 대조군이 유용하다. 이번 작업에서는 제품 코드를 수정하지 않았다.

## 자료와 정리

- [결과 JSON](2026-09-29_grpc-go-4core-bottleneck.json), [run별 CSV](2026-09-29_grpc-go-4core-bottleneck.csv).
- Raw 경로: 양쪽 `/tmp/dmesh-bottleneck-20260929`. 실행/프로파일/분석 스크립트와 host/DPU 로그를 보존했다.
- 로컬 `2026-09-29_grpc-go-4core-bottleneck-raw.tar.gz`에는 실패한 대용량 scheduler trace 외의 자료를 보존한다. Scheduler trace는 로컬 `2026-09-29_grpc-go-4core-bottleneck-trace/`에 별도 보존한다. 둘 다 Git 제외.
- 모든 기록된 host/DPU test process 종료, shared DPA context destroy, 잔여 channel-bench process 없음 확인. NIC/SF/EU partition 및 perf 관련 sysctl 변경 없음.

Artifact SHA-256:

- `2026-09-29_grpc-go-4core-bottleneck-raw.tar.gz`: `e7cb2962d80ff648bc33298b9ce03232a09bf344c45b1ab876252350ccd80085`.
- `invalid-scheduler.data`: `ed6aaad6360d220d2047e3253dcab4625ec611a863f5af9642c867db2cd954a0`.
