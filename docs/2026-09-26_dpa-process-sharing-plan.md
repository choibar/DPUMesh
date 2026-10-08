# DPUMesh DPA process 공유 설계

상태: **2026-09-28 DPU 측 context 공유 구현 완료.** [구현 범위와 검증](2026-09-28_dpu-shared-dpa-context.md). 아래는 당시 설계이며, host context 공유·benchmark process 통합·broker는 여전히 미구현이다. POSIX preload는 범위 밖이다.

## 1. 결정과 범위

첫 구현은 **동일 OS process 안에서 PF/app별 base DPA context 하나를 공유**한다. DPU proxy는 worker들이 이를 공유하고, host는 같은 앱의 여러 channel이 공유한다. Worker별 PE, thread pool, flow별 DPA thread·MsgQ·completion·ring·buffer는 유지한다. Channel당 Comch session도 유지한다.

이 변경은 DPA process 수를 줄인다. DPA thread 수, EU 소모, TX/RX API, descriptor 형식과 per-flow 실행 모델은 바꾸지 않는다. 앞서 보류한 per-channel DPA thread/ring 설계는 별도 단계다.

**Host-dpa 모드에서 OS process가 다른 microservice들의 host DPA context까지 공유하려면 별도 host broker가 필요하다.** `doca_dpa *`를 shared memory에 넣거나 IPC로 전달해서 직접 사용하는 방식은 채택하지 않는다. 검토한 SDK에는 DPA context export/import API가 없고, 자원 수명은 생성한 OS process에 연결된다. Host와 DPU OS 사이에서도 base context를 직접 공유하지 않는다.

**Dpu-dma 모드에서는 host 앱이 DPA context를 만들지 않는다**(`host/channel.c:443`). 앱별 process/channel/Comch를 유지하고 DPU proxy 내부 runtime만 공유하면 된다. 이 모드의 process 한도 해결에 host broker나 앱 process 통합은 필요하지 않다. 아래 host 통합과 broker 단계는 host-dpa의 host 자원 공유를 위한 설계다.

권장 적용 순서:

1. 공통 runtime과 checked lifetime을 도입한다.
2. DPU의 기존 multiworker process, host의 multiple channel에서 runtime 공유를 검증한다.
3. gRPC fixture를 client process 하나와 server process 하나로 통합해 16-core 실험의 process 한도를 없앤다.
4. Host-dpa를 유지하면서 독립 microservice process 수가 늘어나는 배포에는 같은 runtime을 소유하는 host broker를 추가한다.

## 2. 현재 구조와 자원 예산

| 현재 위치 | 현재 소유 단위 | 근거 |
|---|---|---|
| DPU base DPA context | proxy worker의 `objects`마다 하나 | `common/dpa.c:266`, `dpu/comch_server.c:1400` |
| Host base DPA context | host-dpa 모드의 `channel_dev`마다 하나 | `host/channel.c:69`, `:373`, `:393` |
| DPU thread pool | worker마다 32개 slot | `common/dpa.h:20`, `common/dpa.c:314` |
| Go transport | OS process 전체에 singleton channel 하나 | `integrations/grpc/go/dmesh.go:111`, `:129` |
| gRPC scaling fixture | client W개 process + server W개 process + DPU proxy 1개 process | 기존 PF0/PF1 측정 보고서 |

DPU proxy는 이미 한 OS process 안에 여러 worker를 둔다. 반면 Go fixture는 worker마다 host process를 띄우므로 C runtime registry만 추가해도 host process 수가 줄지는 않는다.

W=ARM worker 수, K=worker당 client connection 및 backend pool 크기일 때, 같은 PF/app을 쓰는 아래 구성의 **base DPA process 예상 수**는 다음과 같다. SF extension 개수를 base process 수와 혼동하지 않는다.

| 구성 | DPU | Host client + server | 합계 | W=16 |
|---|---:|---:|---:|---:|
| 현재 | W | W + W | 3W | 48 |
| DPU만 공유 | 1 | W + W | 1+2W | 33 |
| Host 역할별 process 통합 + runtime 공유 | W | 1 + 1 | W+2 | 18 |
| DPU와 host 모두 공유 | 1 | 1 + 1 | 3 | **3** |

이는 설계상 예상치이며 구현 후 생성 성공/해제 로그와 실제 process 수를 대조한다. PF나 app/resource 설정이 다르면 runtime도 추가된다. 다른 OS process의 client/server끼리는 같은 PF를 사용해도 직접 runtime을 공유하지 않는다.

**EU 제약은 별개다.** 현재 host-dpa fixture는 DPU에 ingress K + backend K, host에 client K + server K의 per-flow polling thread가 있어 양쪽 각각 `2WK`, 총 `4WK`개가 실행된다. 현재 kernel은 활성 flow마다 EU를 점유하는 polling 구조다.

| W=16 | DPU active threads | Host active threads | 합계 |
|---|---:|---:|---:|
| K=1 | 32 | 32 | 64 |
| K=2 | 64 | 64 | 128 |
| K=4 | 128 | 128 | **256** |

장비의 190 EU에 비해 W16/K4는 초과한다. Runtime 공유만으로 이 조건까지 실행 가능하다고 약속하지 않는다. W12/K4도 192개다. 실제 가용 EU는 partition과 runtime 내부 자원도 고려해야 한다. 우선 W16/K1·K2를 검증하고 K4의 고코어 구성에는 별도 flow/thread multiplexing을 적용해야 한다.

Worker당 32개 **미리 생성한 thread 객체**와 실행 중인 polling thread를 구분한다. DPU 16-worker pool은 객체 512개이며, 공유한다고 전역 32개 pool 하나로 축소하지 않는다.

## 3. 공통 runtime과 자원 소유권

```text
DPU proxy OS process                 Host client OS process       Host server OS process
  PF/app runtime 1개                   PF/app runtime 1개            PF/app runtime 1개
    ├─ worker 0                         ├─ channel/transport 0        ├─ channel/transport 0
    ├─ worker 1                         ├─ channel/transport 1        ├─ channel/transport 1
    └─ worker W-1                       └─ channel/transport W-1      └─ channel/transport W-1

각 worker/channel: 자기 PE, flow table, thread/MsgQ/completion/ring/buffer
공유 runtime: base context, device open 소유권, optional SF bindings, SDK 호출 직렬화
```

공통 구현 위치는 `src/transport/common/dpa_runtime.{h,c}`를 제안한다. 공개 DPUMesh API에 `doca_dpa *`를 노출하지 않는다.

| Runtime이 소유 | Worker/channel/flow가 소유 |
|---|---|
| PF device open reference와 base context | channel Comch session과 control PE |
| optional SF device reference와 extended context cache | worker/flow별 PE, completion, MsgQ, DMA ctx |
| app/config identity, state, epoch, owner/child/operation references | 기존 thread pool과 `owner[]`, flow table/generation |
| SDK API mutex와 runtime 상태 통지 | flow별 DPA thread/argument, ring, mmap, buffer |

`dmesh_dpa_thread_pool`은 private slot allocator로 남는다. `pool->dpa`는 runtime/binding에서 빌린 handle이며 pool cleanup이 base를 파기할 수 없다. Thread 구조에도 runtime/binding 소유자를 추적할 참조를 둬 memcpy·thread API wrapper가 올바른 mutex와 수명을 사용하게 한다.

Runtime registry는 **현재 OS process 안에서만** 존재한다. Key는 canonical PF identity(PCI domain/BDF와 VHCA), DPA app/ABI identity, compatible context/resource 설정이다. Comch server name, worker index, flow ID는 key에 넣지 않는다. 같은 runtime을 요청하면서 서로 모순되는 resource 설정을 주면 명시적으로 거절하고, 조용히 별도 process를 만들지 않는다. 부모에서 만든 context를 `fork()` 자식이 재사용하는 것도 지원하지 않는다.

SF 사용 시 base runtime 아래 `(SF identity/VHCA)`별 binding을 참조 계산한다. 같은 SF binding은 재사용하며 마지막 자식 해제 후에만 extended context를 destroy한다. 서로 다른 extended contexts도 같은 base의 SDK mutex를 사용한다. PF ring/buf-array와 SF completion/MsgQ의 기존 device 배치는 그대로 보존한다. **SF 추가는 공유의 필수 조건이 아니다.** 첫 검증은 PF 경로로 하고 SF binding은 후속 검증한다. 기존 host-facing 복수 SF의 CQ 실패 이력은 별도 미해결 제약으로 남긴다.

Device pointer가 같다는 이유로 open 소유권을 합치지 않는다. SDK의 성공한 `doca_dev_open()`마다 대응 close가 필요하다. Runtime이 연 PF reference, channel이 연 Comch reference, SF reference를 각각 기록한다. 동일 pointer에 대한 mmap 중복 add 방지와 close 횟수는 다른 문제다.

제안하는 내부 interface의 책임은 다음과 같다. 구체적인 함수명은 구현 시 확정한다.

```c
runtime_acquire(config, &lease);        // compatible runtime을 한 번만 생성, owner reference 취득
runtime_binding_acquire(lease, sf, &binding); // SF가 없으면 base binding
runtime_child_attach(binding, child);   // 살아 있는 종속 자원 추적
runtime_child_detach(binding, child);   // 실제 destroy 성공 후에만 감소
runtime_release_checked(&lease);        // 실패하면 유효 lease/정리 상태 보존
```

Direct DPA API 호출은 runtime-aware wrapper로 모은다. Raw context pointer를 다른 모듈에서 임의로 destroy하지 않도록 소유권을 제한한다.

DPU main은 같은 acquire interface로 runtime lease를 먼저 취득하고 worker들에게 참조를 전달하는 방식을 권장한다. 초기화 실패를 worker 수만큼 반복하지 않고 startup/shutdown 소유권도 명확해진다. 공통 registry는 host의 독립 channel 생성과 다른 native 진입점에서도 같은 규칙을 적용한다. Concurrent channel 생성에 대비해 기존 process-wide logging/trace 초기화 flag도 `pthread_once` 또는 동등한 동기화로 보호한다.

## 4. 동시성: 첫 구현은 runtime별 짧은 SDK mutex

DOCA DPA 객체는 thread-safe하지 않다. Registry/refcount만 lock하고 여러 worker가 같은 base를 동시에 호출하는 구현은 허용하지 않는다.

- **Registry lock:** runtime 검색, INITIALIZING entry 등록, reference 취득에만 사용한다. 느린 SDK 초기화 중에는 유지하지 않는다. 동시 acquire는 초기화 완료/실패를 기다리고, 같은 key의 base를 중복 생성하지 않는다.
- **Runtime SDK mutex:** base/extended와 공유 내부 상태를 만지는 DPA API 호출을 직렬화한다. Memory alloc/free·h2d/d2h memcpy·RPC·thread/completion lifecycle·DPA attachment/handle/error API를 감사해 포함한다. 개별 SDK call 또는 progress를 하지 않는 짧은 dependency 구간만 보호한다.
- **기존 worker/channel 소유권:** PE는 계속 하나의 worker 또는 기존 channel lock 아래에서만 progress한다. Flow/Go transport의 lock을 하나의 전역 data-path lock으로 합치지 않는다.
- Registry lock을 놓고 owner/flow 작업에 들어간다. Owner→runtime SDK mutex 방향만 허용하고 runtime은 owner lock을 역으로 취득하거나 callback을 dispatch하지 않는다. Runtime 상태 통지는 lock을 놓은 뒤 owner가 처리하도록 한다.
- `doca_pe_progress()`, completion 대기, quiesce 반복문 전체, application callback을 SDK mutex로 감싸지 않는다. Quiesce의 각각의 h2d/d2h 호출만 보호하고 그 사이에 mutex를 풀어 다른 worker가 진행하게 한다. Private PE callback에서 shared DPA API가 필요하면 같은 wrapper를 경유한다.
- 생성 중인 자원과 실행 중인 API operation도 참조로 보호한다. Last release가 이들과 경쟁해 context를 파기하지 못한다.

**Steady-state 호출도 존재한다.** DPU의 `shim.c:408` watermark 갱신, host의 `channel.c:1240` RX release 경로가 `doca_dpa_h2d_memcpy()`를 사용한다. 이 호출도 보호해야 하므로 “context 공유에 hot-path 비용이 없다”고 가정하지 않는다. Published cursor/sequence는 성공한 SDK write 이후에만 전진시켜 실패가 영구적인 RX stall로 이어지지 않게 한다. Watermark는 같은 flow owner가 순서대로 갱신한다.

초기 버전은 별도 관리 core/thread 없이 mutex로 정확성을 확보한다. SDK lock 대기 시간, memcpy 호출 수/지연, worker별 처리량과 tail latency를 측정한다. 병목이 확인되면 후속으로 latest-watermark 병합 queue 또는 등록 control page를 검토한다. 이때 per-flow generation, monotonic consumed sequence, DMA memory ordering와 close fence를 별도로 설계한다. 새 관리 thread를 도입하면 ARM/host CPU 예산에 포함한다. 이 최적화는 첫 process 공유 구현의 전제는 아니다.

DPA 프로그램의 mutable global/static state도 감사한다. 일반 DMA 경로의 flow별 argument는 유지하며, HPACK benchmark mode3/4/5의 함수 내부 `static` scratch(`device/dpa_kernel.c:517`, `:568`)는 동일 DPA process의 thread들이 공유한다. 병렬 실행 전에 per-thread heap scratch로 옮기거나 해당 mode의 병렬 실행을 명시적으로 제한한다. 작은 DPA stack에 큰 scratch를 옮기는 방식은 피한다.

## 5. 수명, 실패, 종료

Runtime 상태는 `INITIALIZING → READY → DRAINING → DEAD`, fatal 발생 시 `FAILED`로 전환한다. `DRAINING/FAILED`에는 새 child를 붙이지 않는다. 살아 있는 owner, child, SDK operation, 실패해서 보존한 자원이 모두 사라지기 전에는 runtime을 해제하지 않는다. 마지막 release와 새 acquire의 경쟁도 registry entry 상태로 직렬화한다.

Wrapper는 신규 작업과 정리 작업을 구분한다. `DRAINING/FAILED`에서도 기존 자원의 error 조회, quiesce/fence 확인, destroy와 release는 시도할 수 있어야 한다. 모든 API를 일괄적으로 `state != READY`일 때 거절하면 cleanup 자체가 막힌다.

정상 종료 순서:

1. 해당 channel/worker의 신규 flow와 전송을 중지한다. 다른 owner는 계속 동작한다.
2. 연결별 stop/stopped handshake와 발행된 DMA completion 회수를 확인한다. 기존 reader-detached/RX lease 조건도 유지한다.
3. 해당 flow의 MsgQ/completion/thread/argument/imported mappings를 기존 dependency 순서로 정리한다. Runtime을 공유하는 다른 flow의 객체는 건드리지 않는다.
4. Idle pre-created pool thread까지 정리하고 channel 메모리·Comch·PE를 닫는다. 어느 단계든 실패하면 pointer와 남은 runtime reference를 보존해 재시도한다.
5. 마지막 SF binding의 자식이 없어졌을 때 extended context를 destroy한다. 이미 started인 extension에 별도 start/stop은 하지 않는다.
6. Runtime owner/child/in-flight reference가 모두 없어졌을 때 base stop → destroy → 해당 device reference close를 수행한다.

현재 `channel_dev_close()`는 void이며 DPA destroy 실패를 무시하고, DPU `cleanup_objects()`도 완전한 pool/context 정리를 하지 않는다. 공유 도입 전 checked cleanup으로 바꾸고 `carrier` 및 proxy/Go shutdown까지 오류를 전파한다. Base destroy에 실패했는데 runtime record를 지워 새 context를 계속 생성하는 식의 누수도 금지한다.

Pool 초기화는 `created_count`를 각 thread 생성 성공마다 기록한다. 현재처럼 전체 loop 완료 후에만 `size`를 기록하면 중간 실패의 자원을 놓칠 수 있다. Thread 내부 alloc/create/start의 부분 성공도 각각 추적해 rollback한다.

Proxy 종료에는 owner별 barrier가 필요하다. 각 Driver가 자기 PE에서 drain·checked close를 완료하고 ACK한 뒤 shard thread를 join하고, 마지막으로 main의 runtime lease를 반환한다. 현재 버리는 shard `JoinHandle`을 보관하고 driver task 종료도 기다리도록 한다. App drain만 기다린 뒤 main이 공유 context를 파기해서는 안 된다.

Connection-local 오류와 runtime fatal을 구분한다. Local close 오류는 해당 flow를 quarantine하고 다른 flow의 base를 파괴하지 않는다. 반면 DPA process fatal은 같은 runtime의 모든 owner에 영향을 미친다. Runtime을 FAILED로 표시하고 owner별 error/close 처리를 예약한다. DMA 중단이 확인되지 않으면 mapping을 조기 회수하지 않는다. 자동 투명 재생성·미완료 RPC 재전송은 첫 버전에 넣지 않는다. 재생성 시 runtime epoch를 바꿔 이전 completion/lease가 새 자원에 적용되지 않게 한다.

공유하면 독립 worker별 DPA process 격리는 줄어든다. Isolation이 필요한 배포는 명시적 runtime group으로 몇 개의 base에 나눌 수 있지만, 초기 기본값은 compatible PF/app당 하나다. 조용한 per-worker fallback은 process 한도를 다시 유발하므로 사용하지 않는다.

## 6. Host/Go fixture 통합

Native runtime 공유를 실제로 사용하려면 **한 OS process 안에 여러 독립 channel을 열 수 있어야 한다.** 현재 `getenv()` 기반 server/workload 설정과 Go singleton을 그대로 둔 채 goroutine만 추가하면 모든 부하가 같은 channel/worker로 몰린다.

- Native channel 생성에 immutable options 전달 경로를 추가한다. Server name, pod/workload identity, registry/service와 backend pool 등 현재 channel 선택에 쓰는 값을 channel별로 복사한다. 기존 `dmesh_create_channel()`은 환경변수를 읽는 호환 wrapper로 유지한다. 환경변수를 worker마다 `setenv()`로 바꾸는 방식은 쓰지 않는다.
- Public options에는 service/channel 설정을 노출하고 raw DPA context는 노출하지 않는다. DPA device/app/runtime 정책은 내부 transport 설정으로 해석한다. API 추가는 기존 구조체 layout을 무단 변경하지 않는 additive 방식으로 한다.
- Go에 명시적인 `Transport` instance를 두고 instance별 Dial/Listen/Close를 제공한다. Instance마다 native channel/EQ/poller/mutex를 소유한다. 기존 package-level API는 default instance wrapper로 유지한다.
- Client process 하나에서 W개 transport와 각각 K개 gRPC connection을 연다. Server process 하나에서 W개 transport/listener와 해당 backend pool을 운영한다. 각 transport는 기존 DPU worker/server name과 workload identity를 그대로 사용한다.
- GOMAXPROCS, host CPU affinity, 전체 RPC 병렬도 `W×K×64`, 64B payload, warmup/측정 barrier는 기존 조건과 맞춘다. 단일 process로 바꾸면 Go scheduler/GC 조건도 달라지므로 이전 multi-process 결과와 실행 구성을 함께 표기한다.

목표는 각 역할의 channel/EQ 병렬성을 유지하면서 host base를 역할당 하나로 줄이는 것이다. Listener나 payload ring을 전역 하나로 합치는 작업은 아니다.

## 7. 별도 microservice 사이의 공유: host broker 후속 설계

실제 서비스들이 서로 다른 OS process/container라면 위 fixture 통합을 강요할 수 없다. **Host-dpa의 host context를 이 앱들 사이에서도 공유하려는 경우**, host node의 작은 broker가 PF/app runtime과 DPA 자원을 소유하고 앱은 공유 메모리의 ring/buffer를 사용하게 한다. HTTP/2/gRPC 처리와 routing은 기존 앱/DPU proxy에 남긴다. 한도 내의 기존 host-dpa 배포는 broker 없이 앱별 context로도 동작하며, dpu-dma에는 이 broker가 필요하지 않다.

```text
Microservice A ── control IPC ──┐
  shared ring/buffer A         │
Microservice B ── control IPC ──┼─ host DPA broker ── base runtime / optional bindings
  shared ring/buffer B         │                       ├─ flow A DPA thread/resources
Microservice C ── control IPC ──┘                       └─ flow B DPA thread/resources
```

구체적인 계약:

- Control은 Unix domain socket으로 OPEN/REGISTER/CLOSE를 요청한다. Broker가 app credentials와 생존을 확인하고 `(broker epoch, channel id, flow id, generation)`을 발급한다. Native channel API 뒤에 broker transport를 두어 앱은 DOCA handle을 받지 않는다.
- 첫 버전은 broker가 만든 memfd-backed arena를 앱에 FD로 전달하고 양쪽에서 mmap한다. Broker가 등록과 DPA 접근 수명을 소유한다. Descriptor에는 process별 가상주소 대신 buffer ID/offset/length를 쓰고 주소 변환은 등록된 mapping을 기준으로 한다. 기존 arbitrary app heap을 바로 import할 수 있다고 가정하지 않는다.
- Memfd mapping의 DOCA 등록·DMA 동작은 broker 착수 시 최소 smoke test로 먼저 검증한다. 앱에게 전달하기 전에 grow/shrink 방지 seals를 적용해 등록한 backing size가 바뀌지 않게 하고, broker가 등록 해제까지 FD/mapping reference를 유지한다.
- 정상 payload 경로는 앱이 arena/ring에 쓰고 DPA가 처리한다. Broker가 매 payload를 다시 복사하거나 IPC 메시지로 운반하는 설계는 피한다. 기존 라이브러리의 buffer 채움 비용에 추가 broker copy가 생기지 않는 것을 목표로 한다.
- Channel별 Comch session, flow별 ring/thread/completion의 소유자는 broker로 이동한다. App에는 EQ/event와 buffer lease만 전달한다. API 호출별 context 공유가 아니라 **자원 owner를 한 process로 모으는 구조**다.
- Broker가 MsgQ/completion PE를 progress하므로 CPU 작업은 계속 발생한다. 앱별 shared completion/credit ring과 eventfd로 결과를 전달하고, ring이 가득 차면 completion을 버리지 않고 admission/credit에 backpressure를 건다. PE는 broker worker별 단일 owner를 유지한다. Completion/credit 전달 비용과 broker worker CPU를 성능 결과에 포함한다.
- 서로 신뢰하지 않는 앱이 descriptor에 arbitrary mmap handle/address를 적을 수 없어야 한다. Kernel은 broker가 설정한 flow별 mapping/bounds를 사용하고, 읽은 descriptor의 offset/length/generation을 검증한다. 초기 설계에서 필요한 descriptor/argument 변경과 memory protection 검증을 별도 작업으로 취급한다.
- App crash/IPC 종료 시 broker가 flow를 quiesce하고 DMA completion·lease를 회수한 뒤 unmap한다. App가 종료돼도 broker의 arena reference는 fence까지 유지한다. Broker crash는 그 broker에 연결된 모든 앱의 channel 실패로 취급하고 재접속 시 새 epoch를 사용한다.

하나의 host PF/app broker와 하나의 DPU PF/app runtime만 사용한다면 base process 수는 앱 수와 무관하게 host1+DPU1=2가 목표다. 여러 PF/app을 사용하면 그만큼 늘어난다. Broker는 EU/thread 부족을 해결하지 않고, lifecycle·IPC·격리·공유 메모리 등록을 추가하므로 첫 단계와 같은 patch에 넣지 않는다. SF를 반드시 쓰는 구조도 아니며 복수 SF datapath는 검증된 범위에서만 확장한다.

## 8. 구현 순서와 완료 조건

| 단계 | 변경 위치/작업 | 완료 기준 |
|---|---|---|
| 1. Runtime 기반 | 새 `common/dpa_runtime.*`, `dpa.h`, `object.h`; base/context ownership 분리와 API wrappers | 동시 acquire에서 base 1개, 참조/부분 실패/재시도/마지막 release 검증 |
| 2. DPU 적용 | `common/dpa.c`, `dpu/comch_server.c`, `common/object.c`, proxy `doca/src/shim.c`와 main/Driver shutdown | W worker가 base 1개 공유; worker 하나 close 중 sibling 트래픽 유지; drain ACK/join과 완전 cleanup |
| 3. Host 적용 | `host/channel.c`, `core/carrier.c`; checked close와 borrowed binding | 같은 PF의 복수 channel에서 base 1개; 하나의 close가 다른 channel을 끊지 않음 |
| 4. Fixture 통합 | native options 경로, Go `dmesh.go`, bench client/server 및 runner | W개의 독립 transport가 동일 OS process에서 올바른 DPU worker로 연결 |
| 5. 기능·성능 확인 | native cleanup/session tests + 실제 gRPC fixture | W16/K1·K2에서 예상 base3, 오류 없는 RPC 및 정상 종료, lock 비용 보고 |
| 후속 A | flow/thread multiplexing | W12/16 K4의 EU 초과 문제 해결; 별도 설계/측정 |
| 후속 B | host broker, IPC/arena/lease와 kernel bounds 검증 | 독립 app process 증가에도 base 수 일정, app crash 중 sibling 보존 |

구현 후 검증은 다음 순서로 수행한다.

1. 의미 있는 lifecycle tests: 중복 초기화 경쟁, 마지막 release와 acquire 경쟁, child destroy 실패 시 base 보존, 다른 owner가 있는 close, failed init rollback, SF-before-base 순서, fatal fan-out.
2. 단일 PF에서 2-owner 최소 시험: 각자 thread/메모리 생성, owner A close 후 B 동작, 마지막 close 후 자원 0. 같은 base를 가진 concurrent watermark 갱신과 close 교차도 검증한다.
3. W1→2→4→8→12→16, K1/K2 순서로 64B echo/connection당64 RPC를 측정한다. K4는 EU가 수용하는 조건까지만 수행한다. SF/network/firmware 재설정 없이 기본 경로부터 확인한다.
4. 동시 연결/종료/reconnect, delayed RX release와 partial cleanup 중 payload 무결성, 종료 후 context 누수를 확인한다. 기존 EBADMSG teardown 실패를 공유 도입의 성공으로 덮지 않는다.
5. Process 수는 각 owner의 create/start/stop/destroy 로그와 PID를 함께 대조한다. DPU의 PF-scoped `dpaeumgmt status` 하나를 chip 전체 합계로 쓰지 않는다. Runtime별 active refs/children/state/lock wait, worker별 처리량/latency/CPU를 기록한다.

첫 구현의 완료는 **base process 공유와 정상 종료, W16/K1·K2 검증**이다. 임의 개수의 독립 microservice 공유와 W16/K4 실행은 각각 broker 및 thread multiplexing 단계가 필요하다.

## 9. 근거

- [DOCA DPA 3.5: 객체 thread-safety와 PF/SF context 모델](https://networking-docs.nvidia.com/doca/archive/3-5-0/doca-dpa)
- [DPA Development 3.5: 생성한 OS process와 DPA 객체 수명](https://networking-docs.nvidia.com/doca/archive/3-5-0/dpa-development)
- [BlueField-3 firmware: system-wide process 한도](https://networking-docs.nvidia.com/doca/archive/3-3-0/changes-and-new-features)
- [28개 경계 직접 검증](../bench-results/2026-09-26_dpa-process-limit.md)
- [SF extended context 최소 시험](../bench-results/2026-09-26_dpu-local-sf-extended-test.md)
- [Host-dpa ARM scaling: EU/thread 산정](../bench-results/2026-09-26_grpc-go-64b-host-dpa-arm-scaling.md)
- [PF0/PF1 fixture와 process 배치](../bench-results/2026-09-26_grpc-go-64b-host-dpa-pf01.md)
- [기존 복수 host-facing SF 제약](2026-09-25_nvidia-case-sf-host-dpa.md)
