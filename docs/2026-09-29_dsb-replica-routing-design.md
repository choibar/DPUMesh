# DSB replica routing 변경 설계

2026-09-29. 승인된 설계 및 구현 기록. 아래 설계는 원안을 보존하며, 실제 구현 범위와 검증 결과는 문서 마지막에 기록한다.

## 2026-09-30 control dispatcher 구현 변경

아래 원안의 `endpoint.worker = DMA/H2 owner` 가정은 새 proxy 구현에서 사용하지 않는다.
기존 `DPUMesh0..N` 이름은 Host 설정 호환성을 위한 Comch control alias이며, 한
`dmesh-control` thread가 모든 alias의 session을 관리한다. Data worker는 별도 device
reference와 PE/DMA/DPA 자원을 소유한다. Native multiplexed flow는 peer Comch consumer를
사용하지 않고 MsgQ를 사용하므로, 실제 `doca_comch_connection *`를 worker에 넘기지 않는다.
Worker에는 복사한 OPEN metadata와 flow key/location만 전달한다.

- `src/transport/dpu/dispatcher.{c,h}`: bounded control mailbox, slot reservation,
  session/flow directory, READY/export/CLOSE reply 전달. Payload/descriptor/data completion은
  dispatcher를 통과하지 않는다. Control thread는 eventfd와 Comch notification fd를 기다린다.
- `src/transport/dpu/placement.{c,h}`: DOCA 객체에 접근하지 않는 배정 policy interface.
  `DMESH_FLOW_PLACEMENT=least-flows`(기본) 또는 `round-robin`. Assigned count는 opening과
  spare를 포함한다. 정책 변경은 새 flow에만 적용하며 CPU 부하 균등화는 보장하지 않는다.
  DSB launcher에서는 `DSB_FLOW_PLACEMENT`로 설정한다.
- Flow key `(session_id, session_epoch, flow_id, generation)`와 location
  `(worker, slot, ownership_epoch)`를 분리했다. Ownership epoch는 현재 1이며 이동 API는 없다.
  Worker control 응답 및 Rust IO registration은 식별자를 확인한다. 향후 migration에는
  HTTP/2 상태, 진행 중 RPC, DMA quiescence, Host export 교체에 대한 별도 구현이 필요하다.
- `linkerd/doca/src/backend.rs`: replica당 활성 backend flow 하나와 spare 후보를 구분한다.
  실제 flow owner를 사용하며 manifest worker로 H2 runtime을 선택하지 않는다.
  ERROR/CLOSING은 disable, ConnClosed는 physical retirement로 구분한다.
  기존 H2 owner lease와 physical flow가 모두 정리되기 전에는 spare를 활성화하지 않는다.
- `dmesh_pool.rs`: 모든 source worker가 replica별 H2 service 하나를 공유한다. 설정이
  호환되지 않는 두 번째 pool은 거절한다. Reconnect 시에는 이전 owner service를 drop한 뒤
  새 flow owner runtime에 pool receiver를 넘긴다. Live-flow migration은 아니다.
- Host public API, channel당 Comch 하나, session wire v1과 flow 최대 32개는 유지한다.
  Shared DPA context와 per-flow DPA thread/EU 단위도 바꾸지 않는다.

이 항목은 구현 기록이며 기존 벤치마크 결과에 소급 적용하지 않는다. 사용자 요청에 따라
새 dispatcher의 장비 테스트와 성능 평가는 아직 실행하지 않았다. Legacy C benchmark의
독립 worker 진입점은 유지하고, 실제 Linkerd proxy는 group initializer를 사용한다.
Dispatcher는 data worker와 별도 thread이므로 CPU 예산에는 그 사용량을 포함해야 한다.

구현 빌드 확인: DPU transport의 Ninja 빌드, 현재 ARM 작업환경의 Host library 빌드,
`RUSTFLAGS='--cfg tokio_unstable' cargo build -p linkerd2-proxy -j4`의 개발 실행파일 링크 완료.
새 실행파일은 `linkerd2-proxy/target/debug/linkerd2-proxy`이다. Release 전체 LTO 빌드는
완료하지 않았으며, 기존 `target/release` 실행파일을 이 구현의 결과로 사용하면 안 된다.
테스트 실행·실장비 연결·벤치마크는 수행하지 않았다.

## 목표와 근거

[실측 audit](../bench-results/2026-09-29_deathstarbench-replica-audit.md)에서 서비스당
4 replicas의 DMA 요청이 마지막 replica 하나에 집중됐다. 원인은 동일 VIP로 backend를
등록하고 mock policy가 그 VIP 하나를 Forward하며 H2 client가 선택한 연결을 재사용하기 때문이다.

목표는 다음 세 가지다.

1. Host client는 서비스 VIP만 알고, DPU가 **새 gRPC RPC/HTTP2 stream마다** replica를 선택한다.
2. Replica 추가/제거가 endpoint 집합과 실제 처리량에 반영된다.
3. Replica 분산을 위해 backend connection/DPA flow 수를 worker 수만큼 곱하지 않는다.

RPC 하나의 메시지들은 같은 endpoint에 남는다. Streaming RPC를 메시지별로 분산하지 않는다.
Per-process channel/Comch, 고정 DMA worker, per-flow DPA thread, TX/RX buffer 계약은 유지한다.
200K echo 처리량을 DSB 목표치로 바로 사용하지 않는다. 동일 workload의 수정 전후를 비교한다.

## 권장 구조

```text
Host client (process channel -> ingress worker A)
  Dial(service VIP)
       |
       v
DPU: service policy -> endpoint discovery -> per-RPC P2C/Peak-EWMA
       |
       +-- endpoint E0 -> reusable H2 client -> replica 0
       +-- endpoint E1 -> reusable H2 client -> replica 1
       +-- endpoint E2 -> reusable H2 client -> replica 2
       +-- endpoint E3 -> reusable H2 client -> replica 3
```

기존 `linkerd/app/integration/src/policy.rs::backend()`는 이미
`Kind::Balancer(BalanceP2c)` + Destination discovery + Peak-EWMA를 만든다.
`http/concrete.rs`와 `http/concrete/balance.rs`의 HTTP request balancer를 재사용한다.
새 replica 선택 알고리즘이나 gRPC protobuf 변환은 만들지 않는다.

### 서비스 주소와 endpoint 주소

| 항목 | 예시 | 역할 |
|---|---|---|
| service identity | `srv-search` | policy, metrics, discovery 이름 |
| service VIP | `10.80.0.3:8082` | 모든 caller가 연결하는 논리 주소 |
| replica endpoint | `10.81.0.9:8082` | 특정 host process의 DMA destination key |
| TCP endpoint | `127.0.0.1:18882` 등 | 직접 TCP baseline의 실제 listener |
| worker | `DPUMesh0` | 해당 process의 channel/DMA flow 소유 worker |

Endpoint는 기존의 고유 Pod IP + 서비스 port를 사용한다. Replica를 재배치하거나 추가해도
기존 replica IP가 바뀌지 않도록 manifest에 명시하고, 전체 process 순번에서 매번 재계산하지 않는다.
Host의 Pod IP가 이 머신의 실제 NIC에 bind되어야 하는 것은 아니다. DMA destination key다.
공용 API에 endpoint별 QP나 새로운 conn object를 추가하지 않는다.

Topology 예시(필드 이름은 구현 시 기존 schema와 조정):

```json
{
  "schema_version": 2,
  "services": {
    "srv-search": {
      "vip": "10.80.0.3",
      "port": 8082,
      "discovery": "srv-search.dmesh:8082",
      "endpoints": [
        {"id": "search-0", "dma": "10.81.0.9:8082", "worker": 0,
         "tcp": "127.0.0.1:18082", "enabled": true},
        {"id": "search-1", "dma": "10.81.0.10:8082", "worker": 1,
         "tcp": "127.0.0.1:19082", "enabled": true}
      ]
    }
  }
}
```

Host/DPU는 같은 manifest generation/hash로 시작한다. VIP/endpoint 중복, service/worker 불일치,
용량 초과를 시작 전에 거절한다. DSB 제어 정보의 정본은 이 manifest이며 Consul과 이중 관리하지 않는다.
Consul 등록의 replica ID는 유지하고 DMA replica의 광고 주소는 해당 endpoint로 변경한다.
향후 Kubernetes/Consul watcher 연결은 같은 endpoint discovery 입력을 공급하는 별도 단계로 둔다.

### Host adapter

- Client `DialOptions()`는 계속 VIP로 연결한다. 단일 VIP에 대한 Go `pick_first`는 유지 가능하다.
  Replica 분산은 DPU가 수행하므로 client가 모든 replica에 DMA flow를 열 필요는 없다.
- Server `DPUMESH_SERVICE`는 VIP에서 자신의 endpoint로 변경한다.
  `Listen()`의 일치 검사도 service VIP 대신 manifest의 해당 replica endpoint를 확인한다.
- `Registration()`은 service 이름과 replica ID를 유지하고 endpoint를 광고한다.
- Native `src/core/service_resolve.c`는 IPv4 literal:port를 직접 해석할 수 있다.
  주소 분리를 위해 공개 C API나 session wire format을 바꿀 필요는 없다.
  `DPUMESH_CONFIG` 생성만으로 완료했다고 판단하지 않고 실제 `flow.dst`를 확인한다.
- TCP baseline도 동일 endpoint 정보에서 TCP address를 읽고 기존 round_robin으로 연결한다.

### DPU policy / discovery

- Manifest mode에서 `VIP -> service discovery name`을 해석하고 현재의 단일 `Forward`를
  `BalanceP2c`로 교체한다. HTTP2 경로를 사용해야 한다. Opaque 전송은 stream별 LB가 아니다.
- `mock-destination`은 `Get(path)`에 해당하는 replica endpoint 집합을 반환한다.
  최초 snapshot, 이후 Add/Remove 업데이트, watch 재연결 시 snapshot을 처리한다.
  다른 service endpoint나 기본 echo backend로 암묵적으로 fallback하지 않는다.
- 처음에는 명시적 manifest reload로 membership을 갱신한다. 파일은 atomic replace와
  generation으로 관리한다. 잘못된 업데이트는 마지막 정상 snapshot을 유지하고 오류를 기록한다.
  Proxy의 endpoint 설정과 mock 제어 측이 같은 업데이트를 적용했음을 확인한 뒤 시험한다.
- Membership(구성상 존재)과 transport readiness(현재 전송 가능)는 분리한다.
  Host 시작 전 endpoint도 알려진 DMA 목적지로 인식하고 전송 불가 시 bounded wait/failfast한다.
  Replica별 RPC health check를 추가한다. VIP health 성공만으로 전체 replica ready라 판단하지 않는다.
- P2C/Peak-EWMA는 응답 시간과 가용성에 따라 선택하므로 정확히 25%씩 분배하는 것을 요구하지 않는다.
  동일 성능 시험에서는 모든 replica가 처리하고, 느린 replica에는 적게 보내는지 검증한다.

업데이트 순서도 규정한다. Add는 proxy의 known-DMA 선언을 먼저 적용한 뒤 discovery에 노출한다.
Remove는 discovery에서 먼저 빼고 drain하며, 예전 cache가 남아 있는 동안 known-DMA 표식을
유지해 TCP fallback을 방지한다. Reload 중 일부 구성요소만 갱신되면 새 generation의 준비 완료를
보고하지 않는다. 초기 DSB에서는 explicit reload와 적용 확인으로 이 순서를 보장한다.

### Backend registry와 장애 처리

기존 `SocketAddr -> backend channel pool`은 유지하고 key의 의미를 endpoint로 바꾼다.
같은 key의 여러 channel은 **동일 replica의 예비 연결**만 의미한다.
`take()`의 FIFO/LIFO 순서는 replica 선택에 관여하지 않는다. 기존 (owner, slot) 삭제 보호는 유지한다.

현재 `was_published()`만 사용하면 manifest에 있으나 한 번도 publish되지 않은 endpoint가
TCP로 fallback한다. 알려진 DMA endpoint를 manifest에서 선언하고 해당 목적지는 publish 전,
고갈, 연결 종료 모두 TCP connect를 하지 않도록 connector를 바꾼다. 일반 TCP 목적지는 기존대로 처리한다.

- Spare를 `take`해서 idle pool이 비어도 사용 중 H2 연결이 있으면 endpoint는 살아 있다.
  `idle_channels == 0`을 사망 판정에 쓰지 않는다.
- 마지막 가용 transport가 닫히면 endpoint를 unready로 전환하고 Linkerd readiness/backoff와
  연동한다. 새로운 backend 등록으로 복구할 수 있어야 한다.
- Remove는 신규 선택을 막고 기존 RPC를 drain한 뒤 pool을 닫는다. 제한 시간이 지나면 남은
  요청을 종료하며 queue/RPC의 deadline과 cancellation을 유지한다.
- Process 재시작은 proxy에서 endpoint 연결 세대로 구분한다. 과거 pool이나 지연된 close가 새
  pool을 지우지 않도록 publication token과 현재 세대를 대조한다. Native wire 확장을 전제로
  하지 않으며 기존 event 순서와 slot 재사용 규약을 테스트한다.
- 살아 있는 replica로 **새로운** RPC를 보낼 수 있어야 한다. 이미 보낸 예약 등의 RPC를
  자동 재송신하지 않는다. 초기 설정에서 추가 retry는 활성화하지 않는다.

## Worker와 H2 pool의 소유권

### 단계 A: 최소 routing 수정

기존 shared outbound stack을 유지하고 endpoint 분리 + 기존 HTTP balancer + readiness를 적용한다.
이를 통해 모든 replica가 같은 VIP의 예비 연결로 보이는 결함을 수정하고 실제 분산을 확인한다.
다만 최초 사용 runtime으로 cache/H2 task가 집중될 가능성은 남는다. A 완료를 16-core scaling
완료로 간주하지 않는다. H2 연결 수를 측정하고 endpoint별 pool 상한을 확인한다.

### 단계 B: worker scaling을 위한 소유권 정리

```text
worker A: ingress H2 + service policy + replica balancer
worker B: ingress H2 + service policy + replica balancer
                  |    |
                  v    v
       shared endpoint transport directory
       E0 -> owner worker 0의 H2 client / DMA pump
       E1 -> owner worker 1의 H2 client / DMA pump
```

Logical routing/balancer는 ingress worker마다 구성한다. Backend H2 transport는 endpoint별로
공유하고 **backend DMA flow를 가진 worker에서 생성하고 실행한다**. Caller worker는 용량이
제한된 request handle을 통해 전달한다. 같은 worker면 불필요한 cross-worker queue를 피한다.
HTTP2를 추가로 encode/decode하는 중간 연결은 만들지 않는다.

Directory는 endpoint transport handle을 관리하며, 모든 RPC를 하나의 global task로 처리하지 않는다.
Map lock은 lookup/create/retire에만 사용하고 request body 전송 중에는 잡지 않는다.
RPC body/trailers는 stream으로 전달하고 whole-body buffering을 추가하지 않는다.
Queue 대기에도 deadline/cancellation과 capacity 제한을 적용한다.

Pool 공유 key는 endpoint만으로 부족할 수 있다. 적어도 endpoint 세대, protocol,
transport/TLS identity 설정 등 연결 호환성을 포함한다. Service/source별 policy, authorization,
route filter, request metrics는 상위 stack에 두어 endpoint 공유로 우회하지 않게 한다.
현재 plaintext DSB에서는 같은 endpoint의 transport 설정이 공통이므로 pool 하나로 모을 수 있다.

구현 지점은 `push_http_tcp_client` / reconnect·`SpawnReady` 부근과 HTTP endpoint stack 경계다.
**Connector의 `take()`만 owner worker에서 실행해도 H2 task의 owner는 고정되지 않는다.**
Client 생성, connect/reconnect future 실행, H2 driver spawn 모두 owner runtime에서 실행해야 한다.
기존 service queue/reconnect와 이중 buffering 또는 독립 재접속이 생기지 않도록 작은 prototype으로
먼저 검증한다. 전체 stack 하나를 그대로 global 공유하면 logical routing의 runtime 집중도 남는다.

피해야 할 구성은 worker마다 모든 stack과 backend H2 pool을 복제하는 방식이다.
W workers × R replicas만큼 연결이 생겨 native channel의 32 slots와 DPA flow 예산을 압박한다.
단순히 worker별 `outbound.mk()`를 호출하는 것으로 scaling 수정을 완료했다고 판단하지 않는다.

## 자원 예산과 배치

각 host process는 계속 channel/Comch 하나를 사용한다. 초기값은 replica마다 사용 중 backend H2
연결 1개, spare 1개로 `BACKEND_POOL=1 / BACKEND_MAX=2`다. H2 stream은 연결을 늘리지 않고 다중화한다.
Reconnect/make-before-break에 세 번째 연결이 필요하면 먼저 별도 예산을 확보한다.

Worker i의 보수적 flow 예산:

```text
flow_budget[i] = 해당 worker process들의 outbound client connections
               + 해당 worker replica들의 backend_max 합계
               + probe / reconnect / drain 중 추가 flow reserve
```

DSB 9 services × 4 replicas, frontend 4개가 모든 edge를 유지하면 backend 최대72 +
frontend outbound28 + search outbound8 = **108 flows**다(추가 reserve 제외).
4 workers에 균등 배치하면 worker당27로 현재32 slots 안에 들어가지만 여유는 작다.
단계 B도 backend pool을 공유하므로 기본 예산이 worker 수만큼 곱해지지 않는다.
9 services ×16 replicas + frontend16이면 reserve 전432 flows다. 총512 slots만으로 실행 가능하다고
판단하지 않고 EU, native channel slots, 실제 DPA 할당 한도를 함께 검사한다.

서비스별 replica를 여러 worker로 나누고 모든 서비스의 마지막 replica가 같은 worker에 모이지
않게 한다. 처음에는 명시적 staggered 배치를 쓰고, 이후 실제 서비스별 RPC/CPU로 가중 배치를 비교한다.
하나의 host process를 여러 DMA worker로 나누는 multi-session 변경은 이번 범위에 포함하지 않는다.
16 workers에서도 hot replica endpoint가4개뿐이면 backend 처리가16 cores를 사용한다고 보장할 수 없다.

## 구현 순서와 변경 위치

| 순서 | 변경 | 주요 대상 | 완료 조건 |
|---|---|---|---|
| A1 | VIP/endpoint schema·stable address·예산 검사 | `topology.py`, overlay `transport.go`, tests | 모든 replica의 `flow.dst`가 다르고 caller는 VIP 유지 |
| A2 | Manifest policy/discovery, P2C, 업데이트 watch | `mock-policy.rs`, `mock-destination.rs`, launcher | client H2 연결 하나에서도 여러 replica로 RPC 분산 |
| A3 | Known-DMA 판정, readiness·close/restart | `doca::backend`, `app/src/dmesh.rs`, `tcp/connect.rs` | 미등록/연결 종료 시 TCP fallback 없이 정상 복구 |
| A4 | RPC/endpoint/worker 관측과 비교 | validate/measure harness, proxy metrics | 분산과 queue/연결 수 실측 |
| B1 | Owner runtime의 H2 client prototype | HTTP client/endpoint stack, worker wiring | 생성 순서와 무관한 owner, bounded queue와 cancel 검증 |
| B2 | Worker별 routing과 endpoint pool 공유 | `app/src/lib.rs`, outbound HTTP stack | 연결 수가 W×R로 늘지 않고 worker별 처리 확인 |
| B3 | 4/8/16 workers 재측정 | benchmark harness | 같은 workload·CPU 배치·부하로 수정 전후 비교 |

DSB manifest mode와 기존 echo-target mode는 배타적으로 설정하고 혼용 시 시작을 거절한다.
기존 echo와 일반 TCP proxy 경로를 회귀 검사한다. DPA kernel/API 변경을 routing 수정에 섞지 않는다.

## 검증 기준

1. **Client 연결 하나에서도 분산**: service 하나에4 replicas를 두고 여러 RPC를 보내 method별
   완료 수를 확인한다. HTTP connection 수를 늘려서 생긴 분산으로 대체하지 않는다.
2. **전체 replica 확인**: endpoint별 직접 health/probe 후 VIP 업무 결과를 TCP와 비교한다.
   Review/attractions는 mixed workload에서 호출하지 않으므로 별도 probe로 확인한다.
3. **P2C/readiness**: 같은 성능의 echo replicas에서 집중이 없음을 확인하고, 하나만 느리게 하거나
   정지해 가용성과 선택 수의 변화를 검사한다. 정확히25%씩 분포하는 것은 요구하지 않는다.
4. **Lifecycle**: 시작 순서 변경, idle cache 만료, remove/drain, 재등록, 동일 endpoint 재시작,
   spare 없이 active H2만 생존, 재접속 경쟁, 전체 replica 정지, deadline/cancel을 검사한다.
5. **소유권**: ingress worker, endpoint owner, H2 task worker, backend flow를 대응시킨다.
   첫 readiness probe의 worker를 바꿔도 단계 B의 owner가 바뀌지 않아야 한다.
6. **자원**: endpoint별 active/idle/connecting H2, worker별 flow/DPA thread, queue depth,
   RPC 시작/완료/오류, bytes, CPU를 기록한다. Busy-poll CPU만으로 유효 처리량을 판단하지 않는다.
7. **성능**: 먼저 audit과 같은 W4/R4/4FE/host12cores에서4k/8k 부하를 재실행한다.
   이후 R1/2/4, W4/8/16을 동일 host 배치·동시 HTTP 수에서 각각3회 측정한다.
   HTTP/s와 내부 RPC/s, p99, 오류, CPU를 함께 보고한다. Direct TCP는 mesh와 동등한 baseline이 아니다.
8. **종료**: 모든 host/proxy process 및 flow/DPA 자원이 해제됐는지 확인한다. NIC/SF/EU를 재설정하지 않는다.

단계 A를 첫 review 가능한 routing 수정으로 만들고 실측 후 단계 B로 진행한다.
Replica 분산 수정과16-core scaling 수정은 각각 별도의 수용 기준으로 판단한다.


## 구현 기록 (2026-09-29)

단계 A와 B를 구현했다. Host는 service VIP로 연결하고 서버는 고유 replica endpoint를
등록한다. Manifest를 읽는 mock policy/destination이 기존 P2C/Peak-EWMA HTTP balancer에
membership을 공급한다. `DMESH_SHARD_ROUTES=1`이면 ingress worker마다 outbound routing
stack을 만들고, 호환되는 endpoint의 H2 transport는 지정 DMA worker 하나에서 공유한다.

- 공유 key: endpoint address, 고정 owner worker, H2 parameters, TLS identity/configuration.
  Directory는 weak reference를 보관하여 마지막 routing handle이 없어지면 pool도 종료한다.
- Publication의 `(owner, slot)`과 live counter는 idle spare 수와 분리했다. 같은 주소의
  process 재시작은 기존 reconnect layer가 처리한다. 별도 wire generation은 추가하지 않았다.
- Manifest update는 generation을 올려 atomic replace한다. 세 소비자의 적용 로그를 확인한다.
  Known-DMA tombstone과 connector의 최신 manifest 확인으로 초기 publish 전/제거 후
  TCP fallback을 막는다. 현재 관리 도구는 이미 선언된 endpoint의 enabled 변경을 지원한다.
- Owner queue는 128 requests, 내부 client readiness 대기는 최대 3초다. 호출 취소는 queue
  대기와 응답 future에 전달되며 body/trailers는 스트리밍으로 넘긴다. 자동 RPC retry는 없다.
- 같은 worker에서 들어오는 요청도 같은 bounded queue를 사용한다. 별도의 same-worker
  fast path는 넣지 않았다. 별도 H2 hop이나 encode/decode는 추가하지 않는다.
- Remove는 신규 요청을 차단하고 client handle을 내려놓는다. 이미 dispatch된 RPC는 caller의
  취소/deadline 및 기존 H2 lifecycle로 종료한다. 원안의 독립적인 강제 drain timer와 별도
  endpoint-generation key는 구현하지 않았다. 장시간 streaming drain과 비정상 crash 중의
  slot 재사용 race는 이번 unary DSB 검증만으로 보장하지 않는다.
- Kubernetes/Consul watcher는 이번 범위에 없으며 명시적 manifest가 제어 정보의 정본이다.

[구현/하드웨어 검증 보고서](../bench-results/2026-09-29_deathstarbench-replica-routing.md)에
단일 연결 분산, 36개 endpoint readiness, membership 제거/복구, Host process 재시작,
4/8/16-worker 측정과 자원 회수 결과를 기록한다. 초기 설계 예시와 달리 실제 주소는
`10.81.<service-index+1>.<replica-index+1>:port` 형식이다.
