# Linkerd control plane 연결 — DPU proxy가 클러스터의 정책을 받는다

상태: **구현됨.** jet2 ↔ jet2-dpu PF1에서 Online Boutique 32 pod로 확인했다(아래 "확인").
mock control plane(`scripts/run-local-mock-services.sh`)은 그대로 남아 있고, proxy의 환경 변수만 다르다.

DPU의 `linkerd2-proxy`는 노드의 모든 pod를 대신하는 proxy 하나다. sidecar는 자기 pod 하나의
identity와 inbound 정책만 알면 되지만, 이 proxy는 flow마다 출발 pod와 목적지 pod가 다르다.
아래는 그 차이를 Linkerd의 세 서비스(identity, destination, policy)에 맞춘 방식이다.

## 무엇을 어디서 받나

| 받는 것 | Linkerd 쪽 | proxy 쪽 |
|---|---|---|
| proxy 자신의 인증서 | `linkerd-identity`: ServiceAccount token + CSR | stock (`LINKERD2_PROXY_IDENTITY_*`). control plane과의 mTLS에 쓴다 |
| outbound 정책 | policy controller: 목적지 `<Service ClusterIP>:<port>` | stock outbound stack. HTTPRoute/GRPCRoute, retry, timeout, circuit breaking, split, header filter |
| endpoint | `linkerd-dst`: Service의 endpoint `<pod IP>:<port>` | 그 주소가 host가 listen한 주소와 같으면 DMA backend channel로 보낸다 |
| inbound 정책 | policy controller: 목적지 **pod**의 `(workload, port)` | 목적지마다 따로 조회한다(아래) |

flow의 출발 identity는 host 라이브러리가 flow id에 실어 보내는 workload 문자열
(`DPUMESH_WORKLOAD`, 63 byte 이하)이다. 같은 노드 안의 전달이라 handshake는 없고, proxy는 그
문자열을 mesh TLS로 확인된 client id처럼 다룬다(`DmeshTarget`의 `tls::ConditionalServerTls`).

## inbound: 목적지 pod의 정책을 요청마다 적용한다

- **목적지별 정책 store.** `DMESH_INBOUND_WORKLOADS`가 가리키는 파일은 줄마다
  `<목적지 IPv4> <policy workload>`이다. workload는 policy controller가 받는 형식
  (`{"ns":"<namespace>","pod":"<pod>"}`)이다. flow의 목적지 IP가 표에 있으면 그 workload의
  store에서, 없으면 proxy 자신의 workload(`LINKERD2_PROXY_POLICY_WORKLOAD`)의 store에서
  `(목적지 port)`의 정책을 본다. store는 workload마다 하나이고 처음 쓰일 때 만든다
  (`linkerd/app/src/lib.rs`).
- **연결 단위 gate.** flow를 받을 때 `dmesh_connection_authorized`가 정책의 authorization 중
  하나라도 이 client를 허용하는지 본다. 거부면 연결을 닫는다. protocol을 감지하는 server
  (`Server`에 `proxyProtocol`이 없는 경우)는 TCP authorization이나 HTTP route의 authorization
  중 하나로 통과한다: route에만 `AuthorizationPolicy`를 건 server는 TCP authorization이 비어 있다.
- **요청 단위 정책.** 연결의 HTTP service를 `FusedHttpPolicy`가 감싼다
  (`linkerd/app/inbound/src/policy/http.rs`). 요청마다 route를 찾고, route의 authorization과
  filter, server의 local rate limit을 적용한다. 거부는 inbound proxy와 같은 응답이다
  (403 / gRPC 7, route 없음 404 / 5, rate limit 429 / 8). HTTP route가 없는 server(opaque, TLS)는
  요청을 그대로 넘긴다.
- **access log.** 같은 자리에서 stock `NewAccessLog`가 요청마다 한 줄을 쓴다
  (`LINKERD2_PROXY_ACCESS_LOG`).
- **끼우는 방법.** `ServeHttp`가 연결의 HTTP service를 만들 때 그 연결의 task에 걸린 wrapper를
  집어 쓴다(`linkerd/proxy/http/src/conn_wrap.rs`, task-local). `dmesh::serve`가 flow마다
  `conn_wrap::scope`로 wrapper를 건다. TCP로 들어온 연결에는 wrapper가 없어 동작이 같다.

## pod와 Service가 갖출 것

- pod label `linkerd.io/control-plane-ns: linkerd`: policy controller는 이 label이 있는 pod만 안다.
- pod annotation `config.linkerd.io/skip-inbound-ports: <port>`: discovery가 그 port에 pod 안의
  proxy가 있다고 알리지 않는다.
- server pod는 `<pod IP>:<port>`에 listen한다. `DPUMESH_PORT`만 주면 host 라이브러리가 pod IP를
  찾아 그 주소를 쓴다(`src/core/carrier.c`).
- `DPUMESH_WORKLOAD`는 `MeshTLSAuthentication`이 맞춰 볼 identity
  (`<sa>.<ns>.serviceaccount.identity.linkerd.cluster.local`)다.
- `LINKERD2_PROXY_POLICY_WORKLOAD`의 pod도 label이 있어야 한다. proxy가 자기 inbound port의
  정책을 이 workload로 묻는다.

## 한계

- 목적지 표는 proxy가 시작할 때 한 번 읽는다. pod 구성이 바뀌면 표를 다시 쓰고 proxy를 다시 띄운다.
- protocol을 감지하는 server에서 HTTP가 아닌 flow는 HTTP route의 authorization만으로 연결 gate를
  통과하고, 요청 단위 정책은 걸리지 않는다. 그런 server에는 TCP authorization을 함께 둔다.
- `opentelemetry_span_exports` 계열 카운터는 이 proxy 버전에서 늘지 않는다. span 전송은
  `control_response_total{addr="<collector>"}`로 본다.
- control plane에 닿는 경로는 배포가 정한다. 시험 장비에서는 DPU의 loopback port를 host에서
  `ssh -R`로 넘겼다(mesh-bench `dpumesh/dpu-jet2-cp/cp-setup.sh`).

## 확인

2026-11-10–11, Linkerd edge-26.8.4, DPU worker 15개, 250 req/s. 구성마다 정책이 control plane과
proxy metric 양쪽에 보이는지 읽고, 요청 실패가 없는지 봤다.

| 정책 | 확인한 것 |
|---|---|
| authorization (`Server` + route `AuthorizationPolicy` + `MeshTLSAuthentication`) | 허용 identity를 다른 것으로 바꾸면 18건 중 18건 거부, 되돌리면 0건 |
| local rate limit | currency에 1 req/s를 걸면 30건 중 30건 거절, 되돌리면 0건 |
| HTTPRoute regex, regex 50개, header 추가, 90/10 split | route가 Accepted이고 proxy가 그 route로 요청을 처리 |
| retry + timeout, circuit breaking | Service annotation이 outbound 정책에 반영 |
| access log | 요청마다 한 줄 (30 s에 약 76,000줄) |
| tracing 1% | collector로 보낸 export 호출이 성공 |
| 전부 함께 | 위가 모두 성립하고 실패 0 |
