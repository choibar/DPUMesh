# Online Boutique end-to-end — 2026-09-29

2026-09-30 수정·재측정은 이 문서 아래에 추가한다. 본문 원측정은 수정 전 결과다.

판정: **Online Boutique v0.10.7의 서비스 간 gRPC 14개 경로가 두 가지 방식 모두에서 DPUMesh와 DPU의 linkerd2-proxy를 거쳐 동작했다.**

1. **preload 모드:** Go가 아닌 서비스 6개(Node.js, Python, Java, C#)를 코드 수정 없이 `LD_PRELOAD` shim으로 붙였다. Proxy가 HTTP/2를 종단하는 L7 모드에서 smoke 3회의 gRPC 요청 172개가 모두 성공했고, DOCA 에러는 0건이었다.
2. **native 모드:** 같은 6개 서비스가 각 언어의 DPUMesh gRPC library로 붙었다. 결과는 같았다(요청 172개 모두 성공, DOCA 에러 0).
   - Library: `.NET` Kestrel transport, `Node.js` grpc-js injector, `Java` grpc-netty channel과 provider, `Python` DPUMesh grpcio.
3. **Go 서비스:** Go 서비스 4개는 두 모드 모두 `dmeshgo`를 쓴다. Go runtime은 libc를 거치지 않고 socket syscall을 직접 부르므로 preload가 닿지 않는다.

- **성능:** DPUMesh 없는 두 기준선(kernel TCP, 호스트의 linkerd2-proxy)과 같은 조건에서 비교했다.
  - DPUMesh는 두 기준선보다 느렸다. 사용자 8명에서 처리량은 호스트 proxy의 약 1/3이었다(184–190 대 582–588 req/s).
  - RPC 하나의 왕복은 호스트 proxy보다 약 1 ms 길었다(Go 서비스 p50 1.0–1.2 ms 대 0.16–0.18 ms).
  - 페이지당 호스트 CPU는 호스트 proxy의 2.6–4.4배였다(20.7–35.2 ms 대 7.6–8.0 ms).
  - preload와 native는 모든 측정에서 같았다. 두 모드 모두 DPU 경로의 지연에서 막힌다.
  - 이전 판의 Locust 결과는 부하 도구가 GET 페이지마다 약 40 ms를 더해 무효로 하고 다시 쟀다(아래 "측정 방법").
  - 두 날에 걸쳐 두 번 쟀고 값이 맞았다. 스크립트와 원자료로 다시 잴 수 있다(아래 "재현").
- **DPU 쪽 문제:** 지속 부하에서 DPU의 DPA process가 멈춘다. 수정한 Locust 부하를 건 DPUMesh 실행 6회 모두 크래시가 났다.

## 구성

| 서비스 | 언어, gRPC library | 역할 | preload 모드 | native 모드 |
|---|---|---|---|---|
| frontend | Go | client (서비스 7개) | `dmeshgo` | `dmeshgo` |
| checkoutservice | Go | server, client (서비스 6개) | `dmeshgo` | `dmeshgo` |
| productcatalogservice | Go | server | `dmeshgo` | `dmeshgo` |
| shippingservice | Go | server | `dmeshgo` | `dmeshgo` |
| currencyservice | Node.js 24, `@grpc/grpc-js` 1.14.4 | server | preload | `@dpumesh/grpc-js` |
| paymentservice | Node.js 24, `@grpc/grpc-js` 1.14.4 | server | preload | `@dpumesh/grpc-js` |
| cartservice | C# (.NET 10), `Grpc.AspNetCore` 2.83.0 | server | preload | `Dpumesh.Grpc` |
| adservice | Java 21, grpc-java 1.84.0 (Netty) | server | preload | `dpumesh-grpc.jar` |
| emailservice | Python 3.10, `grpcio` 1.80.0 | server | preload | `dpumesh_grpc` |
| recommendationservice | Python 3.10, `grpcio` 1.80.0 | server, client (productcatalog) | preload | `dpumesh_grpc` |

- **주소:** 각 서비스는 DPUMesh pod 하나다(`DPUMESH_POD_IP` 10.99.0.11–20). Service target 10.99.1.1–9를 서빙한다. 브라우저에서 frontend로 가는 HTTP와 Redis 연결만 kernel TCP를 쓴다.
- **서비스 코드:** 연결 코드는 fork [jukebox03/microservices-demo](https://github.com/jukebox03/microservices-demo)의 `dpumesh` branch에 있다. 모두 `DPUMESH_ENABLE=1`일 때만 켜지고, 꺼져 있으면 upstream과 같게 동작한다.
- **preload 모드의 비활성 코드:** preload 모드에서도 이 연결 코드는 빌드에 들어 있지만 비활성이다. 각 process가 실제로 올린 라이브러리(`/proc/<pid>/maps`)를 기록해 확인했다.
  - preload 모드: 서비스 6개가 `libdpumesh_preload.so`를 올렸고, `libdpumesh_stream.so`를 올린 서비스는 없었다.
  - native 모드: 6개 모두 `libdpumesh_stream.so`와 각자의 binding을 올렸고, preload shim을 올린 서비스는 없었다. binding은 JNI 라이브러리, `Dpumesh.Grpc.dll`, `dpumesh_grpc.node`, DPUMesh grpcio다.
- **grpcio 버전:** Python 서비스는 native 모드를 위해 `grpcio`를 1.76.0에서 1.80.0으로 올렸다. 처음 한 preload 시험(아래 "처음 실행")만 upstream의 1.76.0을 썼다.

## 기능 검증 (장비)

Smoke는 home, product, 장바구니 담기, cart, checkout을 차례로 요청한다. 모든 실행은 proxy와 mock을 새로 띄웠다. 성공 판정은 proxy admin `/metrics`로 했다.

| 실행 | Proxy 모드 | 결과 |
|---|---|---|
| 처음 실행 (upstream clone + patch, preload) | L4 (`MOCK_OUTBOUND_OPAQUE=1`) | smoke PASS. client flow 14개가 모두 linkerd outbound stack을 거쳤다(`proxy{addr}:forward` → `Connecting via dmesh DMA backend channel`). |
| 처음 실행, preload | L7 | smoke 3/3. `http/2` 판별 14/14, route 요청 172개 모두 HTTP 200·`grpc_status=0`. DOCA 에러 0. |
| fork, preload | L7 | 위와 같음(요청 172개 모두 성공, DOCA 에러 0) |
| fork, cart만 native | L7 | 같음. Kestrel `Now listening on: http://dpumesh://10.99.1.3:7070` |
| fork, cart·currency·payment native | L7 | 같음 |
| fork, cart·currency·payment·ad native | L7 | 같음 |
| fork, 전체 native | L7 | 같음 |

- **L7 실행의 서비스별 요청:** 모든 L7 실행에서 같았다.

  | 서비스 | 요청 수 |
  |---|---|
  | productcatalog | 69 |
  | currency | 54 |
  | cart | 18 |
  | recommendation | 9 |
  | shipping | 9 |
  | checkout, email, payment | 각 3 |
  | ad | 요청 5, 응답 4 |

- **ad 호출의 deadline 초과:** frontend가 ad 호출에 거는 100 ms deadline(`src/frontend/rpc.go:120`)을 처음 연결할 때 넘긴 결과다. 페이지는 200이었다.
- **mTLS 없음:** mock control plane이 identity를 주지 않는다(`tls="no_identity"`).
- **종료 시 에러:** 처음 L4 실행만 서비스 종료 시 DOCA 에러 4건을 남겼다. 프로세스가 channel을 닫지 않고 끝날 때의 기존 증상이다. native adapter는 process 종료 때 runtime을 닫고, Node.js와 Python은 SIGTERM에서도 닫는다.

### Adapter 단위 시험 (장비 없음)

각 adapter는 in-process loopback(`libdpumesh_stream_loopback.so`) 위에서 실제 gRPC server와 client로 시험했다.

- **.NET:** 19개.
- **Node.js:** 7개.
- **Java:** 7개.
- **Python:** 6개.
- **C++:** CTest 6개. `dpumesh_stream_test` 16개 항목, loopback 1 MiB echo, endpoint 시험을 포함한다.
- **Go:** `dmeshgrpc` 3개.
- **mutation 확인:** 수신 credit을 잡았다가 돌려주는 경로는 .NET, Node.js, Java 모두 resume 호출을 뺀 mutant에서 시험이 실패함을 확인했다.

### Stream C ABI와 C++ gRPC (장비)

`dpumesh_stream_smoke`로 확인했다. 대상 flow는 10.99.0.51에서 10.99.1.50:9000이며 L4 모드다. 모든 바이트를 검증했다.

| 메시지 | 횟수 | 왕복 p50 | p99 |
|---|---|---|---|
| 64 B | 2000 | 38.7 µs | 121.6 µs |
| 8 KiB | 1000 | 56.2 µs | 574.8 µs |
| 64 KiB | 200 | 178.2 µs | 4.66 ms |
| 1 MiB | 20 | 2.44 ms | 173.6 ms |

- **첫 메시지 지연:** 크기마다 첫 메시지는 약 170 ms였다. 서버 쪽 backend flow를 여는 시간이다.
- **C++ gRPC smoke:** reactor 분리 전과 후를 번갈아 2회씩 돌렸고, 200 call이 모두 통과했다.
- **C++ gRPC 서버 종료 시 에러:** 서버가 끝날 때 DOCA 에러 1–2건(`0x15`)이 남았다. 분리 전 빌드에서도 같아서 기존 문제로 본다.

## 성능

### 측정 방법

- **부하 도구 수정:** upstream Locust(`FastHttpUser`, geventhttpclient 2.4.0)는 본문 없는 GET에도 `Transfer-Encoding: chunked`를 붙인다.
  - 헤더와 빈 종료 chunk를 두 번에 나눠 쓰는데, Nagle 때문에 두 번째 쓰기가 frontend의 delayed ACK(40 ms)를 기다린다.
  - Go HTTP 서버는 응답을 쓰기 전에 남은 요청 본문을 읽으므로, GET 페이지마다 약 40 ms가 더해진다.
  - 재현: 같은 요청을 한 번에 쓰거나 `TCP_NODELAY`를 켜면 상품 페이지가 44 ms에서 15–19 ms로 줄었다.
  - Locust client 소켓에 `TCP_NODELAY`를 켜자 kernel TCP, 사용자 8명에서 174 req/s(p50 44 ms)가 622 req/s(12 ms)가 됐다.
  - 이전 판의 Locust 결과(preload 369·368, native 372·377 req/s, p50 약 70 ms)는 이 지연이 페이지 시간의 대부분이었으므로 모드 비교에 쓰지 않는다.
- **구성 네 가지:**

  | 구성 | 서비스 간 gRPC | proxy |
  |---|---|---|
  | tcp | kernel TCP, 서비스 포트로 직접 | 없음 |
  | 호스트 proxy | kernel TCP, iptables REDIRECT로 호스트 proxy 경유 | linkerd2-proxy(`e4e93c24`, `doca` 없이 빌드), worker 1개, L7 |
  | preload | DPUMesh, Go가 아닌 서비스 6개는 preload shim | DPU proxy, L7 |
  | native | DPUMesh, 6개 모두 언어별 library | DPU proxy, L7 |

  - **호스트 proxy:** DPU proxy와 같은 mock control plane(`MOCK_POLICY_ECHO_TARGET=1`)과 같은 profile network를 쓰고, 원래 목적지로 전달한다. DPU 경로처럼 RPC 하나가 proxy를 한 번 지난다.
  - **namespace:** 호스트 쪽 전부(서비스, Redis, Locust, health-bench, 호스트 proxy)를 rootless network namespace 하나에서 돌렸다. loopback에 10.99.1.1–9를 두어 네 구성의 주소와 kernel 경로를 같게 했다.
- **부하:**
  - Locust closed loop(대기 없음), 4 process, 사용자 8·32·128명 각 40초.
  - health-bench(gRPC health `Check`), 요청 1개씩 대상마다 4–8초.
- **CPU:** 각 구간 앞뒤로 모든 process의 thread별 CPU 시간과 context switch를 `/proc`에서 읽었다.
- **반복:** 두 번 쟀다.
  - 1회차(09-29 밤)는 스크립트의 이전 판으로 쟀다.
  - 2회차(09-30 오전)는 fork에 둔 `dpumesh/bench/matrix.sh 2`로 쟀다.
  - 표의 값은 "1회차 / 2회차"다.

### Online Boutique 전체 부하

frontend req/s(p50)이다. 호스트 CPU는 서비스 process 합이며, 호스트 proxy 구성은 proxy를 포함한다.

| 구성 | 8명 | 32명 | 128명 | 페이지당 호스트 CPU, 8명 | 32명 |
|---|---|---|---|---|---|
| tcp | 685 / 688 (11 ms) | 1,414 / 1,427 (21 / 20 ms) | 1,556 / 1,581 (87 / 86 ms) | 9.3 / 9.4 ms | 8.0 / 8.0 ms |
| 호스트 proxy | 582 / 588 (13 ms) | 621 / 615 (56 ms) | 605 / 611 (240 ms) | 8.0 / 8.1 ms | 7.6 / 8.0 ms |
| preload | 186 / 187 (39 / 38 ms) | 380 / 387* (75 / 74 ms) | DPA 크래시 / DPA 크래시 | 33.7 / 33.6 ms | 22.0 ms / – |
| native | 184 / 188 (39 ms) | 382 / 382 (75 / 76 ms) | DPA 크래시 / 507 (230 ms) | 35.2 / 34.9 ms | 20.7 / 22.3 ms |

- **도중에 끊긴 구간:** \*는 DPA 크래시로 도중에 끊긴 구간이다. req/s는 요청이 오간 동안의 값이고, CPU는 쓰지 않는다.
- **추가 반복:** 1회차에는 사용자 목록을 비우려던 실행이 스크립트 버그로 Locust를 한 번 더 돌았다(`preload-r1b`, `native-r1b`). 8명 190·184, 32명 386·378 req/s로 같았다.
- **호스트 proxy 자체 CPU:** 0.46–0.49 core, proxy를 지난 RPC 하나당 약 59 µs였다.
- **DPUMesh의 idle 비용:** 요청이 없어도 DPUMesh process마다 약 0.1 core와 초당 약 2만 번의 context switch가 든다(host library의 50 µs tick). 10개 process를 합치면 약 1 core다.

### RPC 하나의 왕복 (요청 1개씩)

health-bench가 호스트에서 각 서비스의 health `Check`를 요청 1개씩 불렀다. 값은 p50 µs(1회차 / 2회차)이고, p99는 CSV에 있다.

| 서비스 | tcp | 호스트 proxy | preload | native |
|---|---|---|---|---|
| currency (Node) | 460 / 496 | 155 / 148 | 1,328 / 1,223 | 1,386 / 1,660 |
| payment (Node) | 409 / 464 | 164 / 176 | 1,610 / 1,151 | 1,310 / 1,335 |
| cart (.NET) | 100 / 432 | 329 / 278 | 1,428 / 1,146 | 1,319 / 1,010 |
| ad (Java) | 222 / 283 | 154 / 158 | 1,620 / 1,228 | 2,062 / 1,975 |
| email (Python) | 908 / 1,078 | 1,260 / 1,524 | 1,830 / 2,134 | 1,711 / 1,336 |
| recommendation (Python) | 884 / 811 | 1,743 / 1,689 | 1,876 / 1,665 | 1,588 / 1,547 |
| productcatalog (Go) | 347 / 420 | 162 / 168 | 1,202 / 1,162 | 1,163 / 1,012 |
| shipping (Go) | 369 / 344 | 159 / 160 | 1,068 / 1,092 | 1,107 / 1,041 |
| checkout (Go) | 360 / 378 | 178 / 181 | 1,152 / 1,113 | 1,233 / 1,082 |

- **DPUMesh 측정 방식:** DPUMesh 두 모드의 값은 health-bench만 돈 실행(`m1-*`, 대상마다 4초)에서 얻었다. Locust 뒤에 이어 돈 health-bench는 DPA 크래시로 끊기곤 했다.
- **DPU 경로의 추가 지연:** Go 서비스는 두 DPUMesh 모드에서 코드가 같고, 호스트 proxy보다 약 0.9–1 ms 느렸다. 이 몫이 DPU 경로(DMA 두 번 왕복과 DPU proxy의 L7)다.
- **편차:** 회차 사이 편차는 대체로 20% 안이었고, cart(.NET)의 tcp 값(100 / 432 µs)만 크게 달랐다.
- **tcp가 호스트 proxy보다 느린 서비스:** 이 host에서 반복해 관측됐고 원인은 보지 않았다. 두 기준선 모두 DPUMesh보다는 크게 빠르다.

### 병목

| 구성 | 처리량이 멈춘 곳 | 근거 | CPU 병목인가 |
|---|---|---|---|
| tcp | 약 1,550 req/s | currency(Node)의 JavaScript thread가 0.85 core. 호스트 전체는 36 core 중 11.5 core | 한 thread의 CPU. 전체 CPU는 여유 |
| 호스트 proxy | 약 600 req/s (8명부터) | proxy가 잰 recommendation 지연만 p50 10 → 100 → 300 ms, 나머지는 1 ms 이하. recommendation의 호출당 CPU는 tcp의 2.3배(1.2 → 2.7 ms) | Python GIL로 한 core에 묶인 CPU. proxy는 0.48 core로 여유(health 부하에서는 초당 3만 RPC까지 처리) |
| preload, native | 32명에서 약 380 req/s, 128명에서 DPA 크래시 | 모든 서비스의 proxy 측 지연이 8명 → 32명에서 함께 1 → 3 ms로 늘었다. 부하가 없는 payment, email도 같이 늘었다 | CPU가 아니라 DPU 경로의 지연 |

- **DPUMesh의 지연 구조:** 페이지 하나가 RPC 약 13개를 차례로 부르므로 hop 지연이 그대로 페이지 지연이 된다. closed loop에서 처리량은 사용자 수를 페이지 지연으로 나눈 값이다.
- **DPU proxy shard의 CPU:** busy poll을 끄고 재면 shard thread가 사용자 8·32·64명에서 0.83–0.86 core로 거의 같았다(두 회차 모두).
  - 그때 frontend는 139 / 142, 255 / 254, 355 / 353 req/s였다.
  - 처리한 RPC가 초당 1.9천에서 4.8천으로 늘어도 CPU가 그대로였으므로, 부하에 비례한 CPU 포화는 아니다.
  - 그동안 proxy 측 지연은 2 → 5 → 10 ms로 늘었다. 단일 event loop에서 줄을 서는 쪽이 유력하지만, DPU에서 profiling(root 필요)을 하지 않아 확정하지 않았다.
- **DPUMesh의 호스트 CPU:** 호출당 CPU가 tcp보다 컸다.
  - 32명 기준 currency(Node) 약 5배, productcatalog(Go) 약 2.5배.
  - currency의 JavaScript thread는 32명에서 0.83 core로 포화에 가까웠다.

### preload와 native가 같은 이유

- **공통 경로가 병목이다.** RPC마다 DPU 경로가 약 1 ms를 더한다. 호스트 쪽 전송 계층의 차이(preload의 socketpair 신호와 system call, native의 runtime 전달)는 여기에 묻힌다.
- **절반 이상이 같은 코드다.** RPC의 절반 이상을 받는 Go 서비스(productcatalog 한 곳이 49%, shipping, checkout)와 유일한 외부 client인 frontend는 두 모드 모두 `dmeshgo`를 쓴다.
- **구조가 같다.** 두 방식 모두 DPUMesh thread가 EQ를 읽고 앱의 I/O thread를 깨우며, 수신과 송신에서 각각 한 번씩 복사한다.
- **이전 측정은 모드 차이를 볼 수 없었다.** 이전 판의 Locust 결과는 부하 도구의 40 ms가 페이지 시간의 대부분이었다. health 부하(아래)는 서비스 자체의 처리 한계에서 막혔다.

### 이전 측정: 서비스별 health RPC 부하 (L4)

`health-bench`로 각 서비스의 표준 `grpc.health.v1.Health/Check`를 직접 불렀다.
- **조건:** client 1개, 동시 64개, 대상마다 3초 warm-up 뒤 15초. proxy 해석 비용을 빼려고 L4 모드로 돌렸다.
- **연결:** 모든 연결을 먼저 열어 두고 대상을 차례로 쟀다.
- **CPU:** 측정 구간의 서비스 process CPU 시간을 호출 수로 나눴다.

값은 preload와 native를 번갈아 돈 두 회차(1회차 / 2회차)다.

| 서비스 | preload calls/s | native calls/s | preload p50 µs | native p50 µs | preload CPU µs/call | native CPU µs/call |
|---|---|---|---|---|---|---|
| currency (Node) | 24782 / 25220 | 22788 / 21049 | 2324 / 2227 | 2504 / 2841 | 84 / 84 | 88 / 89 |
| payment (Node) | 28488 / 25697 | 19169 / 23630 | 1960 / 2133 | 2857 / 2522 | 71 / 86 | 111 / 79 |
| cart (.NET) | 75992 / 87743 | 88204 / 63227 | 730 / 582 | 569 / 814 | 95 / 102 | 99 / 87 |
| ad (Java) | 43129 / 52434 | 49644 / 41493 | 1377 / 1132 | 1138 / 1394 | 61 / 65 | 53 / 78 |
| email (Python) | 3894 / 3432 | 3880 / 4061 | 16273 / 17346 | 15952 / 15631 | 719 / 706 | 584 / 553 |
| recommendation (Python) | 3605 / 3734 | 3862 / 2920 | 17152 / 16564 | 16346 / 18284 | 765 / 747 | 585 / 758 |
| productcatalog (Go, 대조군) | 51861 / 47351 | 46646 / 51398 | 1017 / 1116 | 1129 / 1020 | 75 / 118 | 119 / 69 |
| shipping (Go, 대조군) | 48140 / 48047 | 47178 / 49931 | 1100 / 1097 | 1112 / 1039 | 105 / 108 | 110 / 71 |
| checkout (Go, 대조군) | 42416 | 44519 / 43907 | 1229 | 1066 / 1240 | 138 | 93 / 96 |

- **편차:** 두 모드에서 코드가 같은 Go 서비스도 처리량이 ±10%, 호출당 CPU가 69–138 µs로 흔들렸다. 그보다 작은 차이는 판단하지 않는다.
- **Node.js:** native가 두 서비스 모두 처리량이 낮았다(currency −11%, payment −18%). Node의 HTTP/2는 `net.Socket`이 아닌 stream을 JavaScript stream wrapper로 감싸는데, 그 비용으로 본다.
- **Python:** native의 호출당 CPU가 낮은 경향이 있었다(−6~−20%). 처리량은 grpcio 동기 서버의 worker 10개와 GIL에서 막혀 차이가 없었다.
- **.NET, Java:** 편차 안이었다.
- **빠진 값:** preload 2회차의 checkout은 아래 DPA 크래시로 값이 없다. 같은 이유로 연결을 대상마다 새로 연 초기 1회차(preload와 native 각 1회)는 도중에 끊겨 표에서 뺐다.

## DPU 쪽 관측: 지속 부하에서 DPA process 크래시

health RPC 부하 6회 가운데 4회에서 proxy가 멈췄다. 로그는 다음과 같았다.
- `flexio_crash_data`
- `DPA context reported a device-side error; its threads no longer run and new flows fail until the process restarts`

그 뒤 새 flow는 `Failed to start producer completion - DOCA_ERROR_DRIVER`로 실패했다.

- preload와 native 모두에서 나왔다. host adapter와는 무관하다.
- 크래시까지의 누적 처리량과 시각은 실행마다 달랐다(약 126만과 222만 메시지, 110초와 172초). 정해진 개수에서 넘치는 counter는 아니다.
- 네 번 중 세 번은 부하를 많이 받은 client flow가 닫히고 약 8초 뒤였다. 한 번은 flow를 닫지 않은 부하 도중이었다. 원인은 조사하지 않았다.
- 이전 판의 Online Boutique 부하(부하 도구 문제로 서비스당 수천 RPC/s 이하)에서는 나오지 않았다.

수정한 Locust 부하에서는 더 자주 났다.
- **Locust 실행:** Locust를 돈 DPUMesh 실행 6회(두 회차의 preload와 native, 1회차 추가 반복 포함) 중 5회가 Locust 도중 크래시로 끊겼다. 32명 구간에서 3회, 128명 구간에서 2회였다.
  - 128명을 버틴 1회(native 2회차)도 뒤이은 health-bench에서 크래시가 났다.
- **health-bench만 돈 실행:** 1회차 밤에 두 번 크래시가 났다(초당 1천 RPC 미만). 같은 조건의 다른 네 번(`m1-*`)은 끝까지 갔다.
- **33번째 flow:** 09-29–30에 관측한 크래시 12회 중 3회는 DPA pool thread 32번(33번째 동시 flow)을 배정하고 0.83초 뒤에 났다.
  - 32번 thread는 이 branch에서 pool을 32개에서 64개로 늘리면서 처음 쓰이게 됐다(`f79dab8`).
  - 32–40번 thread를 배정하고 100초 넘게 버틴 실행도 있어서, 33번째 flow가 늘 크래시를 내는 것은 아니다.
- **나머지 9회:** 마지막 flow를 연 뒤 25–172초가 지나, 부하 중에 났다.
- **원인 분석이 막힌 곳:** DPA core dump는 만들어지지 않았다(`Failed to create PRM process core dump`, syndrome `0x65e8c0`). `dpa-ps`는 root가 필요하다.
- **코드에서 본 의심 지점:** forward ring 경로(`poll_desc_ring`)는 producer completion queue(512개)를 읽지 않는다. `OPTIMIZE_REPORTS`는 completion을 늦출 뿐 없애지 않는다. 확인하지 않은 가설이다.

## 재현

- **스크립트:** fork [jukebox03/microservices-demo](https://github.com/jukebox03/microservices-demo) branch `dpumesh`의 `dpumesh/bench/`. 준비와 실행은 그 README에 있다. 담긴 것:
  - `setup.sh`: Locust(버전 고정), health-bench, 호스트 proxy와 mock, Redis.
  - `perf.sh`: 한 구성의 측정.
  - `matrix.sh`: 이 기록의 실행 묶음.
  - `summarize.py`: CSV 생성.
  - `dpu/`: DPU proxy의 빌드, 환경, 시작, 정지.
- **대상 버전:** 이 기록과 같은 커밋 묶음의 DPUMesh(linkerd2-proxy `e4e93c24`)와 그 fork branch. DPU에는 같은 DPUMesh checkout을 두고 `ob-bench/build.sh`로 빌드한다.
- **명령:**

  ```sh
  export DPUMESH_ROOT=<DPUMesh checkout>
  dpumesh/setup.sh && dpumesh/bench/setup.sh        # fork checkout에서
  ssh 192.168.100.2 bash DPUMesh-online-boutique/ob-bench/build.sh
  dpumesh/bench/matrix.sh 3                          # 약 45분, results/matrix-r3.csv
  ```

- **원자료:** [2026-09-29_online-boutique-e2e.csv](2026-09-29_online-boutique-e2e.csv). 두 회차의 모든 구간이 한 행씩 들어 있다.
  - Locust 구간: `kind=locust`.
  - health-bench 대상: `kind=m1`, `kind=m64`.
  - DPA 크래시로 비거나 끊긴 구간은 `note`에 적었다.
- **표와 실행의 대응:**
  - 전체 부하: `{tcp,hostproxy,preload,native}-r*`.
  - 요청 1개 왕복: tcp와 호스트 proxy는 같은 실행, DPUMesh는 `m1-*-r*`.
  - busy poll을 끈 측정: `bp0-preload-r*`.
- **1회차의 스크립트 버그:** 커밋한 판에서는 둘 다 고쳤다.
  - DPUMesh 모드의 health-bench 요약에 첫 대상만 남았다. ssh가 표준입력을 읽었기 때문이다. 원본 출력은 온전했고, CSV는 원본을 읽는다.
  - 사용자 목록을 비운 실행이 기본값으로 Locust를 돌았다.

## 측정 조건

- **장비:** rapids4 host, BF-3 DPU(DOCA 3.1.0105). PCI는 `03:00.0`/`94:00.0`, reverse는 `dpu-dma`.
- **Proxy 설정:** `LINKERD2_PROXY_CORES=1`, `DMESH_NUM_WORKERS=1`, `DMESH_SHARDED=1`, `DMESH_BUSY_POLL=1`, `LINKERD2_PROXY_DESTINATION_PROFILE_NETWORKS=127.0.0.0/8,10.99.0.0/16`.
  - Mock은 `MOCK_POLICY_ECHO_TARGET=1`로 띄웠고, L4 실행에만 `MOCK_OUTBOUND_OPAQUE=1`을 더했다.
- **기능 검증과 L4 health 측정의 DPUMesh:** `feature/online-boutique`(`cb9147b`)에 아직 커밋하지 않은 변경을 더한 상태다. 네 구성 비교는 `85aaad4`에 health-bench의 TCP 모드만 더했다.
  - preload IPv6 지원.
  - DPU worker flow 한도 64.
  - C++ reactor 분리와 stream C ABI.
  - 언어별 adapter.
- **Proxy:** `c09915bf`에 `MAX_CONNS = 64`를 더했다.
- **Online Boutique:** fork의 `dpumesh` branch(`v0.10.7` 기준)를 `dpumesh/setup.sh`로 빌드하고 `dpumesh/run.sh`(`DPUMESH_MODE=preload|native`)로 실행했다.
- **네 구성 비교:**
  - Locust 2.43.0(4 process, client 소켓 `TCP_NODELAY`).
  - Redis 7(Alpine 이미지의 binary를 namespace 안에서 직접 실행).
  - 호스트 proxy는 core 34–35, mock은 core 33에 고정했다. DPU proxy 설정은 위와 같다.
  - DPU proxy의 busy poll을 끈 실행은 병목 분석에만 썼다.
- **Runtime:** Go 1.27.1, Node.js 24.13.0, Python 3.10.12, OpenJDK 21.0.12, ASP.NET Core 10.0.12, gRPC C++ 1.80.0 source.
- **보존 자료:** 실행 script와 로그는 host의 `build/e2e/`에 로컬로만 보존한다.
  - Script: `ob-cycle.sh`, `ob-load.sh`, `ob-health.sh`, `stream-hw.sh`, `grpc-ab.sh`, 그리고 네 구성 비교 1회차에 쓴 `perf-run*.sh`와 `probe/`.
  - 로그: `ob-*`, `load-*`, `health-*`, `perf-*`(1회차), `stream-s1`, `grpc-ab`, `online-boutique-2026-09-29`.
  - 2회차 로그는 fork checkout의 `dpumesh/.build/bench/results/`에 있다.


## 2026-09-30: host CPU 수정과 실장비 재검증

아래의 host EQ spin window와 50 µs fallback tick은 같은 날 [host idle wake](2026-09-30_host-idle-wake.md)로 대체됐다.

최종 native에서 **host CPU는 약 47% 감소했지만, 처리량과 지연은 원래 수준이다.**
따라서 같은 node의 host proxy와 성능이 같다는 주장은 이 Online Boutique 구현과
측정 조건에서는 성립하지 않는다. 아래 결과는 수정 전 본문의 기록과 별도다.

### 코드에서 확인하고 고친 부분

- Host EQ의 기본 spin window 1 ms를 0으로 바꿨다. ACK까지 window를 다시
  시작시키므로 ACK 간격이 1 ms보다 짧으면 FD가 계속 readable이고 각 언어의
  reactor가 잠들지 못했다. 명시적인 `DPUMESH_SPIN_US=1000` 설정은 유지한다.
  Doorbell 없는 ACK·push·control 처리를 위한 50 µs fallback timer도 유지한다.
- 공유 Comch session PE를 flow마다 진행하던 일을 drain pass마다 한 번으로
  줄였다. Flow별 오류 확인과 private reverse PE 진행은 계속 수행한다.
  Go·Node·Python·Java·.NET native adapter는 모두 이 공통 host core를 사용한다.
- Proxy의 reader watermark를 64 KiB 단위로 갱신한다. 작은 read마다 synchronous
  `doca_dpa_h2d_memcpy`를 호출하던 비용을 줄이고, 실패하면 이전 watermark를
  유지해 다음 tick에서 재시도한다. 이 구현은 `~/DPUMesh`에 있는 batching을
  가져왔으며 staging gate의 여유 공간을 compile-time assertion으로 검사한다.
- DPU push 경로가 DMA **제출**을 Rust staging의 반환 가능한 byte 수로 보고해
  writer가 아직 DMA 중인 source를 덮어쓸 수 있었다. 기존 8 KiB 직렬 batch를
  유지하고 data·descriptor DMA가 완료된 뒤에만 credit을 반환하도록 고쳤다.
  Descriptor 제출 실패도 flow 오류로 처리한다. 새 DMA pipeline은 추가하지 않았다.
- Boutique forward DPA kernel에는 producer completion 회수가 없었고 모든 copy에
  `OPTIMIZE_REPORTS`를 붙였다. `~/DPUmesh/doca/device/dpa_kernel.c`와
  `doca/dpa_common.h`의 정책처럼 512개 제출마다 report를 요청하고 completion을
  회수·ack한다. Receive credit 확인과 producer report 회수는 서로 다른 일이다.
  이 수정 전 `after-final-r2`에서는 health 측정 중 DPA crash가 다시 발생했다.
  회수만 더한 `after-final-r4`도 64개 동시 health RPC 도중 크래시했다.
- 정상 DPA poll loop에는 reschedule이 전혀 없어 한 번 실행되면 종료 요청까지
  EU를 넘기지 않았다. SDK API를 DPU PF 03:00.0에 직접 조회한 실제 scheduled
  kernel 시간 한도는 **12초**였다(`dpa-capabilities.log`). 무한 실행은
  [NVIDIA DOCA DPA의 watchdog 제약](https://networking-docs.nvidia.com/doca/sdk/doca-dpa)에
  어긋난다. `~/DPUmesh`에 이미 있는 같은 EU의 helper와 notification을 통한
  주기적 handoff를 옮겼다. 65536 DMA 제출 또는 262144 idle spin 뒤 reschedule하고,
  descriptor head·DMA 제출 수·deferred report 수를 보존한다. Helper를 깨우는
  trigger completion을 먼저 해제하고 helper를 멈춘 다음 resume completion을
  해제한다. 정리 실패 때는 소유권을 유지해 재시도한다.
  이 변경은 DPU pool에 적용하며 host-dpa의 미검증 경로까지 확장하지 않는다.
- 같은 EU의 helper는 고정 affinity가 필요하다. 이 장비의 다른 PF 03:00.1에는
  별도 DPA process(PID 2436467)가 이미 실행 중이다. `DPUMESH_DPA_EU_BASE`로
  배치 범위를 선택하도록 하고, 이 장비의 Boutique profile에는 EU 64–127을
  사용한다. Transport의 범용 기본값은 0이다. EU affinity는 자원 예약이 아니다.
  기존 process는 종료하거나 변경하지 않았다. 그 process의 실제 affinity는
  root 소유라 확인하지 못했으므로, 경합은 배치 A/B 결과에 근거한 추론이다.
- DPA device log level은 `~/DPUmesh`처럼 ERROR로 둔다. Reschedule 때마다
  반복되는 device INFO 로그가 hot path 비용이 됐다. 같은 yield 구현의
  3 MiB stream 왕복 p50은 INFO에서 947 ms, ERROR에서 11.5 ms였다.
  Device 로그를 낮춰도 EU base 0의 Online Boutique 긴 지연은 남았다.
- 측정 script는 미기동 서비스·smoke 실패를 거부하고 자신이 시작한 process를
  정리한다. 마지막 health `MEASURE_END`도 수집한다. CPU 계산은 종료한 thread를
  포함한 process CPU 시간 / 실제 요청 수를 사용한다. TID와 thread 이름을 함께
  기록하므로 같은 `MainThread` 이름을 JavaScript main thread로 단정하지 않는다.

Busy TX tail 500 µs를 0으로 바꾸는 실험은 처리량·지연 개선이 확인되지 않아
제외했다. 이 knob도 최종 코드에 남기지 않았다.

### 같은 장비의 native 전후 비교

Host rapids4(Xeon Gold 6554S, 36 cores), BF-3 DPU, DOCA 3.1.0105에서
원래 host/DPU 바이너리와 수정본을 비교했다. 두 실행 모두 서비스 10개와 smoke가
정상이며 L7, reverse `dpu-dma`, worker 1개, busy poll 1, proxy log `info`,
Locust process 4개, level당 20초다. 기본 Rust proxy 로그 설정은 `warn`으로
낮췄지만 이 비교에서는 양쪽 모두 `info`를 사용했다. 최종 DPA device 로그는
ERROR, EU base는 64다. TX tail은 양쪽 모두 기존 500 µs다.

| Native 실행 | 사용자 | req/s | p50 ms | p99 ms | host cores | host CPU ms/page |
|---|---:|---:|---:|---:|---:|---:|
| 수정 전 `before-r4` | 8 | 187 | 38 | 74 | 6.16 | 38.7 |
| 최종 `after-eu64-r7` | 8 | 189 | 38 | 74 | 3.30 | 20.4 |
| 수정 전 `before-r4` | 32 | 385 | 75 | 140 | 8.11 | 24.6 |
| 최종 `after-eu64-r7` | 32 | 383 | 76 | 140 | 4.31 | 13.2 |
| 최종 `after-eu64-r7` | 128 | 536 | 220 | 390 | 4.49 | 9.8 |

Host CPU는 서비스 process의 합이다. Locust·Redis·mock은 제외한다.
Snapshot 구간은 Locust의 시작·종료도 포함하므로 `cores / req/s` 대신 CPU 시간 /
실제 요청 수를 사용했다. 이전 문서의 CPU/page 값과 계산 구간이 다르므로
직접 섞지 않는다. DPU proxy는 busy poll 때문에 두 실행 모두 약 0.97 core다.

같은 날 host proxy 기준선은 사용자 8명에서 561 req/s, p50 14 ms,
9.1 ms CPU/page, 사용자 32명에서 620 req/s, p50 56 ms, 8.5 ms CPU/page였다.
따라서 수정 후에도 native의 낮은 동시성 처리량은 host proxy의 약 1/3이고
페이지당 CPU는 약 2.2배다. CPU 절감과 end-to-end 성능 동등성을 구분해야 한다.
최종 Go health p50은 1.21–1.25 ms다. 기존의 DPU 왕복 지연이 남았으며,
native adapter를 사용한다고 이 공통 transport 비용이 없어지지는 않는다.

최종 preload도 같은 바이너리와 기본 EU base 64로 실행했다
(`after-preload-eu64-r9`). 8/32/128명 처리량은 187/388/530 req/s,
p50 39/75/220 ms, host CPU는 3.32/4.35/4.66 cores,
CPU/page는 20.8/13.2/10.3 ms였다. HTTP 실패는 세 구간 모두 0이다.
Native로 전환하는 것만으로 공통 경로의 병목을 해소하지 못한다는 결과도 같다.

Locust에서 수정 전 8/32명은 HTTP 실패 0/0, 최종 8/32/128명은 2/0/9건이다.
실패는 payment의 VISA_ELECTRON 거절에서 이어지는 checkout HTTP 500으로,
TCP와 host proxy 기준선에도 발생했다. 실패 수를 결과에서 제거하지 않았다.
Health RPC 오류와는 별도로 기록한다.

### Hardware 검사 결과와 범위

최종 바이너리와 EU base 64의 `hw-regression.sh`가 통과했다(`hw-eu64`).

- 실제 gRPC: sibling flow를 계속 사용하면서 40번 close/reopen,
  channel 재생성, 1·8064·8065·8192·8193·65537 B payload.
- 실제 native stream: 64·128·129·8064·8192·65536·1048576·3145728 B를
  byte마다 위치 의존 pattern으로 비교. 큰 메시지는 각각 8번 보내 staging을
  여러 번 wrap한다. 8192 B stream 4개를 동시에 실행한다.
- Server 종료가 10초 이내이며 stream server의 최종 live flow가 0이다.
  Proxy crash/panic report가 없다.
- 원본 바이너리도 강한 pattern으로 재검사했을 때 3 MiB 첫 메시지의 offset
  360448에서 불일치했다. 기존 256 B 반복 pattern은 replay를 놓칠 수 있어
  smoke 검사도 강화했다. 수정본은 같은 크기 8회와 동시 stream 검사를 통과했다.
- Yield 수정 전 `after-final-r3`는 8/32/128명 부하 후 서비스 9개의 health RPC를
  모두 완료했다. Health 오류 0, DPU ERR/crash/panic report 0, script exit 0이다.
  이후 64개 동시 RPC를 추가한 `after-final-r4`에서는 DPA crash가 재현됐다.
  두 실행은 yield 수정을 검증한 최종 결과로 취급하지 않는다.
- 최종 `after-eu64-r7`는 8/32/128명 부하와 서비스 9곳의 health RPC를
  1개 및 64개 동시성 모두에서 완료했다. Health 오류 0,
  DPU ERR/crash/panic report 0, script exit 0이다. 유한한 검사에서 크래시가
  없었다는 결과이며 장기 안정성을 보장하는 수치는 아니다.
- 최종 preload `after-preload-eu64-r9`도 같은 전체 검사를 통과했다.
  Native/preload의 64개 동시 health 검사에서 각각 687857/683076 call이
  성공했고 오류는 0이다. Preload에서도 DPU ERR/crash/panic report 0,
  script exit 0이다. 검사 종료 후 이번에 시작한 host/DPU process는 정리했으며
  다른 PF의 기존 process는 계속 실행 중이다.
- Yield + device ERROR + EU base 0의 `after-yield-quiet-r6`도 검사 오류와
  크래시는 없었지만, 처리량은 8/32/128명에서 45/113/35 req/s,
  p99는 2600/4000/8000 ms였다. EU base 64에서는 189/383/536 req/s,
  p99 74/140/390 ms로 돌아왔다. 이 진단 실행도 원자료와 CSV에 보존한다.
- 같은 최종 바이너리로 EU base만 0으로 되돌린 `placement-eu0-r8`에서도
  8명 처리량 4 req/s, p50 1500 ms, p99 5300 ms가 재현됐다.
  ERR/crash/panic은 0이다. 바이너리 차이가 아닌 EU 배치가 긴 지연을
  만드는 조건임을 확인했다. 기존 job과의 정확한 겹침은 확인하지 못했다.

Host C test 17개와 header/ABI 검사, Go race test, C++ ctest 6개,
DPU `dmesh-doca` Rust test 13개, CPU 계산 test 2개가 통과했다.
`host-dpa` reverse는 이 장비의 host PF 94:00.0에서 DPA process 생성 자체가
`DOCA_ERROR_DRIVER`(syndrome `0xb398a0`)로 실패해 검증하지 못했다.
위 데이터 무결성·성능 결과는 `dpu-dma`에 한정한다.

### 재현과 보존 자료

Fork checkout에서 다음과 같이 실행한다.

```sh
export DPUMESH_ROOT=/home/jukebox/DPUMesh-online-boutique
DPU_EU_BASE=64 bash dpumesh/bench/hw-regression.sh
DPUMESH_SPIN_US=0 DPU_PROXY_LOG=info DPU_EU_BASE=64 USERS_LIST='8 32 128' LDUR=20 \
  M1_WARM=1s M1_DUR=3s bash dpumesh/bench/perf.sh native verify
```

- [2026-09-30_online-boutique-fixes.csv](2026-09-30_online-boutique-fixes.csv):
  원본·최종 native/preload, 같은 날 TCP/host proxy 및 EU 배치 진단 측정치.
- [2026-09-30_online-boutique-fixes.json](2026-09-30_online-boutique-fixes.json):
  base commit, 실제 변경 source와 바이너리 SHA-256, 설정, 검사 및 제외한 실행의 이유.
- `build/hw-ab-2026-09-30/`: CPU snapshot, proxy/service/Locust log,
  baseline 3 MiB 실패, 최종 HW regression, build/test log를 로컬에 보존한다.
  이 디렉터리는 git ignored다. Host와 DPU의 변경된 transport/shim source hash도
  비교해 같음을 확인했다. 임시 작업 clone의 결과는 최종 비교에 사용하지 않았다.
