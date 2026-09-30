# Online Boutique end-to-end — 2026-09-29

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
