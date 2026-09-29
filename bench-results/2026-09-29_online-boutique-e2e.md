# Online Boutique end-to-end — 2026-09-29

판정: **Online Boutique v0.10.7의 서비스 간 gRPC 14개 경로가 두 가지 방식 모두에서 DPUMesh와 DPU의 linkerd2-proxy를 거쳐 동작했다.**

1. **preload 모드:** Go가 아닌 서비스 6개(Node.js, Python, Java, C#)를 코드 수정 없이 `LD_PRELOAD` shim으로 붙였다. Proxy가 HTTP/2를 종단하는 L7 모드에서 smoke 3회의 gRPC 요청 172개가 모두 성공했고, DOCA 에러는 0건이었다.
2. **native 모드:** 같은 6개 서비스가 각 언어의 DPUMesh gRPC library로 붙었다. 결과는 같았다(요청 172개 모두 성공, DOCA 에러 0).
   - Library: `.NET` Kestrel transport, `Node.js` grpc-js injector, `Java` grpc-netty channel과 provider, `Python` DPUMesh grpcio.
3. **Go 서비스:** Go 서비스 4개는 두 모드 모두 `dmeshgo`를 쓴다. Go runtime은 libc를 거치지 않고 socket syscall을 직접 부르므로 preload가 닿지 않는다.

- **성능:** 이번 조건에서 native가 preload보다 빠르다는 증거는 없었다.
  - Online Boutique 전체 부하에서는 frontend가 병목이라 두 모드의 처리량과 지연이 같았다.
  - 서비스별 health RPC 부하에서는 .NET, Java, Python의 차이가 측정 편차 안에 있었다.
  - Node.js는 native가 일관되게 조금 느렸다.
- **DPU 쪽 문제:** 수만 RPC/s의 지속 부하에서 DPU의 DPA process가 멈추는 문제를 새로 관측했다. 두 모드 모두에서 나왔다.

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

### Online Boutique 전체 부하

upstream loadgenerator(Locust)의 사용자에서 대기 시간을 없앴다(`constant(0)`). 사용자 32명으로 15초 warm-up 뒤 60초를 쟀고, preload와 native를 번갈아 2회씩 돌렸다.

| 실행 | frontend req/s | p50 | p95 | p99 | 실패 |
|---|---|---|---|---|---|
| preload 1 | 369.4 | 70 ms | 160 ms | 180 ms | 8 |
| native 1 | 372.3 | 71 ms | 160 ms | 170 ms | 5 |
| preload 2 | 367.5 | 71 ms | 160 ms | 180 ms | 1 |
| native 2 | 377.1 | 69 ms | 160 ms | 170 ms | 4 |

- **실패:** 모두 checkout의 HTTP 500이었다. payment 서비스가 Faker가 만든 `visa_electron` 카드를 거절한 앱 수준 응답이다.
- **병목:** frontend(Go)가 60초에 CPU 약 155–160초(약 2.6 core)를 써서 병목이다.
- **proxy 쪽 지연:** proxy가 잰 서비스별 응답 지연은 두 모드에서 같은 histogram bucket에 들었다.
- **서비스별 CPU:** cart(.NET)만 두 번 모두 native가 약 40% 더 많았다(46.2·45.9초 → 65.1·63.4초). 다른 서비스는 회차마다 방향이 바뀌었다.

### 서비스별 health RPC 부하

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
- Online Boutique 전체 부하(서비스당 수천 RPC/s)에서는 나오지 않았다.

## 측정 조건

- **장비:** rapids4 host, BF-3 DPU(DOCA 3.1.0105). PCI는 `03:00.0`/`94:00.0`, reverse는 `dpu-dma`.
- **Proxy 설정:** `LINKERD2_PROXY_CORES=1`, `DMESH_NUM_WORKERS=1`, `DMESH_SHARDED=1`, `DMESH_BUSY_POLL=1`, `LINKERD2_PROXY_DESTINATION_PROFILE_NETWORKS=127.0.0.0/8,10.99.0.0/16`.
  - Mock은 `MOCK_POLICY_ECHO_TARGET=1`로 띄웠고, L4 실행에만 `MOCK_OUTBOUND_OPAQUE=1`을 더했다.
- **DPUMesh:** `feature/online-boutique`(`cb9147b`)에 아직 커밋하지 않은 변경을 더한 상태다.
  - preload IPv6 지원.
  - DPU worker flow 한도 64.
  - C++ reactor 분리와 stream C ABI.
  - 언어별 adapter.
- **Proxy:** `c09915bf`에 `MAX_CONNS = 64`를 더했다.
- **Online Boutique:** fork의 `dpumesh` branch(`v0.10.7` 기준)를 `dpumesh/setup.sh`로 빌드하고 `dpumesh/run.sh`(`DPUMESH_MODE=preload|native`)로 실행했다.
- **Runtime:** Go 1.27.1, Node.js 24.13.0, Python 3.10.12, OpenJDK 21.0.12, ASP.NET Core 10.0.12, gRPC C++ 1.80.0 source.
- **보존 자료:** 실행 script와 로그는 host의 `build/e2e/`에 로컬로만 보존한다.
  - Script: `ob-cycle.sh`, `ob-load.sh`, `ob-health.sh`, `stream-hw.sh`, `grpc-ab.sh`.
  - 로그: `ob-*`, `load-*`, `health-*`, `stream-s1`, `grpc-ab`, `online-boutique-2026-09-29`.
