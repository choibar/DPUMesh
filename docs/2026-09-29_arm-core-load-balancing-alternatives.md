# DPUMesh Arm core 부하 불균형 — 대안 설계 검토 메모

작성 기준: 4개 lens(host placement / DPU datapath / proxy runtime / dynamic migration / app-mesh)의 17개 대안과 각 2건의 적대적 리뷰, 그리고 리뷰가 인용한 코드·측정치를 직접 재확인한 결과(아래 file:line은 이번에 다시 읽어 확인한 것).

---

## 0. 결론 요약

1. **불균형의 원인은 "connection-level load balancing"이 아니라 3중 pinning + hash 충돌이다.** (a) dst key → worker (`replicaKey` = `gid % W`), (b) caller flow → callee와 같은 worker (같은 `replicaKey`를 `Dial`에도 사용), (c) **proxy 내부 per-dst outbound cache가 process 전역**이어서 한 dst의 backend leg(tower Buffer worker + balancer + h2 client conn task + backend pump)가 "첫 요청을 보낸 shard"에 고정. 여기에 21 keys / 16 workers의 wrap으로 reservation-1/2/3가 geo-0, geo-1, rate-0 worker에 겹친다. 측정된 67%/66%/53% shard는 두 key가 겹친 core다.
2. **어떤 balancing도 throughput은 +5~10%가 상한이다.** DSB 18k에서 DPU 평균 busy 84%(유휴 2.6 core), 84 channel × 888 req/s = 74.6k proxy req/s(≈19.9k user) latency ceiling의 90%, host도 13.2 core 사용(≈20~21k에서 포화). **진짜 이득은 p99(0.73~1.15 s → 100 ms대)와 SLO-usable throughput(14k → 16~17.5k)** 이다. 이 메모의 모든 "기대 효과"는 이 틀 안에서 읽어야 한다.
3. **살아남은 것은 정적 배치(placement-time)뿐이고, 동적 migration 계열은 전부 "정적 배치 먼저, migration은 teardown 수리 후"로 축소됐다.** 이유: park mode(`DMESH_NO_TEARDOWN=1`)에서 모든 이동은 hot worker의 slot을 영구 소모(32개/worker), CLIENT-mode teardown은 Rust proxy를 죽이는 결함이 미해결, 공유 DPA context 이후 DPA fatal이 process 전체를 죽인다.
4. **권장 경로:** E0(프로토콜 gate) → P1(명시적 placement table, Go만, 채널 0 추가) → P2(caller/callee worker 분리 + first-touch prewarm, Go만) → P3(양쪽 leg를 같은 core에 두는 per-shard backend: proxy 변경 또는 host-only virtual replica, **flow 예산에 gate**) → elephant 단일 연결은 host-side K connections(P3와 결합), 못 열면 proxy 내 per-stream pool(R1, +30~60% 상한). 교차 선행과제: 84-channel wedge/EU 규명(DPA thread multiplexing), teardown/park 수리, 1.13 ms per-hop floor.
5. **실무적으로 가장 먼저 부딪힐 벽:** 현재 tree의 DPU는 session frame만 받고(`comch_server.c:201` → `session_fail`), DSB가 쓰는 dmeshgo hostlib는 raw message를 보낸다(`host_lib.c:136` → `comch_client_legacy.c`, `export_dma_metadata` raw send; `.so`는 9/23 빌드). **DSB 실험은 pre-`05e7ebb` DPU 바이너리로 돌리거나 hostlib를 session protocol로 포팅해야 한다.** P1/P2는 Go만 바뀌므로 전자로 즉시 가능하다.

---

## 1. 코드가 말하는 불균형의 실제 구조 (검증된 사실)

### 1.1 3중 pinning

| # | pinning | 코드 | 결과 |
|---|---|---|---|
| (1) | dst key → worker | `apps/dmeshgo/dsb-integration.patch:383-399` `replicaKey`: `gid % workers()`, `keyOrder` = geo, rate, search, profile, rec, user, reservation, review, attractions | 990 calls/1000 인 profile도, 10 calls인 user도 key당 정확히 core 1개 |
| (2) | caller flow → callee의 worker | 같은 `replicaKey`를 `Dialer`/`DialReplicated`(`:266`, `:408`)와 `Listen`(`:458`, `:464`)이 공유 | 4 frontend의 profile-0행 flow 4개가 모두 profile-0의 core에서 h2 server 종단 + tower + authz 실행 |
| (3) | dst key의 backend leg → 첫 요청 shard | `linkerd/app/src/lib.rs:281-287`에서 `dmesh_outbound`를 **1회** 생성, `:417`에서 shard마다 `.clone()`; per-dst cache는 `outbound/src/http.rs:103 push_new_idle_cached`, `discover.rs:56 push_new_cached_discover`, key는 `sidecar.rs:169/177` = `orig_dst`(+version)만; endpoint는 `concrete.rs:109 NewQueue`(tower Buffer worker task), h2 client conn task는 `proxy/http/src/h2.rs:134 tokio::spawn` | 한 dst의 balancer/queue worker/h2 client encode-decode(요청당 cycle의 **~40~50%**)가 그 key에 처음 요청한 shard의 current_thread runtime에 영구 고정. 오늘은 (2) 덕분에 A=B=C(모두 한 core)라 이 pinning이 **보이지 않을 뿐** |

Proxy는 (1)(2)를 요구하지 않는다: backend registry는 process 전역 `HashMap<SocketAddr, Vec<(slot, DmeshIo)>>`(`linkerd/doca/src/lib.rs:30-33`), `DmeshOrTcp::call`은 addr로만 `take`(`outbound/src/tcp/connect.rs:73-77`), `DmeshIo`는 `Arc<Mutex<Inner>>` + cross-thread waker(`io.rs:221-238`). 단, `unpublish`는 worker-local slot 번호로만 evict(`lib.rs:58-67`)하므로 **한 addr를 두 worker가 publish하는 순간 잠재 버그가 터진다** — (worker, slot) 키 재정의는 P3의 선행조건.

### 1.2 `gid % 16` wrap 충돌 (측정 표의 재해석)

gid: geo 0-1, rate 2-5, search 6-9, profile 10-11, rec 12-13, user 14, reservation 15-18 → worker 15,**0,1,2**, review 19 → 3, attractions 20 → 4. shard i는 core 15-i.

| pegged core | shard | 실제 내용 |
|---|---|---|
| 15, 14 | 0, 1 | geo-0 **+ reservation-1**, geo-1 **+ reservation-2** (측정 67%, 66%) |
| 13 | 2 | rate-0 **+ reservation-3** (53%) |
| 11 | 4 | rate-2 + attractions |
| 5, 4 | 10, 11 | profile-0, profile-1 (59%, 58%) |

즉 "geo 67%"는 geo 단독이 아니다. reservation 가중치는 자료가 상충한다(app lens 문맥 "5 calls/1000" vs 리뷰 "search→reservation CheckAvailability ≈ 605/1000, 2026-09-02에 4 replica로 늘려 +55%"). **E1 calibration이 결정**하지만, rate-1(core 12)·rate-3(core 10)이 pegged가 아닌 것과 정합적이므로 reservation은 hot key로 가정하는 편이 안전하다.

### 1.3 예산과 두 개의 상한 (측정)

- DSB 18k: user 17,897 req/s = proxy 67.1k(3.75 pass/req). DPU 평균 busy 84%(13.4 core), pegged 6 core, core 1은 44%. **13x는 shard thread CPU 기준(67% vs 5%)이고 core 기준 spread는 2.3x**; hot core의 나머지 33~45 point는 thread 밖(kernel/epoll/IRQ/DOCA helper/pinning 안 된 mock). 이 부분은 flow 이동으로 안 움직일 수 있다(E1이 확인).
- 상한 A: 완전 균형 = 84% → 100% = **+19%(≤21.4k)**. 상한 B: 84 × 888 = 74.6k proxy = **≈19.9k user**(현재 90%). 상한 C: host 13.2 core @18k → ≈20~21k에서 host 포화. 따라서 **throughput 상한 +5~10%**.
- p99: 12k에서 DMA 41 ms vs TCP 28 ms — 이 격차는 1.13 ms/hop × 3.75 hop의 transport 지연이지 core 불균형이 아니다. 18k의 0.73~1.15 s만 hot core queueing(제거 가능).
- 1-core profile(2026-09-09/10): 106~115k cycles/req, h2 crate 19.5%, outline atomics 17.7%, tokio 6.3%, **DOCA+driver+DmeshIo 3.5~4.5%, kernel 1.1~1.4%**. "datapath below the proxy"는 옮길 가치가 없다.

### 1.4 리뷰가 뒤집은 전제

| 통념 | 실제 |
|---|---|
| "client flow를 옮기면 backend는 pump(~6%)만 남는다" | backend leg L7 ~40~50%가 first-touch shard에 고정(1.1 (3)). 이것이 A1/A4/DPU-A1/proxy-D/dyn-A~D 전부의 효과를 절반으로 깎았다 |
| "work-stealing은 core당 -30%" | 8.9k/core는 busy-poll CPU(spin 포함). event mode 2 driver/4 core = 41.3~42.7k ≈ sharded 2 core 40.2k → **core당 e≈0.5~0.6** |
| "K connections는 core당 +32~51%" | 총 동시성 64 고정 시 1→2→4 conn = 12.8k→14.4k→14.0k(**+12%**, 2026-09-25). +51%(19.3k)는 동시성 4배의 효과 |
| "EU 예산은 wedge 원인이 아님(falsified)" | falsify한 것은 thread **객체** 512개 할당. kernel은 활성 flow마다 `while(1)` polling으로 EU 1개 점유(`dpa_kernel.c:182`, `docs/2026-09-26_dpa-process-sharing-plan.md:45-53`); 118 EU에서 119번째 flow가 silent hang(arm-scaling.md:74-80). 증명된 최대는 128 flow@190 EU. DSB 126 flow OK / 156 partial / ~236 collapse. **190 EU에서 156 flow 실패는 EU만으로 설명이 안 되므로** 미확정 — E3이 결정 |
| "13x spread" | thread 기준. core 기준 2.3x, 유휴 총량 2.6 core |
| "DSB 경로 = libdpumesh" | dmeshgo hostlib(connection당 comch client 1개). libdpumesh는 process당 server 1개(`carrier.c:173-176`, `channel.c:312 EALREADY`) — DSB보다 **더** 집중된다 |

---

## 2. 탈락·축소된 대안

| 대안 | 판정 | 탈락/축소 이유 (리뷰 근거) | 구제된 형태 |
|---|---|---|---|
| A2 multi-session libdpumesh, RR/p2c/least-loaded + DPU feedback | sound-w/c + **flawed** | 측정 경로(dmeshgo)에 없음; 84 flow 모두 t=0에 연결되어 load signal 부재 → RR로 퇴화; 가중치 없는 RR/random은 시뮬레이션상 max/mean 1.99 vs 오늘 2.05(**hot core 불변**); `HELLO_ACK.status`≠0은 host가 session error로 처리; idle session은 control PE를 진행하지 않아 push된 LOAD frame이 소비 안 됨 | **P4**: libdpumesh 앱용 enabling 작업으로만, 정책은 static weight, 신호는 pull-at-connect |
| A0 pump-only offload (per-slot PE, pump을 helper core로) | **flawed ×2** | 옮길 수 있는 몫은 core의 3~6%(profile 3.5%+1.4%); epoll wake는 shard에 그대로 남고 응답 쓰기마다 eventfd syscall 추가; W=16에 helper core 없음; RUNNING tick의 `push_fin`/`state`/`recv_segs`가 single-owner 위반 | 없음. 측정 부분만 E1로 흡수 |
| Proxy-A 완전 non-sharded (env만) | sound-w/c + **flawed** | e 0.5~0.75 → DSB 18k(U_avg 0.84)에서 **regression 예측(13~15k)**; W=N busy-poll driver는 steal 불가; p99 이득은 load < 새 ceiling일 때만 | 없음. zero-code 상한 실험으로만(E-옵션) |
| Proxy-C adaptive per-stream hand-off | sound-w/c + **flawed** | hyper는 `service.call(req)`를 connection task에서 실행(`vendor/hyper/src/proto/h2/server.rs:423`), executor로 넘어가는 것은 response future뿐 → hot core의 **~27%**만 이동; depth-1 edge에 hop당 +0.2~1.0 ms → **hot edge가 느려짐**; DSB는 event mode라 yield-gap 신호 무효 | connection 단위 hand-off 변형은 P3b로 흡수 |
| Proxy-D accept-time L7 placement (home 우선) | sound-w/c + **flawed** | 모든 DSB flow가 t=0 load 0에서 home 배치 → **효과 0**; backend leg 고정(1.1(3)); driver가 tick마다 모든 handle의 Mutex를 잡아 idle flow도 cache-line ping-pong | **P3b**(dst-key 단위 + per-shard stack + dirty flag) |
| DPU-A2 pump pool EPOLLONESHOT/EXCLUSIVE + L7 work-stealing | sound-w/c + **flawed** | `EPOLLEXCLUSIVE`는 이 kernel에서 PE fd(epoll type)에 EINVAL; 단일 elephant 이득은 이미 측정됨: 12.7k → 16.1k(2 core) → 17.1k(4 core), **+35% at 2x CPU/req**; DSB hot channel은 1.3~2.2k req/s로 serial 한계(12.7k)에 한참 못 미침 → stream split 가치 0 | R1의 상한 근거로만 |
| A3 stripe (같은 dst key를 K worker에 publish) | sound-w/c ×2 | option (ii)는 불가: per-dst cache가 `take`를 key당 1회만 → 나머지 K-1 spare는 EU만 소모; "stripe = distinct dst key = in-process replica"만 동작; 예산 산술 오류(84 유지 불가) | **P3 route (ii)** + **A-K** |
| A4 learned EWMA + drain-and-redial | sound-w/c + **flawed** | `dmesh_get_tx_stats`는 block-pool counter, `Stats`는 worker 집계 → per-key 가중치 소스 부재; park는 slot을 영구 소모 + 첫 park 후 `saw_teardown`으로 driver가 1 ms tick polling으로 강등(`driver.rs:487/529/592/599`) | 정적 = **P1/P2**; 동적 = **M1(gated)** |
| dyn-A/B/C/D (GOAWAY re-spawn / REDIRECT / whole-edge / host make-before-break) | 대부분 sound-w/c(C, D는 1건 flawed) | 공통: 이동 1회 = hot worker의 slot 1~2개 영구 소모(`comch_server.c:1311-1364`, `Released 0`), CLIENT teardown UAF, 공유 DPA context fatal, 새 연결마다 500 ms `policy.changed()` 대기(`dmesh.rs:181-186`); DSB mix는 stationary → 동적 가치 0 | 정적 = **P2/P3**; 동적 = **M1** 하나로 통합, sensor로 D의 load-vector, actuator로 D의 make-before-break 채택 |
| App-4 load-aware rotation | sound-w/c + flawed | 위와 동일 + per-slot 신호 부재 | M1 |

---

## 3. 생존 대안

각 항목: 메커니즘 / 해결·미해결 / 기대 효과 / 비용·리스크 / 노력 / 검증. 수치는 (측정)·(추정) 구분.

### 3.1 배치 시점 (placement-time)

#### P1. 명시적 placement table + 측정 가중치 기반 replica spec (채널 0 추가, Go만)

**메커니즘.** `dmesh.go`의 `replicaKey`에서 `gid % W`를 env(`DMESH_PLACEMENT="srv-geo:0=15,srv-reservation:1=6,..."`)로 읽는 명시적 (service, replica) → worker 표로 바꾼다. client와 server가 같은 표를 읽으므로 proxy는 무변경. Stage 1은 replica 수를 그대로 두고 reservation-1/2/3와 review/attractions를 geo/rate worker에서 떼어 search/rec/user worker로 옮긴다(충돌만 제거). Stage 2는 E1에서 얻은 key별 thread CPU로 replica 수를 재배분(예: rate:4 geo:3 profile:3 search:3 rec:2 reservation:3 + user/review/attractions:1 = 21 keys, **worker당 ≤2 key, hot unit ≤16개**)하고 LPT로 packing한다.

**해결 / 미해결.** 해결: hash 충돌, key 간 weight 차이. 미해결: pinning (2)(3) — 한 key의 caller 4 flow + backend leg는 여전히 한 core; single-caller elephant; 1.13 ms floor.

**기대 효과(추정, 리뷰 corrected).** Stage 1: max shard thread CPU 67% → ~59%(profile이 최대), pegged core 6 → 2~4, throughput ±0. Stage 2: max ~48~53%, min 10~25%, **spread 13x → 3~5x**, busiest core 85~95%(thread 밖 35~40 point 때문에 <85%는 불가), **usable(p99<100 ms) 14k → ~16k, p99@18k 0.73~1.15 s → 100~200 ms**, hard ceiling ~20k(host +2 Go process로 host-bound). 제안 원안(reservation 4→1, hot unit 17개)은 ~73% key를 만들어 **역행**하므로 채택 불가.

**비용·리스크.** worker당 slot 밀도 12 → 최대 18(4 client + taken + spare × 3 key) — DSB에서 돌린 적 없는 밀도(wedge 위험, E3 병행). 가중치는 workload mix당 1회 calibration. 채널 84 그대로.

**노력.** S (`dmesh.go` + `dsb_run_services.sh` spec).

**검증.** E2. 판정: pegged core 수, max/min shard thread CPU, `mpstat -P ALL`, p99@18k, "chan: connected" 84개, 멈춘 wrk 0.

#### P2. caller-side placement 분리 + first-touch prewarm (채널 0 추가, Go + 옵션 S proxy)

**메커니즘.** `Dialer`/`DialReplicated`만 caller flow의 server 이름을 (caller id, callee, replica) 가중 LPT 표로 고르고 `Listen`은 `replicaKey`를 유지한다. proxy는 이미 addr로만 `take`하므로 shard A의 client flow가 worker B의 backend를 쓴다(non-sharded 모드가 매일 하는 일; sharded에서는 미검증). 핵심 추가는 **B==C prewarm**: dst의 first request를 그 key의 home shard(=backend pump shard)에서 나가게 해 pinning (3)의 owner를 결정론적으로 pump shard에 맞춘다 — Go만으로는 `DMESH_CLIENT_ID=0` frontend를 `replicaKey`에 남기고 시작 시 dst마다 warm RPC 1개를 먼저 보내게 하면 된다(5 s discovery idle timeout 후 재결정되지만 hot key는 idle하지 않음). 모든 caller process(search replica 포함)에 고유 `DMESH_CLIENT_ID`를 준다(현재 search 4개가 동일 src 4-tuple).

**해결 / 미해결.** 해결: pinning (2) — caller-side L7(요청당 ~50%)이 84 flow 단위로 퍼짐. 미해결: pinning (3)의 residual(backend leg ~40~50%가 key당 한 core), single-flow elephant, floor. prewarm 없이는 hop 4개/req + hot owner 2개가 한 shard에 겹칠 확률 ~60%.

**기대 효과(추정).** hot shard thread CPU 67 → **40~48%**(residual 27~33% + 분산 몫), **spread 13x → ~3x**(user 5%는 그대로), DPU 총 busy 84 → 90~95%(cross-runtime wake 2회/req: 각 1~3 µs, 대상이 park 중이면 수십 µs), throughput **+3~8%**(≤19.9k), p99@18k **100~300 ms**, usable **16~17k**. per-hop RTT +0.05~0.1 ms → latency ceiling 74.6k → ~67~70k(이미 측정된 수준이라 headroom 변화 없음).

**비용·리스크.** sharded 모드 cross-shard `take` 최초 실행(2026-09-28 unsharded run의 미규명 DPA fatal 경로와 인접); shared-nothing 불변식 약화; 5 s idle 후 owner 이동으로 run 간 변동. spare 수는 **올리지 말 것**(key당 take 1회; 63 channel 추가는 wedge 영역).

**노력.** S (Go ~15줄 + warm-up). 옵션 S proxy: `info_span!("dmesh")`에 `worker=i` 추가(검증용), registry (worker, slot) 키(P3 선행).

**검증.** E4. 판정: gRPC microbench cross-shard take에서 core당 req/s 비율(M=1..4에서 측정, 0.75 미만이면 kill), "backend gone; refusing" 0, DSB에서 'Spawning p2c pool queue'가 뜬 shard == pump shard, max shard ≤50%, p99@18k ≤300 ms.

#### P3. 양쪽 leg를 같은 core에: per-shard backend + worker-aware take (flow 예산에 gate)

**메커니즘.** pinning (3)까지 깨서 (caller, callee) edge 전체가 배치된 core에서 실행되게 한다. 두 route.
- **(i) proxy**: `dmesh_outbound`를 shard마다 `mk()`(App이 `Outbound<()>`·profiles·resolve·policies를 보존해 shard thread 안에서 생성; W배 discovery/policy watch), `DmeshOrTcp::call`에서 thread-local shard id를 읽어 `take(addr, prefer=shard)`, registry를 (worker, slot)로 키잉, live-per-(addr, worker) guard(이미 그 shard에 h2 client가 있으면 spare를 잡지 않음). host: server process가 affinity set의 worker마다 spare 1개를 publish(dmeshgo Listener는 spare마다 `DPUMesh{j}`를 dial 가능; libpumesh는 P4 필요).
- **(ii) host-only**: hot replica마다 V개의 **virtual dst key**(`10.0.<idx>.<1+r>`, port `8086+v`)를 한 process가 `combinedListener`로 서비스하고 caller f는 `v = f % V`를 dial(V = caller 수). registry가 SocketAddr 키라 proxy 무변경, mock-policy echo target이 임의 port를 Forward backend로 처리. 사실상 "in-process replica".

두 route의 자원 비용은 동일: **(hot key, 추가 worker)당 +2 flow(taken + spare) = +2 EU**.

**해결 / 미해결.** 해결: pinning (1)(2)(3) 전부, cross-core hop 0(sharded e≈1 유지). 미해결: single-flow elephant(연결 1개는 여전히 core 1개), floor, host 포화, **flow 예산**.

**기대 효과(추정).** 가중 LPT로 **spread → 1.3~1.5x**(상한 = mean + max edge weight ≈ 15%), busiest core ~85%, DPU-limited ceiling 19.5~20.5k(host가 ~20~21k에서 먼저 묶임), **p99@18k 80~150 ms, usable 16.5~17.5k**. route (ii)는 caller가 대칭일 때만 균형(caller identity로 배치; 단일 elephant caller는 다시 pin됨 → A-K와 결합).

**비용·리스크.** 예산: 12 hot key × k=2 → 126 → 150 flow > 증명된 128. **geo/profile만 V=2 + zero-traffic key(review, attractions) spare 제거 → ~132 flow**가 E3 전 상한. per-shard stack은 TCP 경로 cache에도 영향(stock outbound h2load 회귀 확인). 4:1 backend h2 multiplexing이 사라져 요청당 ~2~4% cycle 증가(depth 감소, 2026-09-28 q64 vs q256 +9%/RPC의 backend 몫).

**노력.** (i) M proxy + S Go. (ii) S(launch script + port formula). 둘 다 예산은 E3에 의존.

**검증.** E5.

#### P3b. proxy-side accept-time placement (host가 worker를 못 고를 때의 대체)

libdpumesh처럼 process당 server가 1개인 host를 위해 proxy가 `ConnReady`에서 shard `Handle`을 고른다(P3 (i)의 per-shard stack 필수; `new_service`도 배치된 task 안에서; driver는 idle remote handle의 Mutex를 tick마다 잡지 않도록 tx/rx/watermark **dirty flag** 추가 — busy-poll에서 특히). pump은 accept한 driver에 남아 segment마다 cross-core wake → e ~0.75~0.85(추정). 배치 신호는 t=0에 load가 없으므로 **정적 표(dst→shard) 또는 가중치**여야 한다. 노력 M. host가 고를 수 있으면 P3 (i)/(ii)가 항상 우월.

#### P4. libdpumesh multi-session (enabling만; 정책은 static weight)

`channel_dev`를 doca_dev 1개 + `sessions[W]`로 분리(mmap은 dev에 1회 등록되므로 dev를 나누면 안 됨; 같은 dev 위에 comch client N개는 미검증 → smoke test 필수), `channel_conn_open(session, cfg)`, `DPUMESH_SERVERS` 또는 `target@DPUMeshN` 문법, `slot_open`이 정적 가중치 표로 session 선택, `backend_maintain`이 spare를 여러 session에 분산. host-dpa reverse 모드는 `dpa_runtime`으로 DPA process 공유. **하지 말 것:** RR/p2c/least-loaded(§2), `HELLO_ACK.status` 재활용, unknown frame(구 host에서 session 전체가 EPROTO로 죽음 → 버전 gate 필요). `doca_comch_cap_get_max_clients`를 로그로 남겨 server당 client 상한 확인. 노력 M~L. 가치: `integrations/grpc/go` 류 앱에서 P2/P3를 가능하게 함(오늘은 process의 모든 flow가 worker 1개 → DSB보다 나쁨).

### 3.2 flow 이동 (migration)

#### M1. 통합 migration — teardown 수리 후의 one-shot corrector

**메커니즘(구제된 형태).** sensor: DPU per-core busy(`/proc/stat`, thread CPU가 아님 — 67% thread는 85% 임계를 절대 안 넘김; busy-poll에서는 work-based 지표)와 per-slot pump counter(`driver.rs pump_recv/pump_send`에 추가; 현재 `objs->recv_msg_cnt`는 worker 집계). host로의 전달은 dyn-D의 방식이 가장 싸다: push rcvbuf의 미사용 영역 `[2080, 4096)`에 worker의 기존 `doca_dma`로 W-byte busy vector를 100 ms마다 DMA-write(프로토콜 변경 0, region이 desc/data ring과 분리되어 ordering 불필요). actuator 우선순위: (a) gRPC-aware client의 **make-before-break**(새 ClientConn READY 후 old drain·close; `grpc.ClientConn.Close()`는 in-flight를 cancel하므로 in-flight counter 필요) — stall 0; (b) DPU-only GOAWAY + 다른 shard `Handle`로 re-spawn(per-connection `Close` 핸들을 `server.rs:335` 근처에서 (worker, slot) 키로 등록; src port는 결정론적이라 SocketAddr 키는 alias); (c) OPEN-time REDIRECT는 P4 + 버전 gate + host multi-session 위에서만.

**하드 gate(전부 충족 전에는 실행 금지).** ① CLIENT-mode teardown이 proxy 안에서 안전(100회 teardown+reconnect, event mode, 180 s idle gap 포함, DPA fatal 0, `Released == Assigned − live`); ② `saw_teardown` 강등 제거(첫 park 후 driver가 1 ms tick polling으로 영구 전환 → depth-1 edge RTT 악화); ③ `dmesh.rs:181-186`의 500 ms `policy.changed()` 대기 제거(cache hit 시 매 새 연결이 꽉 채워 기다림 → 이동 1회당 hot edge 0.5 s 정지, ~675 RPC); ④ registry (worker, slot) 키.

**해결 / 미해결.** 정상 상태는 P2/P3와 동일; 고유 가치는 **workload drift**뿐(DSB mix는 stationary → 가치 0). 미해결: single-flow elephant, floor.

**기대 효과.** P2/P3 수렴값. 오늘 실행 시 **음의 효과**: 이동당 hot worker slot 1~2개 영구 소모(hot worker당 ~7~25회 후 그 worker는 새 연결 거부 → hot service의 다음 spare 등록 실패), re-dial wedge 경로, process 전체 DPA fatal 노출.

**노력.** M(app+proxy) + **L(transport 선행: teardown/park)**.

**검증.** E7.

### 3.3 요청/스트림 단위

#### R1. two-tier split: driver + h2 connection은 pin, per-stream future는 work-stealing pool (B의 구제 형태)

**메커니즘.** `DMESH_SHARDED` 유지. `dmesh.rs:202`의 `outbound.new_service(target)`를 `pool_handle.enter()` 안에서 호출해 tower Buffer worker·SpawnReady·h2 ClientTask(`h2.rs:134`)가 pool에 생기게 하고, `server.rs:87`의 `TokioExecutor`를 pool `Handle`로 spawn하는 executor로 바꿔 `H2Stream` future를 훔칠 수 있게 한다. **`h2.rs:88`만 바꾸면 안 된다**(backend framing만 pool로 가고 dispatch는 shard에 남아 최악). `LINKERD2_PROXY_CORES ≥ 2` + `rt.rs`에 `on_thread_start` pinning + taskset 확장(현 스크립트 CORES=1이면 "pool"은 pin 안 된 current_thread 1개). env gate.

**해결 / 미해결.** 해결: **단일 h2 연결이 core 하나를 넘는 경우**(proxy 내 유일한 lever). pin 잔존: server h2 framing ~10%, driver/DOCA 3.5%, `CallInPlace` dispatch, tokio/atomics 일부(~25~35%). 미해결: floor, 채널 수.

**기대 효과(추정).** 단일 elephant 연결 **+30~60%**(12.8k → 17~20k @M=64; K=4 baseline 19.3k → 25~30k) — non-sharded 측정 상한(17.1k, +35%, CPU/req 2배)과 정합. DSB 18k(W=16 shard + 같은 core에 겹친 pool): hot core 100 → 65~75%, throughput +4~13%, 총 수요 14.1~15.3 core로 p99 수십 ms 복귀 가능; **15k 이하에서는 latency-bound edge에 3~9% 손실, 균형 부하에서 core당 12~33% 손실**. 요청당 cross-core hand-off 4~6회, e_hybrid 0.6~0.8(미측정). W=8 shard + 8 pool 구성은 hot key가 8 shard에 겹쳐 오늘보다 나쁨.

**노력.** M.

**검증.** E6(c). kill: W=1 shard + pool 1 core가 sharded 1 core의 **1.3x** 미만(원안의 1.6x는 pin 몫 45~55%에서 도달 불가한 기준).

#### R2. (측정 완료된 상한) non-sharded / per-slot PE 계열

단일 flow M=64, driver = tokio task: 12.7k(1 core) → 16.1k(2) → 17.1k(4). 즉 stream-level split의 상한은 **+35%**이고 DSB hot channel(1.3~2.2k req/s)은 serial 한계와 무관. per-slot PE(DPU-A1/A2)의 C 변경(7개 `consumer_pe` progress 지점, `conn->state`/`push_fin`/counters의 atomics, ERROR+CLOSING pump join, C 프로그램 3개 opt-in)은 이 이득으로 정당화되지 않는다. 유일한 부수 이득: PE가 slot과 함께 죽어 `saw_teardown` 강등이 사라질 가능성(미검증). **채택 안 함**, 단일 연결 >10k req/s가 필요해질 때 재고.

### 3.4 애플리케이션 단위

#### A-K. hot edge당 K개 연결을 K worker로 spray (gRPC round-robin picker)

**메커니즘.** `balancedConn`을 replicas × K ClientConn으로 확장, 연결 k는 `DPUMesh{(gid + k·stride) % W}`(dmeshgo는 연결당 server 이름 선택 가능; libdpumesh는 P4), src port 공간 확장(`clientBase()+idx*8+r`는 r<8만 가정). 양쪽 leg를 local로 두려면 P3 route (ii)의 virtual key와 짝짓는다.

**해결 / 미해결.** 해결: **caller 하나의 연결이 core 하나를 넘는 elephant**(RPC는 독립이라 reorder 비용 0; Presto의 올바른 아날로그). 미해결: 고정 동시성에서는 core당 +10~12%뿐(측정); 채널 K배(EU); callee의 gRPC-go per-connection writer 상한(~9~10k, 16-core memo)은 오히려 완화.

**기대 효과.** caller 부하 > core 1개일 때 K core로 선형 분할; 그 외 +10~12%.

**비용·리스크.** flow 예산(K EU/edge), host memory, dead stripe는 reconnect 경로 수리 전 재dial 불가.

**노력.** S.

**검증.** E6(b).

#### A-O. hot edge를 opaque(L4 forward)로 (config만, 보완재)

`MOCK_OUTBOUND_OPAQUE` per dst → 요청당 ~4x 저렴(62k vs 15.3k req/s/core @-m100). 그 edge의 per-RPC L7(route/metrics/policy)을 포기. balancing이 아니라 elephant 축소; P3 후에도 core가 90%를 넘는 edge(profile)에만.

#### 공짜 부수 항목(균형과 무관하지만 hot core를 깎음)
`dma.c` push-cursor pull 경로의 `doca_mmap ... isn't aligned to 64B` 로그 **4,700 line/s**(DOCA call + logging tax, 전 worker 상시); mock 3개가 taskset 없이 shard core에 뜸(`dsb-dmesh.sh:18-20`); mlx5 comp IRQ vector affinity(E1이 thread 밖 몫을 IRQ로 지목하면 hot core에서 떼기).

---

## 4. 순위와 권장 경로

### 4.1 순위 (근거)

| 순위 | 대안 | 왜 이 자리인가 |
|---|---|---|
| 1 | **P1** | 코드 0줄(Go 표), 채널 0, 리스크 0. 측정된 hot core 6개 중 3개가 wrap 충돌(§1.2)이므로 확실한 첫 이득. 동시에 E1 calibration 표를 만들어 모든 후속 단계의 가중치를 제공. 두 리뷰 모두 sound |
| 2 | **P2** | pinning (2)를 채널 0 추가로 깨는 유일한 방법. 4건의 리뷰(host-A1 ×2, app-2 ×2)와 dyn-A/B/D, DPU-A3의 "alternative"가 모두 이 형태로 수렴. 조건: B==C prewarm(없으면 hop 4개·owner 충돌 60%), 고유 client id |
| 3 | **P3** | "few flows, weight concentration" 문제의 실제 해답(edge 전체가 한 core, sharded 효율 유지, spread 1.3~1.5x). app-1/2, host-A4, dyn-C/D, DPU-A1의 리뷰 alternative가 전부 "per-shard stack + worker-aware take + spare on K workers / distinct key per (replica, worker)". **flow 예산이 유일한 gate** → E3 결과에 따라 (ii) V=2 부분 적용부터 |
| 4 | **A-K (+P3 (ii))** | elephant **단일 연결** 케이스의 host-side 해답. 측정상 stream split(R1/R2)은 +35% 상한이고 CPU/req 2배인 반면, K 연결은 core 선형. 단 EU 예산 |
| 5 | **R1** | 앱이 연결을 못 쪼갤 때의 proxy 내 유일한 lever. +30~60%로 유한하고 저부하에서 손실. env gate로 실험 가치는 있음 |
| 6 | **M1** | drift 대응이라는 고유 가치는 인정되나 teardown/park/500 ms 수리 전엔 음의 효과. 리뷰 12건 중 "지금 돌려도 된다"는 것이 0건 |
| 7 | **P4** | libdpumesh 앱이 W worker를 쓰려면 필수지만 DSB 문제엔 무관. static weight로만 |
| — | **P3b** | host가 못 고르는 경우의 대체; P3 (i) 없이는 효과 반감 |

### 4.2 두 케이스를 덮는 조합

- **Few flows + edge 간 weight 불균형(DSB 형태):** P1 → P2 → P3. 기대: spread 13x → ~1.5x, p99@18k 0.73~1.15 s → ~100 ms, usable 14k → 16.5~17.5k, throughput 18k → 19.5~20k(host/latency-bound). **이 이상은 balancing으로 불가** — 1.13 ms floor(84 × 888)와 host 포화가 다음 벽.
- **Elephant 단일 h2 연결(caller 하나가 core 하나 초과):** A-K로 K worker에 분할 + P3 (ii) virtual key로 양쪽 leg local. 앱이 연결을 못 늘리면 R1(+30~60% 상한). 두 경우 모두 **flow 예산 = EU 예산**이므로 DPA thread multiplexing(`docs/2026-09-26_dpa-process-sharing-plan.md` follow-up A: polling thread 1개가 N개 ring 서비스)이 근본 enabler.
- **교차 선행과제(우선순위 순):** ① E0 프로토콜 gate(hostlib 포팅 또는 old DPU), ② 84-channel wedge/EU 규명, ③ registry (worker, slot) 키 + 500 ms policy wait 제거(둘 다 S), ④ teardown/park 수리(M1과 재연결의 전제), ⑤ 1.13 ms per-hop floor 원인(100 µs tick으로 악화된 것만 앎).

### 4.3 정직하게 남기는 불확실성

- hot core의 thread 밖 33~45 point의 정체(IRQ/softirq/DOCA helper/mock/epoll)를 아무도 분해하지 않았다. IRQ affinity에 묶인 것이면 어떤 flow 이동으로도 안 움직이고, 위 수치는 그만큼 낙관적이다. **E1이 첫 실험인 이유.**
- 84 × 888 ceiling은 profile channel이 이미 2.2k req/s(depth ~2.5)라는 사실과 모순되는 "soft" 모델이다. 그럼에도 18k 위를 DMA로 본 적이 없다.
- wedge가 EU라면 P3/A-K는 multiplexing 전엔 부분 적용만 가능하고, comch client/server cap이나 per-worker 밀도라면 P1의 ≤2 key/worker 제약이 더 중요해진다. 190 EU에서 156 flow 실패는 EU 단독으로 설명되지 않는다.
- sharded 모드에서 cross-shard `take`는 실행된 적이 없다(non-sharded에서만). P2의 e는 E4 전까지 추정.

---

## 5. 테스트베드 실험 계획 (jet1 host + BF-3, 순서대로)

공통 조건: DSB rig = `/home/youngmin/dpumesh/DeathStarBench`(hotelres-bench-patches), 4 FE × 4 wrk2, `mixed-workload_type_1`, spec `reservation:4,rate:4,search:4,profile:2,geo:2,recommendation:2`, W=16, `DMESH_SHARDED=1`, event mode, load point마다 stack 재시작(re-dial wedge). 공통 계측: `mpstat -P ALL 1`(usr/sys/irq/soft 분리), `/proc/<pid>/task/*/stat`(dmesh-shard-*, DOCA helper, main runtime), `/proc/interrupts` delta, proxy 로그의 "Assigned DPA pool thread"/"taken by connector"/"published" per worker, wrk2 p50/p99/non-2xx, host busy.

| # | 실험 | 판정 지표(결정 기준) |
|---|---|---|
| **E0** | 프로토콜/빌드 gate: 현재 tree proxy + dmeshgo hostlib로 DSB 1개 edge 연결 시도 | DPU 로그 `Rejecting incompatible or malformed Comch session protocol` 유무. 뜨면 **(a)** Go-only 단계(E1, E2, E4-DSB, E5(ii))는 pre-`05e7ebb` DPU 빌드로, **(b)** proxy 변경 단계(E4-prewarm-proxy, E5(i), E6(c), E7)는 hostlib session-protocol 포팅(M) 후 |
| **E1** | baseline 계측(코드 0): 12k/14k/18k. 추가로 18k에서 `DMESH_BUSY_POLL=1` 1회, hot shard 2개에 `perf record -F 499 -g` 8 s | ① hot core의 thread 밖 몫 분해(%irq+%soft > 20 point이면 IRQ affinity 작업을 P2 앞에 배치); ② **9개 key 전부의 thread CPU 표**(P1 Stage 2·P2·P3 가중치); ③ perf에서 libdoca+driver+io+epoll 자기시간(>10%면 예상 밖, <5%면 A0 계열 영구 폐기); ④ busy-poll 대비 hot shard throughput 차이(reactor 비용 상한) |
| **E2** | **P1** Stage 1(표만, replica 동일) → Stage 2(재가중 spec, ≤2 key/worker, reservation ≥3), 각 12/14/18/21k | Stage 1: pegged core 6 → ≤3, max shard thread ≤60%, 18k throughput ≥ baseline, 84 channel, 멈춘 wrk 0(→ 충돌 vs per-key cost 분리). Stage 2: max/min shard thread ≤3x(13x에서), core busy 최대 <90%, **p99@18k <200 ms**, p99<100 ms인 최대 부하 ≥16k. 실패(≤2 key/worker에서 멈춤)면 wedge가 per-worker 밀도 |
| **E3** | wedge/EU 이분법(작동 topology 126 flow에서 한 번에 한 축만): (a) `dpaeumgmt partition query` + running DPA thread 수 == flow 수 확인; (b) key/caller 불변, spare만 +8씩 추가 → 134/142/150/158…; (c) 126 flow를 재배치해 한 worker에 comch client 14개 집중; (d) gRPC bench를 dmeshgo 방식(연결당 comch client)으로 worker당 8/12/16 client; `doca_comch_cap_get_max_clients` 로그 추가 | "요청이 일부에서 멈춤"을 재현하는 **첫 축**이 원인. (b)가 EU 수와 일치하면 EU → P3/A-K 예산 = 그 수, multiplexing이 선행; (c)/(d)면 per-worker 밀도/comch cap → P1의 key/worker 제약이 상한이고 P3는 worker당 밀도만 지키면 채널 추가 가능 |
| **E4** | **P2**: (1) gRPC bench W=2, sharded, client `DPUMESH_SERVER=DPUMesh0`/server `DPUMesh1` vs 둘 다 0, M=1,4,64; (2) DSB P2 + FE0 prewarm, 14/18k; (3) prewarm 끄고 3회 재시작(owner 충돌 변동) | (1) cross-shard/same-shard req/s per core ≥0.75(미만이면 kill), "backend gone; refusing" 0, 요청당 DPU CPU 증가 ≤25%; (2) 'Spawning p2c pool queue'/'taken by connector'의 ThreadId == pump shard, **max shard ≤50%, spread ≤3x, p99@18k ≤300 ms**, throughput ≥ baseline, 84 channel; (3) run 간 max shard 편차로 prewarm 필요성 정량화 |
| **E5** | **P3**: E3 예산 안에서 (ii) geo/profile V=2 + zero-traffic key spare 제거(~132 flow) 먼저, 예산이 허용하면 (i) per-shard stack + `take(addr, prefer)` + (worker, slot) registry + hot 4 service spare on 2 worker | flow 수 ≤ E3 예산, **spread ≤1.5x**, DPU 총 CPU ≤ baseline+5%(hop 0 검증), throughput ≥19k, **p99@18k ≤150 ms**, stall 0, (i)의 경우 stock outbound h2load 회귀 0 |
| **E6** | elephant 단일 flow(gRPC bench, client 연결 1개, M=64/256): (a) sharded 1 core baseline; (b) **A-K** K=2/4를 K worker로, 총 in-flight 고정과 K배 두 가지; (c) **R1** W=1 shard(core 15) + pool 1 core(14) | (a) 12.8k/19.5k 재현; (b) 총 in-flight 고정 시 ≥+10%, K배 시 core당 선형(핵심 판정: **연결 하나가 core를 넘을 때만 K가 값어치**); (c) 2 core ≥ 1.3x sharded 1 core 아니면 R1 kill; hot-core %와 pool thread CPU를 직접 기록 |
| **E7** | 동적 gate(M1 전제): sharded event mode, teardown ON, 100회 teardown+reconnect, 3분마다 재연결 + 180 s idle gap 포함 30분 soak; 500 ms policy wait 제거 후 connect→첫 RPC 시간 | DPA fatal 0, `Released == Assigned − live`, `Failed to start consumer completion context` 0, 첫 teardown 후 driver가 fd wake 유지(saw_teardown 강등 없음), connect→첫 RPC <20 ms. **하나라도 실패하면 M1은 그대로 보류**, 통과 시에만 one-shot corrector(worker당 ≤15 move)로 시작 |
| (옵션) | non-sharded 상한(코드 0): DSB 14k/18k, `DMESH_SHARDED` unset, W=16, `LINKERD2_PROXY_CORES=16`, event mode | 18k에서 proxy req/s가 67.1k 미만이면 "균형은 이미 충분히 좋고 per-pass cost가 lever"라는 증거; 이상이면 P3의 목표선 |

E1·E2·E4는 Go 변경 + 계측만이라 하루 단위, E3는 이분법 회수만큼(각 run ≈ 5분 + 45 s settle), E5·E6(c)·E7은 proxy/transport 변경을 포함한다. E0 결과가 (a)이면 E1~E4는 즉시 시작 가능하다.