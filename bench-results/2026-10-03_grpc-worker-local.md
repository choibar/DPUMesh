# gRPC-go: worker-local lazy backend 검증 및 성능 측정 (2026-10-03)

최종 빌드로 **21개 구성 × 3회 = 63회** 측정을 완료했다. 측정 범위의 최고 중앙값은 **59,294 RPC/s** (8 workers, 1 replica, 16 client flows), P99 **24.51 ms**였다.

최종 측정의 RPC 오류는 **0건**이다. Warm-up과 측정/종료 구간을 포함해 계측한 **93,998,638개 요청 전달**에서 worker 및 OS thread 불일치는 모두 **0건**이었다. 같은 worker의 client flow들은 replica별 backend/H2 connection 하나를 재사용했다.

**측정 조건**

- Host jet1의 격리 빌드 `/tmp/dmesh-worker-backend-host`에서 client process 1개와 server replica process들을 실행했다.
- gRPC-go RawCodec 64-byte echo, client flow마다 in-flight RPC 64개. Client는 1개 process에서 여러 `grpc.ClientConn`을 생성했다.
- DPU: sharded + busy-poll + DPU-DMA. Client placement는 round-robin으로 worker별 flow 수를 고정했고, backend는 요청한 worker에 고정했다.
- Workers 1/2/4/8, replicas 1 또는 worker 수, client flows/worker 1/2/4. Host channel의 현재 32-flow 상한 안에서 측정했다.
- Host client: CPUs 0–7, GOMAXPROCS=8. Server replicas 전체: CPUs 8–15, process마다 GOMAXPROCS=4.
- DPU worker i: CPU 11-i. 제어/측정 도구는 CPUs 0–3. DPU CPUs 12–15가 offline이어서 이번 실험은 8 workers까지 진행했다.
- 각 구성은 별도의 proxy/server 기동으로 시작했고, 같은 proxy/server를 유지하면서 client를 3회 재실행했다. 회당 warm-up 10초 + 측정 30초.
- 표는 3회 중앙값이다. P99도 각 회에서 산출한 P99의 중앙값이다. 그래프 오차막대는 3회 최소–최대이며 confidence interval이 아니다.
- CPU는 1초 간격 /proc 샘플 중 측정 구간에 들어가는 구간의 tick 차이로 계산했다. 100%는 CPU 1개이며 server CPU는 모든 replica의 합계다.
- 모든 최종 결과는 동일한 proxy, bench-client, bench-server, native Host library 바이너리 SHA-256을 사용한다. 서로 다른 proxy 기동 사이의 변동은 동일 기동 내 3회 범위보다 클 수 있다.

**Throughput (K RPC/s)**

| DPU workers | Server replicas | 1 client flow/worker | 2 client flows/worker | 4 client flows/worker |
|---:|---:|---:|---:|---:|
| 1 | 1 | 9.91 | 13.32 | 14.51 |
| 2 | 1 | 20.84 | 27.12 | 29.13 |
| 2 | 2 | 22.96 | 25.79 | 25.29 |
| 4 | 1 | 39.62 | 48.60 | 50.52 |
| 4 | 4 | 42.31 | 42.74 | 43.02 |
| 8 | 1 | 55.24 | 59.29 | 56.28 |
| 8 | 8 | 55.67 | 55.19 | 56.56 |

![Throughput scaling](/home/youngmin/DPUMesh/bench-results/2026-10-03_grpc-worker-local.png)

**Latency와 CPU 상세**

| Workers | Replicas | Client flows | RPC/s median [min–max] | P50 / P99 ms | DPU worker CPU 범위 % | Host client / server CPU % |
|---:|---:|---:|---|---|---|---|
| 1 | 1 | 1 | 9,914 [9,879–9,921] | 6.46 / 8.65 | 99.6–99.6 | 62 / 50 |
| 1 | 1 | 2 | 13,318 [13,313–13,362] | 9.71 / 13.29 | 99.5–99.5 | 78 / 63 |
| 1 | 1 | 4 | 14,511 [14,501–14,522] | 17.70 / 22.44 | 99.5–99.5 | 82 / 67 |
| 2 | 1 | 2 | 20,837 [20,783–20,854] | 6.21 / 8.97 | 99.5–99.9 | 109 / 87 |
| 2 | 1 | 4 | 27,123 [27,114–27,152] | 9.52 / 13.59 | 99.4–99.9 | 135 / 109 |
| 2 | 1 | 8 | 29,126 [29,062–29,135] | 17.61 / 22.72 | 99.4–99.9 | 144 / 119 |
| 2 | 2 | 2 | 22,956 [22,924–23,001] | 5.61 / 8.19 | 99.4–99.9 | 119 / 113 |
| 2 | 2 | 4 | 25,789 [25,743–25,836] | 9.95 / 13.42 | 99.4–99.9 | 129 / 124 |
| 2 | 2 | 8 | 25,287 [25,285–25,386] | 20.26 / 24.22 | 99.4–99.9 | 133 / 122 |
| 4 | 1 | 4 | 39,616 [39,604–39,687] | 6.48 / 10.17 | 99.5–100.0 | 182 / 144 |
| 4 | 1 | 8 | 48,598 [48,596–48,662] | 10.55 / 15.79 | 99.3–100.0 | 219 / 175 |
| 4 | 1 | 16 | 50,521 [50,495–50,533] | 20.22 / 28.13 | 99.6–100.0 | 236 / 198 |
| 4 | 4 | 4 | 42,306 [42,281–42,360] | 6.05 / 9.02 | 99.3–99.9 | 195 / 219 |
| 4 | 4 | 8 | 42,740 [42,721–42,789] | 11.98 / 15.03 | 99.4–99.9 | 128 / 156 |
| 4 | 4 | 16 | 43,021 [42,998–43,026] | 23.75 / 27.11 | 99.3–99.9 | 143 / 150 |
| 8 | 1 | 8 | 55,239 [55,176–55,285] | 9.24 / 15.22 | 99.7–100.0 | 246 / 198 |
| 8 | 1 | 16 | 59,294 [59,286–59,308] | 17.37 / 24.51 | 99.8–100.0 | 170 / 157 |
| 8 | 1 | 32 | 56,285 [56,272–56,303] | 36.36 / 44.65 | 99.0–100.0 | 178 / 152 |
| 8 | 8 | 8 | 55,669 [55,632–55,703] | 9.19 / 12.27 | 99.5–99.8 | 160 / 271 |
| 8 | 8 | 16 | 55,192 [55,114–55,202] | 18.55 / 22.33 | 99.7–99.9 | 175 / 256 |
| 8 | 8 | 32 | 56,557 [56,484–56,572] | 36.23 / 41.52 | 99.6–99.9 | 199 / 236 |

최고 처리량 구성의 Host CPU는 client **1.70 cores**, server **1.57 cores**였다. Control dispatcher는 **0.93%**였다.

아래는 8-worker의 각 replica 설정에서 가장 높은 처리량을 낸 구성의 개별 worker CPU다. 전체 구성의 개별 값은 JSON/CSV의 `worker_cpu_pct` 배열에 worker 0부터 순서대로 있다.

| Worker | 1 replica / 16 flows CPU % | 8 replicas / 32 flows CPU % |
|---:|---:|---:|
| 0 | 99.97 | 99.82 |
| 1 | 100.00 | 99.90 |
| 2 | 99.98 | 99.78 |
| 3 | 99.95 | 99.90 |
| 4 | 99.95 | 99.87 |
| 5 | 99.98 | 99.59 |
| 6 | 99.87 | 99.85 |
| 7 | 99.81 | 99.73 |

**해석과 한계**

- 4→8 workers 구간에서는 처리량 증가 폭이 줄었다. Server replica 증가만으로 이 제한이 해소되지는 않았다.
- Busy-poll에서는 유휴 polling도 CPU를 사용하므로 worker CPU 약 100%만으로 Arm 연산 병목을 확정할 수 없다.
- Host 전체 할당 CPU가 소진된 상태는 아니었다. 단일 Host process의 공유 channel/EQ/lock, DMA 진행 경로 등의 개별 병목은 이 측정만으로 확정하지 않았다.
- 8 replicas에서는 backend가 64개(W×R)다. Client flows 32개인 경우 총 native data flows는 96개다. 1 replica의 같은 client-flow 조건에서는 backend 8개, 총 40개다.
- 기존 `dmesh-lat` 로그는 segment 단위 probe이므로 RPC latency 결과에 사용하지 않았다. 위 latency는 client에서 측정했다.
- 별도 비정상 종료 관찰: benchmark client가 preflight 오류에서 Close 없이 즉시 종료한 경우 pool 대여 7회/반납 5회였고, 두 client flow는 약 6초 뒤 proxy를 종료할 때까지 반납 로그가 없었다. 이 짧은 관찰로 영구 누수를 단정할 수는 없으며, 비정상 process 종료 후 자동 회수는 추가 검증이 남아 있다. 명시적 Close를 수행한 오류 경로는 9/9로 정상 반납됐다.
- 이 수치는 지정한 단일-client process와 CPU/GOMAXPROCS 조건의 측정 범위 내 최고값이다.

**실제 기능 검증**

- Listener만 시작한 시점에는 data/backend flow가 0개였다. 모든 성능 구성에서 자동 확인했다.
- 각 `(worker, replica)`의 backend/H2 connection은 하나였고 반복 client 실행에서도 재사용됐다. 21개 구성의 publication/할당/반납 수를 검증했다.
- 4개 client connection의 첫 RPC를 동시에 시작해 두 workers에 backend가 하나씩 생성되는 것을 확인했다.
- Client connection 하나를 닫는 동안 다른 세 connection의 RPC가 정상 진행됐다.
- Client flow 세 개를 유지한 채 server process를 교체했다. 같은 두 workers에서 backend를 다시 만들었고 client native flow의 추가 재연결 없이 복구됐다.
- 고유 sequence payload를 사용해 1, 8064, 8065, 8192, 8193, 65537, 1048577-byte echo를 검증했다.
- Backend max=1에서 두 번째 worker의 요청이 제한 시간 내 실패하고 다른 worker의 backend로 우회하지 않는 것을 확인했다. Max=2로 재시작한 뒤 두 workers의 RPC가 정상 복구됐다.
- 최종 성능 구성에서 DPA pool thread 대여/반납 906/906, server 재시작 테스트에서 8/8, 명시적 Close를 수행한 capacity 테스트에서 9/9를 확인했다. 테스트 프로세스는 모두 종료했다.

**검증 과정에서 수정한 내용**

- Worker pinning을 고정 `15-i`에서 process의 실제 CPU affinity 안의 online CPU 선택으로 변경했다. Offline CPU에 pin 실패한 채 실험하는 문제를 방지한다.
- 요청의 origin worker/OS thread와 backend owner를 비교하는 counters, native flow의 backend 여부 로그를 추가했다.
- Bench client에 측정 구간, JSON 결과, 오류 집계, 실행 deadline, transport 정리를 추가했다. RPC goroutine이 오류로 빠진 실행은 유효한 성능 결과로 채택하지 않는다.
- Backend 64개를 동시에 생성한 초기 실행에서 5초 생성 대기 제한으로 warm-up RPC 14개가 실패했다. 해당 실행을 제외하고 생성 대기 예산을 30초로 조정했다. Caller cancellation과 H2 readiness의 기존 3초 bound는 유지했다. 생성 자체의 cold-start 지연은 남아 있다.
- 6초 지연 backend publication 회귀 테스트가 통과했고, 최종 빌드로 전체 63회 측정을 다시 구성했다.
- 종료된 mock process를 검사할 때의 ESRCH race를 cleanup 도구에서 처리했다. 최초 실패 판정과 재확인 기록을 모두 보존했다.

**빌드/테스트 및 재현 자료**

- DPU transport build, 최적화 Rust release build, native `make test`, `dmesh-doca` 16개 unit tests, H2 pool regression test, Host Go tests가 통과했다.
- [전체 수치 JSON](/home/youngmin/DPUMesh/bench-results/2026-10-03_grpc-worker-local.json) · [CSV](/home/youngmin/DPUMesh/bench-results/2026-10-03_grpc-worker-local.csv) · [그래프](/home/youngmin/DPUMesh/bench-results/2026-10-03_grpc-worker-local.png)
- [측정 harness](/home/youngmin/DPUMesh/integrations/grpc/go/scripts/worker_backend_bench.py) · [집계](/home/youngmin/DPUMesh/integrations/grpc/go/scripts/summarize_worker_backend.py) · [실행 설명](/home/youngmin/DPUMesh/integrations/grpc/go/README.md)
- 원본 로그와 재현 명령 archive: `/home/youngmin/DPUMesh/bench-results/2026-10-03_grpc-worker-local-raw.tar.gz` (로컬 보관). 기존 Host 운영 경로는 격리 빌드와 별개다.
- 최종 batch: `post-timeout` 6회 + `post-timeout-rest` 9회 + `final-matrix` 48회. 초기 `full` batch와 pilot은 최종 통계에서 제외했다.

```text
763bfb586943f0bb16a6ade8016949d50aa173386b0075588db5467c4cb1cb3a  /home/youngmin/DPUMesh/linkerd2-proxy/target/release/linkerd2-proxy
5b056f4581e9c0954714920d6986166c3d9af71024b7e60057037f35c745bd05  /tmp/dmesh-worker-backend-host/integrations/grpc/go/bin/bench-client
4562f4605d67cf78b60347e143cff7ffc9059295b62417a060c1a79aff412a60  /tmp/dmesh-worker-backend-host/integrations/grpc/go/bin/bench-server
4586465510dcb4739f03cb2c342356ca35e982c63b33e4a74965f317b99aee46  /tmp/dmesh-worker-backend-host/build/lib/libdpumesh.so
```


**추가 확인: 이전 1-core 18K 결과와의 차이**

이전 `2026-09-30_grpc-go-single-process-flow-scaling`의 1 worker / 1 replica / 4 client flows / flow당 64 RPC 결과는 17,956.9 RPC/s였다. 이번 본문 결과 14,510.9는 19.2% 낮다. 이전에도 backend connection 하나를 공유했으므로 단순히 backend 공유로 바뀌었기 때문이라고 설명할 수 없다.

추가 통제 실험은 최종 바이너리를 그대로 사용하고 1 worker / 1 replica / 4 flows, 64B, flow당 64 RPC, warm-up 5초 + 측정 20초 × 3회로 실행했다. 각 조건은 새 proxy/server로 시작했다. Client GOMAXPROCS=8, CPU affinity와 나머지 설정은 동일하며 datapath 코드를 수정하지 않았다.

| 경로 | Server GOMAXPROCS | RPC/s 중앙값 | 반복 범위 | P99 ms |
|---|---:|---:|---:|---:|
| Service VIP | 4 | 14,118.9 | 14,064.9–14,228.5 | 23.02 |
| Service VIP | 8 | 14,421.7 | 14,371.9–14,514.5 | 22.65 |
| Replica 직접 전달 | 8 | 16,042.4 | 16,030.8–16,079.3 | 20.73 |

- 당시 실험은 replica endpoint를 직접 지정하는 Forward 경로였고, 이번 matrix는 Service VIP→discovery→Balancer 경로였다. 동일 GOMAXPROCS에서 VIP가 direct보다 10.1% 낮았다. Endpoint 하나여도 현 mock policy는 VIP에 Balancer(P2C/PeakEWMA)를 구성한다. 이 경로에는 endpoint 선택·부하 추적·응답 분류·metrics 등의 추가 계층이 있으며, 개별 계층의 비용은 분리 계측하지 않았다.
- GOMAXPROCS 4→8에서 중앙값은 2.1% 증가했다. 조건별 proxy 재기동이 있으므로 작은 차이를 전부 이 설정의 인과 효과라고 단정하지 않는다.
- Direct의 16,042.4도 과거 17,956.9보다 10.7% 낮다. 남은 차이는 아직 원인 미확정이다. 과거 바이너리와 현재 바이너리의 빌드 설정도 달랐다: 과거 release profile은 LTO=true/codegen-units=1, 해당 artifact fingerprint의 RUSTFLAGS는 `--cfg tokio_unstable`; 현재 최종 실행은 ThinLTO/codegen-units=16 및 target-cpu=native였다. Backend/driver 변경, audit 계측, Host library/harness, 실행 core 차이도 함께 있어 해당 잔여 하락을 어느 하나의 비용으로 확정할 수 없다.
- 9회 모두 RPC 오류 및 audit worker/thread mismatch 0건, scoped proxy/server 종료 성공. 기존 matrix와 이번 추가 실험의 수치를 섞어 집계하지 않았다.
- [추가 비교 JSON](/home/youngmin/DPUMesh/bench-results/2026-10-03_grpc-onecore-comparison.json). 원본 로그·실행 harness: `/tmp/dmesh-onecore-regression-20261003/`.
