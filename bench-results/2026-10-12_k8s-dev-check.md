# Kubernetes 배포 전제 확인 (jet2) — 2026-10-12

**판정:** [k8s 배포 설계](../docs/2026-10-08_k8s-deployment-plan.md) 검토에서 나온 전제 문제를 고쳤고, jet2와 jet2-dpu에서 Pod로 확인했다.
- 인프라가 재시작되면 앱이 종료 코드 75로 끝나고 kubelet이 다시 붙인다.
- broker가 caller의 주소와 mesh 신원을 확인한다.
- 앱 Pod는 non-root이고, Service ClusterIP로 다이얼하면 실제 Linkerd를 거쳐 DMA로 간다.

기능 확인이고 벤치마크가 아니다. RPC/s는 각각 한 번 돌린 값이다.

## 조건

| 항목 | 값 |
|---|---|
| Host | jet2, Xeon 6515P 32 CPU, Ubuntu 24.04, kernel 6.8.0-146, DOCA 3.5.0098 |
| DPU | jet2-dpu, BF-3, Ubuntu 22.04.5, kernel 5.15.0-1099-bluefield, DOCA 3.5.0098 |
| 함수 | PF1: DPU `03:00.1`, host `0b:00.1`, Comch `DPUMeshK8`. PF0에는 `~/DPUmesh` rig가 돌고 있어 건드리지 않았다 |
| DPU proxy | linkerd2-proxy(doca, release), worker 1, busy poll, DPU core 14. jukebox로는 representor를 열 수 없어서(`devlink` EPERM) privileged Pod 안에서 돌렸다 |
| control plane | 1–3절은 mock(`bench/grpc/dpu`, L7). 4절은 클러스터의 Linkerd edge-26.8.4 |
| 부하 | Go `channel-bench` 64 B unary echo, 1 conn × 64, warmup 1 s, 측정 5 s. 서버 host core 24–31, 클라이언트 16–23 |
| 코드 | `feature/grpc-perf` `5263d5b`, `feature/grpc-all` `4db1957`, `feature/doca-broker` `451c274`, `feature/k8s-deploy` `e103ab7`, proxy `fb4b3632`. 표의 값은 그 뒤 뺀 LISTEN 재시도가 들어 있던 빌드로 쟀다. 뺀 코드로 다시 돌린 결과는 [5절](#5-10-13-재확인) |

## 1. 직접 경로 (`feature/grpc-perf`)

| 확인 | 결과 |
|---|---|
| 기본 실행 | ok, 9,233 RPC/s, 오류 0 |
| proxy SIGTERM 중 부하 | 서버·클라이언트 모두 종료 코드 75, 5.5 s 안 |
| proxy SIGKILL 중 부하 | 서버·클라이언트 모두 75, 5.5 s 안. C++ 서버도 같다 |
| 서버 kill -9 직후 재시작 | 바로 LISTEN 성공 |
| 은퇴한 주소 (`DMESH_LISTENER_GRACE_MS=4000`) | 유예 안에서는 TCP 시도 없음(DMA pool이 기다림), 유예 뒤에는 TCP로 감(`connect timed out`) |

- 상실을 알아채는 데 약 5 s가 걸린다. host의 Comch client는 proxy가 끝나도 이벤트를 받지 못하고, 보내기만 실패한다. idle 세션은 1 s마다 PING을 보내므로, 나머지는 DPU 쪽에서 죽은 프로세스의 연결이 정리되는 시간이다.
  - probe로 확인했다. 부하 없이 SIGTERM이면 약 1 s다.
- L4(opaque) 정책에서는 backend가 아직 publish되지 않은 DMA 주소를 outbound가 바로 거절한다. 수정 전 코드도 같으므로 L7로 확인했다.

## 2. 언어 binding (`feature/grpc-all`)

| binding | 단위 테스트 | DMA 실행 (Go 클라이언트) | proxy SIGKILL |
|---|---|---|---|
| Go | `go test` 4 패키지 ok | 9,113 RPC/s | 75 |
| C++ | ctest 6/6 | ok | 75 |
| Java | gradle test ok | 8,786 RPC/s (병합 후 8,877) | 75 |
| .NET | 19/19 | 8,576 RPC/s | 75 |
| Node | 8/8 | 8,492 RPC/s | 75 |
| Python | 6/6 | 2,599 RPC/s (sync 서버, worker 10) | 75 |

언어 branch에는 코드 변경이 필요 없었다. 모든 binding이 같은 stream 라이브러리와 core를 거치고, 종료는 core가 한다.

## 3. broker (`feature/doca-broker`)

| 확인 | 결과 |
|---|---|
| broker를 거친 실행 | ok, 9,028 RPC/s, 정상 종료 때 "session lost" 로그 없음 |
| proxy SIGKILL | broker child가 `session_error`를 알리고 앱 둘 다 75, 5.3 s |
| broker 종료 | 앱 둘 다 75, 0.33 s |
| `--verify addr`, 자기 주소(10.200.0.2) | LISTEN·다이얼 ok |
| 남의 주소로 LISTEN(10.99.1.60) | broker가 EACCES로 거절 |
| 빌린 출발지(10.99.0.61) | CONN_OPEN을 EACCES로 거절 |
| `--client-mem-mib 64` | 두 번째 region(32 MiB)을 거절, 앱은 시작 실패 |
| `--max-clients 1` | 두 번째 client 거절 |

## 4. Kubernetes Pod (`feature/k8s-deploy`)

배치 (namespace `dpumesh-dev`, host 경로는 rig와 겹치지 않게 dev 전용):
- **이미지**: jet2의 `localhost:5000` 레지스트리.
  - node 이미지: ubuntu:24.04, broker, `/opt/dpumesh/lib`(libdpumesh와 host의 DOCA·rdma-core), mlx5 provider.
  - echo 이미지: ubuntu:24.04와 Go 바이너리. 라이브러리는 노드에서 받는다.
- **broker Pod**
  - privileged, `hostPID`, `--verify pod`.
  - init이 라이브러리를 `/var/lib/dpumesh-dev/lib`에 깐다.
  - 소켓은 `/run/dpumesh-dev`, kubelet pods 디렉터리는 `HostToContainer`로 마운트한다.
- **앱 Pod**
  - `runAsNonRoot`, `capabilities: drop ALL`, `seccompProfile: RuntimeDefault`.
  - 소켓과 라이브러리 경로를 읽기 전용 hostPath로 받는다. 서버 env는 `DPUMESH_PORT=9000`뿐이다.
- **DPU proxy Pod** (jet2-dpu)
  - hostNetwork, `ClusterFirstWithHostNet`, SA `dpu`, projected token(`identity.l5d.io`), trust roots ConfigMap, 키·CSR init.
  - rig와 같은 방식으로 실제 Linkerd에 붙였다.

| 확인 | 결과 |
|---|---|
| broker의 서버 확인 | `verify pod addrs 10.244.0.79 pod 8f2f143b-… workload echo.dpumesh-dev.serviceaccount.identity.linkerd.cluster.local` |
| 클라이언트 → Service `echo`(10.107.192.200:9000) | Linkerd destination이 `podIP:9000`을 골라 DMA로 감. ok, 7,789 RPC/s, 오류 0 |
| 서버 Pod IP로 LISTEN하는 다른 Pod | `create channel: permission denied` |
| 출발지 10.244.0.99로 다이얼하는 Pod | CONN_OPEN 거절, preflight 실패 |
| proxy SIGKILL | 서버 Pod가 6 s 뒤 75로 종료하고 kubelet이 재시작했다. proxy가 돌아온 뒤 Ready가 됐고, Service 다이얼도 다시 ok(7,723 RPC/s) |
| broker Pod 교체 | 서버가 75로 종료했다. 새 broker가 뜨기 전에 재시작된 시도는 실패(1)했고, 그 뒤 스스로 붙었다 |

## 발견해서 반영한 것

- broker가 kubelet pods 디렉터리를 propagation 없이 마운트하면, broker보다 늦게 뜬 Pod의 token tmpfs가 보이지 않는다. 그 Pod는 비인증이 된다. `mountPropagation: HostToContainer`가 필요하다.
- Linkerd identity의 CSR은 CN 64자 제한이 있다. SA `dpumesh-dpu`는 init이 실패했고, `dpu`로 바꿨다.
- 정상 종료한 client의 세션을 broker가 "lost"로 보고하던 문제를 고쳤다(`1527f24`).
- policy controller가 hostNetwork proxy의 inbound workload를 "unknown server"로 답한다. DMA authz gate는 기본 정책(`all-unauthenticated`)으로 판정했다. 앱 Pod의 정책으로 판정하는 것은 후속 과제다.
- 태그를 다시 쓰는 dev 이미지는 `imagePullPolicy: Always`가 아니면 캐시된 옛 이미지로 돈다.

## 5. 10-13 재확인

LISTEN 재시도를 뺀 코드(위 SHA)로 1–4절을 다시 돌렸다. 결과는 같다.
- 직접 경로: 9,713 RPC/s. proxy SIGTERM·SIGKILL에 서버·클라이언트 모두 75(5.4 s). 서버 `kill -9` 직후 재시작은 바로 LISTEN 성공. 유예 동작도 같다.
- 다른 서버가 주소를 LISTEN하는 동안 새 서버를 띄우면, 이제 바로 `create channel: address already in use`로 시작에 실패한다.
- broker: 9,593 RPC/s. proxy SIGKILL에 75(5.5 s), broker 종료에 75(0.33 s). squatting·출발지 위조·메모리·client 상한 거절도 같다.
- Pod: Service 다이얼 8,165 RPC/s. proxy SIGKILL 5 s 뒤 서버가 75로 종료했고, 재시작 뒤 다시 서빙했다(8,219 RPC/s). squatting·출발지 위조는 거절됐다.
