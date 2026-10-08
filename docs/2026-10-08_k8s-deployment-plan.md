# Kubernetes 배포 설계 — 2026-10-08

상태: **설계 초안 (2판, 멀티 노드 기준).**
- 기준 브랜치: `feature/doca-broker`(= grpc-all + host broker).
- 참고한 구현: `~/DPUmesh`(jukebox03/DPUmesh, main `3dd76966`). 노드 안의 Kubernetes 배포와 노드 간 peer carrier(RDMA, inline IPsec)가 있다.

## 1. 목표와 결정

DPUMesh를 쓰는 애플리케이션을 **컨테이너 이미지와 Kubernetes manifest로 배포**한다.
- 운영자는 노드 쌍(host + DPU)마다 준비 작업을 하고 `helm install`을 한 번 한다.
- 앱 배포자는 PodSpec 몇 줄만 쓴다.
- 같은 노드의 Pod끼리는 PCIe DMA로, 다른 노드의 Pod끼리는 DPU↔DPU RDMA로 통신한다. 앱은 둘을 구분하지 않는다.

**원칙: 설계는 처음부터 N개 노드 쌍을 기준으로 한다.** 구현은 노드 하나부터 시작하지만, 설정 모델·이름·신원·네트워크는 노드가 늘어도 바뀌지 않아야 한다.

정한 것:

| 질문 | 결정 |
|---|---|
| DPU(ARM)를 어떻게 관리하나 | 같은 클러스터의 **k8s 노드로 join**한다. DPU 쪽 프로세스는 DaemonSet 이미지로 배포한다. |
| 서비스 탐색·정책 | **기존 Linkerd control plane**(`linkerd install`)을 쓴다. 노드 간 경로에 필요한 배치·키 정보는 별도의 `dpumesh-controller`가 서명해 배포한다(4.3). |
| libdpumesh + DOCA 런타임 라이브러리 | **노드가 주입**한다. device plugin이 `/opt/dpumesh/lib`로 마운트한다. 앱 이미지에는 넣지 않는다. |
| 구현 순서 | 노드 하나로 k8s 경로를 먼저 완성하되, 멀티 노드 구조(inventory, controller, 신원)를 그때 함께 넣는다. 두 번째 노드는 2단계에서 붙인다. |

## 2. 설계를 정하는 사실

### 2.1 노드 안 (이 repo)

- **DPU proxy는 host PF당 프로세스 하나다.** `linkerd2-proxy`(doca feature)가 (DPU device, host PF representor) 쌍을 잡고, 그 PF 위의 모든 host 프로세스를 서비스한다. 같은 쌍을 두 프로세스가 열 수 없다.
- **DMA로 받은 연결은 Linkerd outbound 스택을 그대로 탄다**(`linkerd2-proxy/src/main.rs`의 `spawn_dmesh`).
  - 설정은 표준 `Config::try_from_env()`라서 실제 control plane을 가리킬 수 있다.
  - endpoint가 DMA인지는 `backend::is_dma()`가 정한다. dispatcher에 LISTEN된 `ip:port`면 DMA로 보내고, 아니면 DPU에서 TCP로 보낸다.
  - `DMESH_ROUTES` manifest는 선택 사항이다.
- **Linkerd identity는 끌 수 없다**(`linkerd/app/src/env/identity.rs:158-162`). DPU proxy도 SA token으로 인증서를 받아야 한다.
- **fork의 upstream 기준은 2026-06 무렵이다**(`linkerd2-proxy-api 0.20.0`). `~/DPUmesh`는 edge-26.8.4로 검증했다.
- **host broker는 PF당 데몬 하나다.** `dpumesh_broker --listen <sock> --pci <addr>`가 DOCA device를 소유하고, 앱 연결마다 fork한다.
  - 앱에는 소켓만 있으면 된다(`src/transport/host/channel.c:524-545`).
  - 인증은 없다. `SO_PEERCRED`를 로그로만 남긴다.
- **앱은 DOCA 라이브러리를 로드한다.** `libdpumesh.so.5`가 doca-common, comch, dma, dpa와 flexio를 직접 링크한다(`Makefile:15`).
- **주소는 k8s 그대로 쓴다.**
  - 클라이언트는 `svc:port`를 DNS로 풀어 ClusterIP로 보낸다(IPv4만).
  - 서버 replica는 각자 `podIP:port`로 LISTEN한다. 같은 키를 두 번 LISTEN하면 EADDRINUSE가 난다.
- **Pod 안에서는 필수 env가 없다.**
  - 라이브러리가 `KUBERNETES_SERVICE_HOST` 쪽 경로의 출발지 주소로 Pod IP를 찾는다(`src/core/carrier.c`의 `pod_ip_lookup`). `DPUMESH_POD_IP`는 Pod 밖에서만 필요하다.
  - 서버는 `DPUMESH_PORT`(targetPort)만 주면 `<pod ip>:<port>`로 LISTEN한다. `DPUMESH_SERVICE`는 다른 주소를 서빙할 때만 쓴다.
  - `DPUMESH_SERVER`의 기본값은 `DPUMesh0`이다.
- **용량**
  - channel(프로세스) 하나당 flow 32개.
  - DPU worker 하나당 flow 64개, Comch alias 하나당 session 64개.
  - 채널 하나당 등록 메모리 약 96 MiB. broker가 할당한다.
  - dpu-dma 경로는 칩당 28개인 DPA process 한도와 무관하다.
- **장비에서 아직 함께 돌려 보지 않은 것**
  - broker + linkerd2-proxy 조합.
  - broker를 거친 backend 경로.
  - 실제 Linkerd.
  - 컨테이너 실행.

### 2.2 노드 간 (`~/DPUmesh` peer carrier)

- **모드가 둘 있다**(`doca/dpu_worker.c:1186-1283`).
  - **v1**: node pair·worker마다 TLS 1.3 채널을 쓴다. 소프트웨어 암호이고, TCP나 RDMA-CM 위에서 돈다.
  - **v2 `ipsec-pod-pair`**: Pod 쌍마다 association을 만든다. RoCEv2 RC SEND/RECV를 NIC inline ESP(AES-128-GCM)로 보호하고, 별도의 crypto owner 프로세스가 필요하다.
  - 장비에서 두 DPU 사이로 돌려 본 것은 v2 probe뿐이다(Kubernetes·host·Pod 없이, worker 1). 측정치는 RTT +6–7 µs, 처리량 −5~13%다. v1은 두 노드에서 돌려 본 적이 없다.
- **데이터 흐름**
  - source DPU가 Linkerd로 endpoint를 고른다. 그 Pod가 다른 노드에 있으면 peer carrier로 `STREAM_OPEN`을 보낸다.
  - destination DPU는 다음을 확인한 뒤, 바이트를 DPU arena에 받아 대상 Pod의 등록 메모리로 DMA한다.
    - 등록이 살아 있는지
    - source Pod가 그 peer 노드에 있는지(`pod_on_node`)
    - source Pod가 주장한 Service의 endpoint인지
    - inbound 정책
  - 응답은 같은 handle로 돌아온다.
  - 채널은 처음 필요할 때 열리고, 60 s 동안 쓰지 않으면 닫힌다. full mesh가 아니다.
- **배치와 신원의 출처는 서명된 topology다.**
  - 문서 형식은 `node=<host노드>,<fabric ip:port>,<DPU pubkey>`, `pod=<uid>,<node>,<ns>,<sa>,<ip>`, `service=`, `endpoint=`다.
  - Pod 신원 단위는 **Pod UID**이고, 노드 이름은 **host 노드 이름**이다.
  - DPU끼리의 인증은 이 topology의 `node=` 키에 고정(pin)한 Ed25519 키로 한다.
- **DPU별 준비물**
  - Ed25519 node key: 처음 시작할 때 만들고, 노드 사이에 복사하지 않는다.
  - fabric 네트워크: p0 위 SF netdev, IPv4, RoCEv2 IPv4-mapped GID, MTU.
  - v2에서는 crypto owner가 runtime보다 먼저 떠 있어야 한다.
- **모든 노드에서 같아야 하는 값**: 바이너리 버전, cluster ID, ARM worker 수, peer MTU, peer port, 보안 모드.
  - 모든 wire 버전이 정확히 일치해야 하고 협상이 없다. **rolling upgrade는 불가능하다.**
- **현재 carrier의 제약** (코드에서 확인, 검증되지 않음)
  - Service endpoint가 아닌 client 전용 Pod는 노드 간 stream을 시작할 수 없다.
  - v2는 destination에 Linkerd inbound 정책의 **명시적 ADMIT**을 요구한다. 정책이 없으면 거절한다.
  - 연결 하나는 remote 목적지 하나만 가질 수 있다.
- **이 repo로 옮겨야 한다.** carrier는 `~/DPUmesh`의 DPU runtime(`dpu_proxy.c` + embedded Linkerd)에 붙어 있다. 이 repo의 DPU data plane(linkerd2-proxy fork + dispatcher)과 다르므로 포팅이 필요하다.
  - 배포 설계는 carrier가 요구하는 입력을 제공하는 쪽만 정한다(4장).
  - connector는 세 갈래가 된다: 로컬 LISTEN이면 DMA, topology상 다른 DPUMesh 노드의 Pod면 peer, 그 밖은 TCP.

## 3. 구조

```text
                    ┌────────────── Kubernetes control plane (jet2) ──────────────┐
                    │ API server · Linkerd (identity/destination/policy)           │
                    │ dpumesh-controller: Pod/Service/EndpointSlice/inventory      │
                    │   → 서명된 topology, DPU 공개키 보관                          │
                    └──────────▲──────────────────────────────▲───────────────────┘
                               │ 관리망 10.200.0.0/24          │
 host jet1 ─────────────── DPU jet1-dpu                  DPU jet2-dpu ─────────────── host jet2
  앱 Pod (channel: 1)        dpumesh-dpu                  dpumesh-dpu                 앱 Pod
  dpumesh-node               ├ crypto-owner (sidecar)     ├ crypto-owner              dpumesh-node
  ├ broker ◄─ Comch/DMA ──►  ├ topology-agent (sidecar)   ├ topology-agent  ◄─ Comch/DMA ─► ├ broker
  ├ device-plugin  (PCIe)    └ proxy  ◄═══ fabric: RoCEv2 + IPsec (p0 SF) ═══►  └ proxy   ├ device-plugin
  └ install-libs                                                                         └ install-libs
```

### 네트워크는 세 평면이다

| 평면 | 경로 | 용도 |
|---|---|---|
| PCIe | host PF(`0b:00.1`) ↔ 같은 노드의 DPU | Comch control + DMA 데이터. 노드 안 통신 |
| 관리 (k8s) | `10.200.0.0/24`, p0 | kubelet, API server, Linkerd, controller, flannel. 앱의 일반 TCP |
| fabric | p0 위 DPU SF, 별도 subnet(예: `10.77.0.0/24`) | DPU↔DPU peer carrier. TCP control(base port) + RoCEv2(ESP) |

- tmfifo(`192.168.100.0/30`)는 host마다 같은 주소를 쓰고 노드 사이에서 라우팅되지 않는다. 그래서 **어떤 설정에도 tmfifo 주소를 쓰지 않는다.** 콘솔·복구용으로만 남긴다.
  - `~/DPUmesh`의 `DPUMESH_NODE_RDMA_ADDR=192.168.100.2:47900`은 단일 노드용 자리값이었다.
- DPU의 k8s 노드 주소는 관리망 `10.200.0.0/24` 안에 둔다. 지금 flannel이 host-gw backend라서, 노드끼리 같은 L2에 있어야 Pod 라우트가 잡힌다.
- `~/DPUmesh`는 DPU와 통신할 때 host를 거쳤다(feed relay, scope tunnel, paired TLS). 여기서는 **DPU가 클러스터 네트워크로 controller와 Linkerd에 직접 붙는다.** 그러면 host 쪽 relay 데몬이 필요 없다.

## 4. 클러스터 구성 모델

### 4.1 노드 쌍 inventory

노드별 값은 helm values의 `nodes:` 목록 하나에만 둔다. 이 목록은 ConfigMap `dpumesh-inventory`로 렌더링된다.
- DaemonSet Pod는 downward API의 `spec.nodeName`으로 자기 행을 찾는다.
- controller는 이 ConfigMap을 API로 읽는다. 마운트하지 않으므로 수정이 바로 반영된다.
- `~/DPUmesh`처럼 노드 쌍마다 helm release를 따로 두지 않는다. **release는 클러스터에 하나**다.

```yaml
cluster:
  id: jet                       # 모든 DPU가 HELLO에서 비교
  version: 0.1.0                # 이미지 태그. 노드 전체 lockstep
  armWorkers: 8                 # 모든 노드 동일 (peer lane 수, v1 port 매핑)
  peer: {enabled: false, transport: rdma, security: ipsec-pod-pair, port: 47900, mtu: 1024}
nodes:
- host: jet1
  dpu: jet1-dpu
  hostPF: "0b:00.1"             # broker --pci, proxy REP_PCI_ADDR
  dpuDev: "03:00.1"             # proxy DEV_PCI_ADDR
  fabric: {pf: "0000:03:00.0", netdev: enp3s0f0s0, rep: en3f0pf0sf0, rdmaDevice: mlx5_2, ip: 10.77.0.1/24}
- host: jet2
  dpu: jet2-dpu
  ...
```

- GID index는 적지 않는다. DPU init 컨테이너가 `/sys/class/infiniband/<dev>/ports/1/gids`에서 fabric IP의 IPv4-mapped RoCEv2 GID를 찾아 넣는다. `~/DPUmesh`에서는 기본값 0이 대개 틀렸다.
- 노드 label도 이 목록에서 나온다. 설치 스크립트가 붙인다.
  - host: `dpumesh.io/role=host`
  - DPU: `dpumesh.io/role=dpu`, taint `dpumesh.io/dpu=true:NoSchedule`

### 4.2 노드 공통 조건

설치할 때(helm schema)와 실행할 때 모두 검사한다. 하나라도 다르면 그 노드는 peer에 참여하지 않는다.
- 같은 이미지 버전: host 라이브러리, DPU proxy, DPA kernel, crypto owner.
- 같은 DOCA 버전: DPU BFB와 이미지 안의 DOCA. crypto owner IPC의 HELLO가 DOCA 버전을 싣는다.
- 같은 `cluster.id`, `armWorkers`, `peer.*`.

### 4.3 신원과 신뢰

| 무엇 | 만드는 곳 | 보관 | 쓰는 곳 |
|---|---|---|---|
| DPU node key (Ed25519) | `dpumesh-dpu`가 처음 시작할 때 | DPU hostPath `/etc/dpumesh/node-static.key` (0400) | peer TLS. 공개키는 controller로 간다 |
| controller 서명 키 | helm 설치 hook Job | Secret. DPU에는 공개키만 ConfigMap으로 | topology 서명·검증 |
| DPU 공개키 목록 | topology-agent가 보고 | ConfigMap `dpumesh-node-keys` | topology `node=` 행 |
| Linkerd 인증서 | Linkerd identity (SA token) | DPU Pod memory | proxy ↔ Linkerd control plane |
| Pod 신원 (UID) | broker가 커널 증거로 확인 | DPU 등록 정보 | `pod_on_node`, inbound 정책, 노드 간 claim |

- **공개키 보고**: topology-agent가 자기 SA token을 붙여 controller에 POST한다.
  - controller는 TokenReview로 호출자가 `dpumesh-dpu` Pod인지와 어느 DPU 노드에 있는지 확인한다. 그 DPU 노드를 inventory로 host 노드에 매핑해 키를 받는다.
  - `~/DPUmesh`의 node CA, CSR, mTLS 경로가 필요 없다.
- **키는 ConfigMap에 보관한다.** `~/DPUmesh`는 보고된 키를 메모리에만 두었다. 그래서 controller가 재시작하면 모든 노드의 키가 0으로 발행되고, 모든 DPU의 peer 채널이 초기화될 위험이 있었다.
- **topology 배포**: topology-agent가 controller에서 topology를 받아 hostPath `/etc/dpumesh/feeds/`에 원자적으로 쓴다(write, fsync, rename). runtime은 1 s마다 읽고 서명과 rollback을 검사한다.
- **Pod 신원**
  - broker가 `SO_PEERCRED` → `/proc/<pid>/cgroup`에서 Pod UID와 container ID를 얻는다. 그래서 broker 컨테이너는 `hostPID`가 필요하다.
  - 얻은 신원은 DPU 등록에 싣는다. 노드 간 경로는 Pod UID 없이는 동작하지 않으므로, **1단계부터 구조에 넣는다.**
  - 봉인(`~/DPUmesh` v6의 sealed assertion)은 3단계에서 한다. 그 전까지 DPU는 host broker를 믿는다.

## 5. 컴포넌트

### 5.1 DPU 노드 준비 (노드 쌍마다, 운영자)

`packaging/prepare-dpu-node.sh` 같은 스크립트로 처리하고, 결과는 DPU에 영구 설정으로 남긴다.
1. **BFB·DOCA 버전 확인**: 클러스터 공통 버전과 맞춘다.
2. **관리망**: `10.200.0.0/24` 주소를 준다(OVS에 걸린 SF나 internal port. OOB가 같은 L2에 닿으면 OOB도 가능). API server에 닿는지 확인한다.
3. **fabric**
   - p0 위에 SF를 만들고 IPv4를 준다. MTU를 맞추고 RoCEv2 GID가 생겼는지 확인한다.
   - eswitch와 flow steering 모드는 crypto owner(DOCA Flow HWS)가 요구하는 값으로 둔다. `~/DPUmesh`에 문서화되어 있지 않으므로 0단계에서 확인한다.
   - 이웃 DPU와 `rping`, `ib_send_bw`가 되는지 본다.
4. **kubelet**
   - kubelet과 kubeadm(v1.34.11, arm64)을 설치하고 `kubeadm join`한다. join은 사람이 짧은 token으로 한다.
   - `reservedSystemCPUs`와 eviction 임계값을 정한다. eMMC가 작다.
   - CPU 계획(BF-3 16코어 기준 지침):
     - 시스템과 kubelet 2코어
     - crypto owner 1코어
     - dispatcher·peer manager 1코어
     - 나머지를 worker로 (`armWorkers` ≤ 12)
5. **영구 디렉터리**: `/etc/dpumesh`(node key, feeds).

DPA EU partition은 dpu-dma 경로에 필요 없다.

### 5.2 `dpumesh-dpu` DaemonSet (DPU 노드 전부)

- 대상: `nodeSelector dpumesh.io/role=dpu`, taint 허용.
- 권한: `privileged`, `hostNetwork`, `dnsPolicy: ClusterFirstWithHostNet`. RDMA, DOCA Flow, Comch가 모두 host 네트워크·장치 맥락에서 돈다.
- 업데이트: `updateStrategy: OnDelete`(8장).
- 컨테이너

  | 이름 | 종류 | 역할 |
  |---|---|---|
  | `fabric-check` | init | inventory 자기 행 확인, fabric netdev·GID·MTU 검사, GID index 찾기. 실패하면 시작하지 않는다 |
  | `linkerd-identity` | init | Linkerd 키와 CSR 생성(`~/DPUmesh/packaging/linkerd-init.sh` 이식) |
  | `crypto-owner` | native sidecar (`restartPolicy: Always` init) | `dpumesh_crypto_owner --socket /run/dpumesh-peer/crypto.sock --pci <fabric.pf> --sf-iface <fabric.rep> --ovs-guard ovsbr1`. peer v2일 때만 켠다 |
  | `topology-agent` | native sidecar | 공개키 보고, topology 수신, feeds 기록 |
  | `proxy` | main | `linkerd2-proxy`(doca, release) + peer carrier |

  - native sidecar는 main보다 먼저 시작하고 나중에 끝난다. crypto owner를 runtime보다 먼저 띄우고 나중에 내려야 한다는 순서 조건을 k8s 기본 기능으로 맞춘다.
  - crypto owner 소켓은 mode 0600이므로 같은 uid(root)로 돌리고 emptyDir로 공유한다.
- `proxy` env

  ```text
  LINKERD2_PROXY_DOCA_DEV_PCI_ADDR / REP_PCI_ADDR / SERVER_NAME   # inventory 자기 행
  DMESH_NUM_WORKERS=<armWorkers>  DMESH_SHARDED=1  DMESH_BUSY_POLL=1
  LINKERD2_PROXY_{IDENTITY,DESTINATION,POLICY}_*                  # linkerd inject 결과에서 복사
  DPUMESH_CLUSTER_ID  DPUMESH_NODE_NAME=<host 노드>              # topology의 node 이름은 host 기준
  DPUMESH_NODE_KEY_FILE=/etc/dpumesh/node-static.key  DPUMESH_TOPOLOGY_FILE  DPUMESH_CONTROLLER_KEY_DIR
  DPUMESH_PEER_{TRANSPORT,SECURITY,BIND,PORT,MTU,RDMA_DEVICE,GID_INDEX}
  DPUMESH_PEER_CRYPTO=unix:/run/dpumesh-peer/crypto.sock
  ```

  peer 변수 이름은 `~/DPUmesh` 기준이다. 포팅하면서 바뀌면 따라간다.
- readiness: admin `:4191/ready`. peer가 끊겨도 Ready는 유지한다. 노드 안 통신은 계속되어야 하기 때문이다.
- 볼륨
  - hostPath `/etc/dpumesh`: node key, feeds. 재시작해도 남아야 한다.
  - ConfigMap `dpumesh-controller-pub`: controller 공개키, `DPUMESH_CONTROLLER_KEY_DIR`. 교체용으로 키를 최대 4개 둔다.
  - emptyDir `/run/dpumesh-peer`: crypto owner 소켓.

### 5.3 `dpumesh-node` DaemonSet (host 노드 전부)

- 대상: `nodeSelector dpumesh.io/role=host`. 업데이트: `OnDelete`.
- **init `install-libs`**: `/opt/dpumesh/lib`를 host `/var/lib/dpumesh/lib/<version>/`로 복사한다.
- **`broker`**
  - 권한: `privileged`, **`hostPID`**(Pod 신원 확인).
  - hostPath: `/run/dpumesh`.
  - 실행: `ulimit -l unlimited && exec dpumesh_broker --listen /run/dpumesh/broker.sock --pci <hostPF>`.
  - 메모리 limit: `채널 수 × ~96 MiB` 이상.
  - 컨테이너가 재시작되면 노드의 모든 앱 채널이 끊긴다.
- **`device-plugin`**: 리소스 `dpumesh.io/channel`, 장치 개수는 기본 16이다.
  - `Allocate` 응답:

    ```text
    mounts: /run/dpumesh               -> /run/dpumesh     (ro)
            /var/lib/dpumesh/lib/<ver> -> /opt/dpumesh/lib (ro)
    envs:   DPUMESH_ENABLE=1  DPUMESH_BROKER=/run/dpumesh/broker.sock
            DPUMESH_SERVER=DPUMesh0      DPUMESH_STREAM_LIBRARY=/opt/dpumesh/lib/libdpumesh_stream.so
    ```

  - health: broker가 accept하고, 짝 DPU(inventory)의 `dpumesh-dpu` Pod가 Ready일 때만 Healthy다. peer 상태는 health에 넣지 않는다.
  - device plugin 마운트는 PodSpec에 나타나지 않는다. 그래서 앱 namespace를 PSA `restricted`로 둘 수 있다.
  - 구현은 `~/DPUmesh/node/dpumeshd.py`의 device plugin 부분에서 시작한다.

### 5.4 `dpumesh-controller` Deployment

`~/DPUmesh/controller/dpumesh_controller.py`를 이식해 고친다.
- **입력**
  - Pod, Service, EndpointSlice. 지금은 5 s polling이고, watch로 바꾸는 것은 선택이다.
  - `dpumesh-inventory`, `dpumesh-node-keys`. nodes file과 subPath 마운트는 쓰지 않는다.
- **출력**
  - 서명된 topology(`node=`, `pod=`, `service=`, `endpoint=`).
  - destination DPU가 확인할 workload scope.
  - HTTPS로 제공하고, 호출자는 SA token으로 인증한다.
- **배치**: host 노드, replica 1. 꺼져 있으면 마지막으로 검증된 topology로 동작한다(fail-static).
  - 그동안 새로 뜬 Pod는 노드 간 endpoint가 되지 못한다. 노드 안 DMA는 영향받지 않는다.
- **`pod=` 행에는 DPUMesh Pod만 넣는다**(`dpumesh.io/channel` 요청 Pod). 노드 이름은 Pod의 `spec.nodeName`(host)이다.
- 1단계는 peer를 끈 채 controller와 topology-agent만 돌린다. 키 보고와 topology 도착까지 확인한다.

### 5.5 Linkerd control plane

- `linkerd install --crds`, `linkerd install`. 버전은 edge-26.6 – 26.8이고, control plane Pod는 host 노드에 둔다.
- DPU proxy가 받는 것:
  - identity: SA `dpumesh-dpu.dpumesh-system`. 모든 DPU가 같은 SA다. DPU끼리의 구분은 node key로 한다.
  - destination: Service의 모든 ready endpoint. 다른 노드 endpoint도 포함한다.
  - policy: outbound 정책(HTTPRoute, GRPCRoute).
- 노드 간 요청의 **부하 분산은 source DPU의 Linkerd가 한다.** destination DPU는 inbound 정책 판정만 한다.
  - 같은 노드 endpoint를 선호할지는 Linkerd의 지연 기반 P2C에 맡긴다. 필요하면 topology-aware routing을 검토한다.
- **v2 inbound 정책**: carrier가 명시적 ADMIT을 요구한다. 그래서 다음 둘 중 하나로 사용자 부담을 없앤다.
  - (a) controller가 DPUMesh Service마다 기본 `Server`와 `AuthorizationPolicy`를 만든다.
  - (b) carrier가 Linkerd의 클러스터 기본 정책을 ADMIT으로 해석하게 고친다.
- server-first 프로토콜은 Service에 `config.linkerd.io/opaque-ports`를 단다.

### 5.6 주입 라이브러리 레이아웃

- `/opt/dpumesh/lib`에 들어가는 것:
  - `libdpumesh.so.5`, `libdpumesh_preload.so`, `libdpumesh_stream.so`.
  - DOCA(common, comch, dma, dpa), flexio, rdma-core 의존 라이브러리. glibc는 넣지 않는다.
- 모두 `RUNPATH=$ORIGIN`으로 두고, SDK 링크 플래그는 `-Wl,-rpath,/opt/dpumesh/lib`를 넣는다. 그러면 `LD_LIBRARY_PATH`가 필요 없다.
- Ubuntu 22.04(glibc 2.35)에서 빌드한다. 앱 이미지는 glibc ≥ 2.35여야 하고, Alpine과 static 바이너리는 지원하지 않는다.

### 5.7 이미지, 레지스트리, 버전

| 이미지 | arch | 내용 |
|---|---|---|
| `dpumesh/node:<v>` | amd64 | broker, device plugin, `/opt/dpumesh/lib` payload |
| `dpumesh/dpu:<v>` | arm64 | proxy(doca, release, peer carrier), crypto owner, topology-agent, init 스크립트, DOCA 런타임(+ DOCA Flow, DPDK) |
| `dpumesh/controller:<v>` | multi | controller |
| `dpumesh/sdk:<v>` | amd64 | 헤더, 링크용 `.so`, `dpumesh.pc`, Go 모듈, grpcio wheel, jar, .NET dll, Node addon |

- 모든 이미지와 helm chart를 `cluster.version` 하나로 묶는다.
- 앱 이미지는 공개 ABI에만 의존하므로 transport가 바뀌어도 다시 빌드하지 않는다.
- 레지스트리는 클러스터 안 `registry:2`(jet2, `10.200.0.2:5000`)이고, host 2대와 DPU 2대의 containerd에 등록한다.
- arm64 이미지는 DPU에서 네이티브로 빌드한다.

## 6. 사용자 계약

### 6.1 앱 개발자

| 통합 방식 | 빌드 | 코드 변경 |
|---|---|---|
| preload (Go가 아닌 libc 앱) | 없음(glibc 이미지면) | 없음. env `LD_PRELOAD=/opt/dpumesh/lib/libdpumesh_preload.so`, 클라이언트 `DPUMESH_TARGETS` |
| Go gRPC | cgo, SDK의 `dmeshgo` 모듈 | `dmeshgrpc.Listen`, `DialOptions()`, 종료 시 `CloseTransport()` |
| C++ gRPC | SDK 안에서 gRPC 1.80.0과 빌드 | `DmeshRuntime`, `CreateDmeshChannel`, `AttachDmeshGrpcServer` |
| Python gRPC | SDK의 grpcio 1.80.0 wheel + `dpumesh-grpc` | `insecure_channel`, `serve` (sync만) |
| Java gRPC | `dpumesh-grpc.jar` + grpc-netty(unshaded) | `forTarget`/`forPort`면 없음 |
| .NET gRPC | `Dpumesh.Grpc.dll` | `UseDpumesh()`, `DpumeshHttpHandler` |
| Node gRPC | addon | 서버만 `serve()`. 클라이언트는 preload |
| native C | `-ldpumesh` | channel/EQ/QP API |

공통 요구:
- glibc(≥ 2.35) 이미지를 쓴다.
- 컨테이너당 DPUMesh 프로세스는 하나다.
- SIGTERM을 받으면 채널을 닫고 끝낸다.
- 서버의 health 포트는 LISTEN이 끝난 뒤에 연다.

**노드 간 통신은 앱에 보이지 않는다.** 같은 코드, 같은 이미지가 어느 노드에서나 돈다.

### 6.2 배포 담당자

```yaml
apiVersion: v1
kind: Service
metadata: {name: echo}
spec:
  selector: {app: echo}
  ports: [{port: 8080, targetPort: 8080}]
---
# Deployment의 Pod template
containers:
- name: app
  image: 10.200.0.2:5000/echo-server:v1
  env:
  - {name: DPUMESH_PORT, value: "8080"}   # 서버만. Service의 targetPort
  ports: [{containerPort: 8080}, {containerPort: 8081, name: health}]
  readinessProbe: {grpc: {port: 8081}}
  resources:
    limits: {dpumesh.io/channel: 1}
```

- 클라이언트 Pod에는 env가 필요 없다. `limits` 한 줄만 단다.
- 서버의 `DPUMESH_PORT`는 Service의 `port`가 아니라 `targetPort`다. Linkerd가 고르는 endpoint가 `podIP:targetPort`이고, DPU는 그 주소가 LISTEN되어 있을 때만 DMA로 보낸다.
- 클라이언트는 `echo:8080`으로 다이얼한다. replica가 어느 노드에 있든 같다. 노드 배치에는 제약을 두지 않는다. `dpumesh.io/channel` 리소스가 있는 노드면 어디든 된다.
- namespace에 Linkerd injection을 켜지 않는다.
- **노드 간 경로 때문에 추가로 요구될 수 있는 것** (carrier 제약, 2.2):
  - client 전용 Pod도 어떤 Service에 선택되어야 한다.
  - v2에서는 Linkerd `Server`와 `AuthorizationPolicy`가 필요하다.

  둘 다 사용자에게 넘기지 않고, carrier 수정이나 controller 자동 생성으로 없애는 것이 목표다(10장 2-3). 끝내 남으면 이 계약에 추가한다.

## 7. 자동화 경계

| 무엇 | 누가/어떻게 |
|---|---|
| BFB·DOCA 버전, DPU 계정, host sudo 작업 | 운영자, 수동 (노드 쌍마다) |
| DPU 관리망·fabric(SF, IP, MTU) 설정, kubelet 설치, `kubeadm join` | 운영자, 준비 스크립트 + 수동 join (노드 쌍마다) |
| inventory 행 추가 (`nodes:`) | 운영자, values 수정 + `helm upgrade` |
| 레지스트리, Linkerd, controller, `dpumesh-node`, `dpumesh-dpu`, 서명 키, 노드 label | `helm install` 한 번 (+ 설치 스크립트) |
| GID index, node key 생성·보고, topology 배포, Pod 신원 확인, 라이브러리·소켓 주입, DPU 상태에 따른 배치 차단, endpoint 변화 반영, 노드 간 경로 선택 | 자동 |
| 통합 방식 선택, SDK 빌드, 종료 처리, health 포트 | 앱 개발자 |
| 리소스 한 줄, 서버 `DPUMESH_PORT`, Service | 배포 담당자 |

**노드를 하나 더 붙이는 절차**:
1. DPU를 준비한다(5.1).
2. `nodes:`에 한 행을 추가하고 `helm upgrade`한다.
3. 나머지는 자동이다. DaemonSet이 새 노드에 뜨고, node key가 보고되고, topology에 `node=`가 추가되고, 기존 DPU들이 새 노드를 알게 된다.

서버의 `DPUMESH_PORT`까지 없애려면 어댑터가 앱 코드의 포트를 넘겨야 한다. 그러려면 채널을 연 뒤에 LISTEN을 등록하는 API가 필요하고, C++·Node·.NET은 앱 코드가 포트를 넘기도록 API가 바뀐다. 지금은 env 한 줄로 둔다. webhook은 두지 않는다.

## 8. 장애와 업그레이드

| 사건 | 영향 |
|---|---|
| broker 재시작 | 그 노드 앱의 채널이 모두 끊긴다. 앱이 종료하고 kubelet이 재시작해야 다시 붙는다. 어댑터가 스스로 끝내는지 확인한다(작업 1-8). |
| DPU proxy 재시작 | 그 노드 앱의 채널이 끊기고, **다른 노드에서 이 노드로 오는 stream도 끊긴다**(peer 채널 fault). 상대 DPU는 새 incarnation으로 다시 연다. |
| crypto owner 재시작 | 그 DPU의 모든 v2 association이 revoke된다. 노드 안 통신은 계속된다. SA id는 owner가 살아 있는 동안 재사용되지 않으므로, `--sa-limit`를 다 쓰면 재시작이 필요하다. |
| controller 중단 | topology가 마지막 버전으로 고정된다. 새 Pod는 노드 간 endpoint가 되지 못한다. 노드 안 통신과 기존 stream은 계속된다. |
| node key 분실(DPU 재설치) | 새 키가 보고되어 topology가 갱신될 때까지 그 노드는 peer에서 빠진다. |
| 앱 비정상 종료 | broker child가 flow를 닫는다(검증됨). 노드 간 stream은 `POD_GONE`이나 채널 idle로 정리된다. |

- **업그레이드는 클러스터 전체를 한 번에 한다.** peer wire에 버전 협상이 없어서, 버전이 섞이면 노드 간 통신이 깨진다. 절차:
  1. 이미지를 올린다.
  2. 모든 `dpumesh-dpu`와 `dpumesh-node` Pod를 지운다(OnDelete).
  3. 앱이 재시작한다.
- 버전이 섞인 상태를 결정적으로 막기 위해, controller는 `cluster.version`이 다른 DPU를 topology에서 뺀다. 이를 위해 topology-agent가 자기 버전을 함께 보고한다.
- **알려진 위험**: Rust DPU 프로세스의 CLIENT teardown segfault. dpu-dma에서는 이후 재현되지 않았다(작업 0-2에서 확인). 노드 간 경로가 생기면 이 프로세스가 죽을 때의 영향이 다른 노드까지 퍼진다.

## 9. 범위 밖 (이번 설계)

- dual-listen(DMA와 TCP 동시)과 외부 ingress. DPUMesh가 아닌 클라이언트나 NodePort에서 DMA 전용 서버로 가는 경로가 없다. 3단계에서 다룬다.
- workload mTLS (`~/DPUmesh` PLAN §11), 등록 봉인, 다중 Pod 쌍 격리 검증, live rekey.
- host-dpa 경로. broker가 지원하지 않는다.
- IPv6. 프로세스 하나가 여러 포트를 서비스하는 것.
- RoCE lossless 설정(PFC/ECN). `~/DPUmesh`도 다루지 않았다. 부하 실험에서 손실이 보이면 다시 본다.

## 10. 작업

### 0단계: bare metal (jet1, jet2와 각 DPU, k8s 없이)

1. jet1에서 broker + linkerd2-proxy(mock) + Go gRPC echo 서버/클라이언트를 모두 broker를 거쳐 실행한다.
2. proxy를 띄운 채 앱을 100회 재시작한다. proxy 생존과 broker child 정리를 확인한다.
3. broker를 privileged·hostPID 컨테이너로, 앱을 비특권 컨테이너로 실행한다. 5.6 레이아웃과 broker의 Pod 신원 확인을 함께 검증한다.
4. host에 DOCA 패키지가 없는 jet2에서 3을 반복한다. host 준비물을 정한다.
5. **fabric**: 두 DPU에 SF, IP, GID, MTU를 설정한다. `rping`과 `ib_send_bw`를 돌리고, `~/DPUmesh`의 `peer_e2e_probe`와 crypto owner를 이 쌍에서 다시 돌린다. eswitch와 flow steering 요구값을 기록한다.

### 1단계: 노드 하나, 멀티 노드 구조

1. jet1의 DPU를 관리망에 연결해 join한다(label, taint).
2. 레지스트리와 빌더를 준비한다.
3. Linkerd를 설치하고, DPU proxy Pod가 identity, destination, policy와 붙는지 확인한다.
4. 이미지 빌드 스크립트를 만든다. `make install`, `dpumesh.pc`, RUNPATH가 포함된다.
5. helm chart를 만든다. inventory는 1행이고, `dpumesh-node`, `dpumesh-dpu`(peer off), `dpumesh-controller`, 설치 hook(서명 키), RBAC, device plugin이 들어간다.
6. broker가 Pod UID를 DPU 등록에 싣는다(코드).
7. Go gRPC echo(서버 2 + 클라이언트)를 배포한다. 분산, scale, Pod 삭제 후 복구, topology에 `pod=`·`node=`가 나타나는지 확인한다.
8. broker 또는 proxy를 재시작했을 때 앱이 종료하고 다시 붙는지 확인한다.
9. 결과를 `bench-results/`에 기록한다.

### 2단계: 두 노드 (peer carrier)

전제: peer carrier가 이 repo의 DPU data plane에 들어와 있어야 한다(별도 코드 작업. connector 세 갈래, 등록에 Pod UID, topology 소비).
1. jet2의 DPU를 준비하고, inventory에 2행을 넣고 `helm upgrade`한다.
2. `peer.enabled=true`로 바꾼다. crypto owner sidecar를 켜고, `fabric-check`를 통과하는지 본다.
3. 서버 replica를 두 노드에 나눠 두고 echo를 양방향으로 보낸다. 같은 노드 DMA와 노드 간 RDMA의 지연·처리량을 비교한다.
4. 장애 시험: 상대 DPU proxy 재시작, crypto owner 재시작, controller 재시작(키 보존), 노드 하나 cordon과 drain.
5. 버전이 다른 DPU가 topology에서 빠지는지 확인한다.

### 3단계: 실제 앱과 보안

- dual-listen과 ingress. DeathStarBench와 Online Boutique를 두 노드에 걸쳐 배포한다.
- 등록 봉인(`~/DPUmesh` v6), 다중 Pod 쌍 격리, rekey, 권한 축소(privileged 제거).

## 11. 확인이 필요한 것

1. **"거의 다 된" 노드 간 RDMA가 어디에 있는지.** `~/DPUmesh` main의 peer carrier인가, 다른 브랜치나 저장소인가? 이 repo의 DPU data plane으로는 누가 언제 옮기나? v1(TLS)과 v2(IPsec pod-pair) 중 무엇이 기본인가?
2. **carrier 제약을 코드에서 풀지.** client 전용 Pod의 노드 간 stream, v2의 명시적 ADMIT 요구.
3. **v2 기본 정책을 어떻게 할지.** controller가 자동 생성할지, carrier가 해석할지(5.5).
4. **물리 연결.** jet1과 jet2의 p0가 직결인지 스위치인지. 스위치라면 RoCE용 설정이 필요한지.
5. **DPU 관리망.** OOB가 `10.200.0.0/24`와 같은 L2에 닿는지, 아니면 p0 SF로 줄지.
6. **두 DPU의 BFB·DOCA 버전.** jet1 host는 DOCA 3.5이고 jet2 host에는 DOCA가 없다.
7. **device plugin.** `~/DPUmesh` 코드(Python)를 가져올지, 새로 쓸지.
