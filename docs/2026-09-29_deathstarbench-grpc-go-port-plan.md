# DeathStarBench HotelReservation의 현재 DPUMesh Go API 포팅 계획

2026-09-29 작성. 아래는 구현 전 계획이다. 현재 API 포팅 및 W=1/2/4 실제 검증을
완료했으며, 구현 범위·측정 결과·남은 검증은
[구현 보고서](../bench-results/2026-09-29_deathstarbench-current-api.md)에 기록했다.

## 목표와 범위

기존 HotelReservation 포팅을 `integrations/grpc/go` + native `libdpumesh`로 이전한다.
DPU는 `DMESH_SHARDED=1`, `DMESH_BUSY_POLL=1`, `DMESH_NUM_WORKERS=W`,
reverse 경로는 우선 `dpu-dma`를 사용한다. 같은 PF/app의 DPU context는 현재 구현대로 공유한다.
첫 통합은 host에서 서비스 프로세스들을 실행하고 DPU proxy를 통과하는 구성이다.
Kubernetes 배포, host-dpa 비교, DPA thread multiplexing, 동적 worker 이동은 후속 범위다.

대상은 기존 Go 포팅이 있는 **HotelReservation**이다. 외부 frontend HTTP와
MongoDB/Memcached/Consul/Jaeger 연결은 기존 경로를 유지하고 서비스 간 gRPC를 DMA로 연결한다.
RPC 정의, 업무 로직, tracing interceptor와 deadline/metadata 의미를 유지한다.
Go의 `net.Conn` adapter 포팅을 애플리케이션 전체의 zero-copy 구현으로 표현하지 않는다.

## 확인한 현재 상태

- 현재 모듈: `integrations/grpc/go`는 프로세스당 channel/Comch/EQ poller 하나,
  connection당 native QP를 사용한다. `DialContext`, `ListenService`, `CloseTransport`가 진입점이다.
- 구형 모듈: `apps/dmeshgo`는 `libdmesh_hostlib`에 연결한다. 동일한 `dmeshgo` 모듈명을
  사용하지만 구현과 wire protocol이 다르다. 두 모듈을 혼용하지 않는다.
- 구형 포팅: `apps/dmeshgo/dsb/dmesh/{dmesh,balanced}.go`, `dsb-integration.patch`,
  `dsb_run_services.sh`, `dsb_host_setup.py`가 있다.
- Host의 `/home/youngmin/hotelres-dmesh`에는 실제 구형 포팅이 있고, go.mod가
  `/home/youngmin/bf-workspace/dmeshgo`를 참조한다. Go 1.26 / gRPC 1.71.0이다.
- Host의 `/home/youngmin/dpumesh/DeathStarBench`는 `hotelres-bench-patches` 브랜치이며
  별도 checkout이다. 여기의 HotelReservation은 Go 1.18 / gRPC 1.56.3이고 구형 dmesh
  adapter가 없다. 수정 중인 `registry/registry.go`와 `.orig` 파일이 있어 보존해야 한다.
- 기존 adapter는 destination replica마다 worker와 가상 주소를 고르고 `Dial(...)`을 호출한다.
  `combinedListener`는 한 프로세스에서 여러 listener를 열고, `balancedConn`은 별도 ClientConn
  묶음을 만든다. 현재의 process configuration 및 단일 listener 모델로 그대로 옮길 수 없다.
- Frontend는 search/profile/recommendation/user/reservation/review/attractions를 호출하고,
  search는 geo/rate를 호출한다. 서비스 프로세스가 client와 server 역할을 동시에 한다.

## 목표 구성

```text
wrk2 -- HTTP --> frontend replicas (host)
                   │ gRPC / current dmeshgo
                   │ one process channel -> fixed DPUMesh<i>
                   v
             DPU sharded proxy (W workers, busy-poll)
                   │ service VIP -> backend selection
                   v
             service replicas (host)
                   └─ search -> geo / rate도 동일한 gRPC 경로
```

- 프로세스마다 고유 workload/Pod IP 식별자와 고정 `DPUMESH_SERVER`를 부여한다.
  최초 channel open 전에 PCI, service target, backend pool 설정까지 완료한다.
- 서비스당 안정된 VIP:port 하나, replica마다 별도 Pod IP/identity를 둔다.
  호출자는 service VIP로 `DialContext`하고, 서버는 `DPUMESH_SERVICE`를 설정해
  `ListenService()` 하나로 수신한다. Replica 선택은 DPU routing/backend registry가 담당한다.
- **worker 선택은 호출자 프로세스 기준**이다. 한 프로세스의 connection 수를 늘려도
  그 프로세스의 native Driver는 여러 shard로 분산되지 않는다. Frontend를 포함한 실제
  replica 프로세스를 여러 worker에 배치하고, worker별 connection/부하를 기록한다.
- Backend가 다른 shard에 있을 수 있다. 연결 성립뿐 아니라 양쪽 leg의 실제 owner와
  cross-shard 처리 비용을 검증한다. 모든 처리가 한 core에 모인다고 가정하지 않는다.
- 1차 배치는 명시적인 manifest로 고정한다. 이후 CPU 측정에 따라 replica 배치를 조정한다.
  기본 API에 multi-channel을 추가하는 작업은 이번 포팅의 선행 조건으로 삼지 않는다.

## 구현 순서와 완료 조건

1. **재현 가능한 source/build 기준 확정**
   - 두 DSB 디렉터리의 차이와 기존 패치를 대조하고 별도 `codex/` 작업 브랜치/작업 디렉터리를 만든다.
   - DPUMesh의 기존 미커밋 수정은 보존하고 빌드에 사용한 commit + diff hash를 남긴다.
   - DSB 모듈 의존성을 현재 `integrations/grpc/go`로 연결한다. 로컬 경로는 생성형 go.work
     또는 설정 가능한 replace로 관리하고 개인 절대 경로를 배포용 설정에 고정하지 않는다.
   - Host native library/서비스와 DPU transport/kernel/proxy를 같은 session protocol로 빌드한다.
   - 완료 조건: TCP 서비스 빌드와 현재 Go adapter 테스트 통과, 실제 링크된 library 확인.

2. **서비스 설정 및 control plane 연결**
   - 하나의 manifest에 service VIP/port, replica identity, worker, host CPU,
     replica 수와 backend pool/max를 정의한다. Host registry 설정과 DPU mock destination/policy
     설정을 여기서 생성해 주소 불일치를 방지한다.
   - Consul target을 서비스 이름으로 해석한 뒤 DMA 모드에서 VIP로 연결한다.
     임의 replica 주소와 구형의 모든 서비스 공통 포트 8086 규칙은 제거한다.
   - Consul 등록/해제 ID와 readiness를 점검한다. DMA-only listener에 TCP health probe를
     적용하지 않고 실제 RPC readiness 또는 별도 관리 endpoint를 사용한다.
   - 완료 조건: 1개 서비스와 1개 호출자의 실제 RPC 성공, identity/routing 로그 일치.

3. **공통 dialer/listener와 lifecycle 포팅**
   - `dmesh/dmesh.go`: 현재 API용 얇은 adapter로 교체한다. `DialContext`에 context를 전달하고
     구형 source-port 생성, 호출마다 worker 선택, 복수 listener fan-in을 제거한다.
   - `dialer/dialer.go`: tracing, keepalive, credential 설정을 유지하면서 DMA transport를 연결한다.
     첫 단계는 service당 ClientConn 하나를 사용하고, 필요 시 connection 수를 별도 설정으로 확장한다.
     Replica 수를 client connection 수와 동일시하지 않는다.
   - `dmesh/balanced.go`: 구형 replica별 channel wrapper의 호출부를 조사해 제거/이전한다.
     단일 VIP + DPU backend 선택과 기존 client replica round-robin을 중복 적용하지 않는다.
   - 각 서비스의 listener와 cmd 종료 경로를 갱신한다. 시작 중 실패도 부분 생성 자원을 회수한다.
     종료는 신규 요청 차단 → 제한 시간 내 gRPC drain → 남은 RPC 강제 종료 → outbound
     ClientConn/listener 정리 → `CloseTransport` 순서로 처리하고 native cleanup 오류를 보고한다.
   - Search의 listen 전에 outbound dial이 일어나도 첫 channel이 올바른 서비스 설정으로
     생성되도록 모든 환경 설정은 launcher가 미리 제공한다.
   - DPU가 HTTP/2를 해석하는 현재 경로에서 검증한 plaintext gRPC를 초기 기준으로 쓴다.
     TLS가 설정된 실행은 조용히 해제하지 않고 지원 경로를 확인하거나 명시적으로 거절한다.
   - 완료 조건: 단일 RPC, 동시 listen/dial, deadline/cancel, 정상 종료 및 재시작 검증.

4. **Sharded 실행 harness와 자원 예산**
   - 기존 실행 스크립트의 고정 홈 디렉터리와 광범위한 `pkill -9`를 실행별 PID 관리,
     readiness 대기, TERM 후 제한 시간 내 종료로 교체한다.
   - 우선 W=1, 이후 W=2/4에서 same-shard/cross-shard를 검증한다.
     `DMESH_SHARDED`는 존재 여부로 활성화되므로 값 0으로 off 처리하지 않는다.
   - Channel당 session flow ID 한도 32에 outbound 연결, accepted backend, 준비된 backend를
     합산한다. 프로세스별 `DPUMESH_BACKEND_POOL/MAX`는 fan-in 및 outbound 여유에 맞게 정한다.
   - 별도로 worker당 32-slot DPA pool 및 실제 EU/thread 사용량을 산정한다.
     Proxy의 ingress/backend 두 leg, 대기 backend, HTTP/2 pool/reconnect 중 겹침을 포함한다.
     Context 하나 공유가 thread/EU 한도 해제를 의미하지 않는다.
   - 수치가 한도를 넘으면 replica 배치/연결 수를 조정한다. NIC/SF/EU partition 변경은 하지 않는다.
   - 완료 조건: 실행 전 예산 검사, 실행 후 actual resource count와 manifest 대조.

5. **HotelReservation 기능 및 복구 검증**
   - `frontend -> search -> geo/rate`부터 multi-hop을 검증한 뒤 전체 9개 gRPC 서비스로 확대한다.
   - 검색/추천/로그인/예약과 review/attractions 경로를 기존 TCP 결과와 비교한다.
     예약처럼 상태가 바뀌는 검증은 분리된 데이터셋이나 재현 가능한 초기화를 사용한다.
   - Tracing metadata, 오류 코드, timeout, 동시 요청을 확인한다.
   - Replica 추가/종료, 재연결, 반복 stack 재시작, 3분 이상 idle 후 재개를 검증한다.
     과거의 idle 후 DPA fatal 및 종료 시 proxy panic을 재발 여부 확인 항목으로 둔다.
   - 완료 조건: 기능 결과 일치, 요청/종료 오류 없음, resource 누수/고갈 없음.
     Traffic 성공과 teardown 성공은 별도 판정한다.

6. **성능 평가**
   - 동일한 새 서비스 바이너리에서 direct TCP와 DPUMesh sharded busy-poll을 비교한다.
     TCP는 transport 참고 기준이며, mesh 비용 비교는 가능한 동일 proxy TCP 경로를 별도 표시한다.
   - 동일 데이터/replica/host affinity/GOMAXPROCS/tracing 조건에서 HotelReservation의
     `mixed-workload_type_1` 및 개별 endpoint를 wrk2 부하 단계별로 측정한다.
     Echo의 64B/64 RPC 조건을 DSB workload의 요청 크기/동시성과 혼동하지 않는다.
   - W=1/2/4부터 검증하고 자원 예산이 허용하면 8/12/16으로 확장한다.
     Core 확장과 replica 확장을 분리한 실험을 둔다.
   - Offered/achieved RPS, p50/p95/p99, non-2xx/timeouts, host 서비스별 CPU/GC,
     DPU shard별 CPU와 flow 수, DB/cache 및 load generator 병목을 기록한다.
     Busy-poll의 CPU 100%만으로 유효 처리 병목이라고 판단하지 않는다.
   - Warmup 이후 각 조건 최소 3회 반복하고 실패 실행도 별도 남긴다.

## 산출물

- 현재 Go API 기반 DSB adapter, 서비스 설정 및 종료 처리 패치.
- 하나의 topology manifest로 설정/실행/정리를 수행하는 harness와 재현 README.
- Host/DPU 빌드 정보 및 실제 RPC 경로 증거, 기능/복구 검증 결과.
- TCP 대비 sharded DPUMesh 성능표와 worker별 CPU/flow 분포.

첫 구현 묶음은 1~3단계와 1개 edge 검증으로 제한한다. 이후 multi-hop 및 W=4 검증을
통과한 상태에서 전체 HotelReservation과 대규모 성능 평가로 확장한다.
