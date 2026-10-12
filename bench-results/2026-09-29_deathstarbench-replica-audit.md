# DeathStarBench replica routing audit — 2026-09-29

**현재 DMA 포팅에는 replica별 요청 분산이 빠져 있다.**

서비스별 4개 replica를 기동했지만 DMA의 업무 RPC는 replica 3 하나에 집중됐다. 동일한 계측 binary의 direct TCP 모드는 4개에 거의 균등 분산됐다. 이전 기능 검증은 RPC 전달과 lifecycle 검증이며 replica scaling 검증으로 해석하면 안 된다.

## 조건과 계측

4 ARM workers, sharded busy-poll dpu-dma. 9개 gRPC 서비스 각각 4 replicas + HTTP frontend 4개 = host 40 processes. Host CPU 0–11 / GOMAXPROCS=4, wrk2 CPU 12–15. Frontend당 HTTP connections=64. TLS=false, tracing sample=0, backend pool=1/max=2. Mixed-workload_type_1, 2k RPS 20초 warmup 후 4k/8k 각각 30초. TCP는 proxy를 거치지 않는 직접 연결이며 동일 app affinity/replica/isolated infrastructure를 사용했다. 따라서 mesh 대 mesh 성능 비교는 아니다.

9개 server에 임시 `grpc.StatsHandler`를 추가해 method별 완료 RPC/오류/protobuf bytes를 집계했다. 원래 업무 handler와 interceptor는 유지했다. 500ms마다 JSON snapshot을 쓰고 부하 전후 2초 여유를 두었다. 계측 overhead는 별도 분리하지 않았으며, 각 조건 1회인 진단 결과를 기존 최대 처리량 결과로 대체하지 않는다. Warmup/readiness를 업무 계수에서 제외했다. 종료 직전 취소된 RPC도 완료 계수에 포함될 수 있어 HTTP 응답 수와 경계가 정확히 일치하지 않는다.

## 처리량

| Mode | Offered HTTP/s | Completed HTTP/s | max frontend p99 (ms) | Host app CPU (100%=1 core) | RPC errors incl. stop boundary |
|---|---:|---:|---:|---:|---:|
| dmesh | 4000 | 3951.62 | 70.40 | 1144.0% | 45 |
| dmesh | 8000 | 5602.54 | 10530.00 | 1142.5% | 107 |
| tcp | 4000 | 3982.75 | 5.80 | 1011.7% | 0 |
| tcp | 8000 | 7960.17 | 23.09 | 1152.3% | 4 |

모든 trial의 wrk2 HTTP/socket 오류는 0. 서버 계측의 RPC 오류는 0이 아니므로 구분한다. 부하 종료 시 client cancellation이 발생하고 frontend 실패 로그는 각 측정 내부(start+2s ~ end−2s)에 없었다. RPC별 오류 코드/시각을 수집하지 않았으므로 각 오류의 원인을 모두 확정한 것은 아니다. p99는 frontend별 p99 중 최댓값이며 global p99가 아니다.

## 실제 replica별 업무 RPC 수: offered 4k, 30초

| Service | DMA replica 0 / 1 / 2 / 3 | TCP replica 0 / 1 / 2 / 3 |
|---|---|---|
| attractions | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 |
| geo | 0 / 0 / 0 / 71104 | 17885 / 17884 / 17886 / 17886 |
| user | 0 / 0 / 0 / 1176 | 291 / 293 / 292 / 293 |
| profile | 0 / 0 / 0 / 117434 | 29581 / 29582 / 29582 / 29581 |
| recommendation | 0 / 0 / 0 / 46396 | 11697 / 11696 / 11696 / 11696 |
| reservation | 0 / 0 / 0 / 71616 | 18029 / 18029 / 18028 / 18031 |
| search | 0 / 0 / 0 / 71104 | 17886 / 17885 / 17886 / 17884 |
| rate | 0 / 0 / 0 / 71095 | 17885 / 17884 / 17885 / 17887 |
| review | 0 / 0 / 0 / 0 | 0 / 0 / 0 / 0 |

Review/attractions는 이 mixed workload가 호출하지 않으므로 양쪽 0이 정상이다. 8k offered에서도 DMA는 동일하게 replica 3에 집중됐다.

## 원인

1. `overlay/dmesh/transport.go:75–98`: DMA dial target은 서비스 VIP 하나 + pick_first, TCP는 실제 replica 주소 목록 + round_robin이다. `Listen/Registration`은 모든 DMA replica를 같은 VIP:port로 등록한다. 고유 Consul ID/Pod IP가 있어도 backend 목적지 키는 같아진다.
2. `mock-policy.rs:120–137,252`: `MOCK_POLICY_ECHO_TARGET=1`이 VIP를 그대로 단일 `Forward` backend로 반환한다. Replica endpoint discovery/balancer가 없다.
3. `linkerd/doca/src/lib.rs:51–87`: VIP별 backend channel Vec에서 `pop()`으로 마지막 채널을 꺼낸다. HTTP/2 client가 이 연결을 재사용한다. 이는 요청별 load balancer가 아니다. 로그에는 서비스 9개에 대해 connector take가 총 9번만 발생했고, 4개 frontend가 있어도 서비스별 하나의 backend 연결이 재사용됐다.
4. `linkerd/app/src/lib.rs:281,417`에서 sharded acceptor들은 공통 outbound stack을 clone한다. HTTP stack에는 공유 cache가 있다. Connector 생성 로그는 ThreadId(02)에 8개, ThreadId(03)에 1개로 집중됐다. Publish 로그로 ThreadId(02)는 owner=1, ThreadId(03)는 owner=2임을 대응할 수 있다. 이는 연결 생성 위치의 증거이며, 모든 후속 L7 실행 시간이 그 thread에 귀속된다는 프로파일 증거는 아니다.

Replica=worker=4의 현재 배치에서는 각 서비스의 replica 3이 모두 worker 3이다. 따라서 LIFO 선택은 host replica 집중과 backend DMA worker 집중을 동시에 만든다. Busy-poll worker CPU 100%만으로 유효 업무가 균등하다고 판단할 수 없다. Staggered 배치는 서비스 간 집중을 완화할 뿐 서비스 내 replica LB를 해결하지 않는다.

## Echo와 비교할 때

16 ARM 64B echo 재측정은 200,563.5 RPC/s, 16개 destination VIP, worker당 4 client connections, connection당 64 in-flight RPC다. 이번 진단은 4 ARM / 서비스 VIP 9개 / HTTP 256 connections이며 동일한 조건이 아니다.
Mixed workload는 search 60%(내부 RPC 5개), recommendation 39%(2개), user 0.5%(1개), reservation 0.5%(2개)이므로 기대 평균은 3.795 RPC/HTTP다. 4k DMA 계측값도 약 3.795였다. 이전 6.8k HTTP/s는 약 25.8k 내부 RPC/s에 해당하므로 200k/6.8k를 transport 성능 비율로 사용하면 안 된다. 이 단위 보정으로 replica 분산 결함이 사라지는 것은 아니다.
Protobuf payload 평균은 JSON 최종 응답 크기나 wire bytes와 다르다. 실제 4k DMA service별 평균은 아래와 같다. 단순히 모든 DSB 메시지가 64B보다 훨씬 크다고 설명하는 것도 맞지 않는다.

| Service | Mean request bytes | Mean response bytes |
|---|---:|---:|
| geo | 10.0 | 10.2 |
| user | 42.6 | 0.0 |
| profile | 7.8 | 330.1 |
| recommendation | 24.0 | 8.7 |
| reservation | 29.5 | 0.6 |
| search | 34.0 | 3.4 |
| rate | 34.2 | 73.6 |

## 수정 방향과 한계

- 서비스 VIP와 실제 replica endpoint를 분리하고, replica마다 고유 DMA destination key로 backend를 등록한다.
- DPU policy/discovery가 VIP → replica endpoint 목록을 공급하고, 요청마다 endpoint를 고르는 L7 balancer를 사용한다. Backend H2 pool은 실제 endpoint를 기준으로 관리한다.
- Shard별 outbound cache/runtime ownership과 cross-worker 연결 재사용 비용을 함께 확인한다. Go의 pick_first만 round_robin으로 바꾸어도 주소가 VIP 하나이면 해결되지 않는다.
- 수용 기준에 replica별 업무 RPC counts, per-worker 유효 처리량, R=1/2/4 증가 시 처리량, replica 제거/재등록, 동일 부하 direct TCP/DPUMesh 비교를 포함해야 한다.

이번 작업은 원인 진단이다. 제품 routing 구현은 변경하지 않았다. 전체 성능 차이 중 replica LB 결함의 기여도를 수치로 분리하려면 수정 전후 같은 계측 조건으로 다시 측정해야 한다. Host app CPU도 포화에 가깝지만, 무업무 replica의 진행 처리 비용까지 포함하므로 애플리케이션 자체 비용으로만 해석하지 않는다.

## 정리와 자료

DMA/TCP 각각 host 40 processes 정상 종료, proxy/mocks 정상 종료. 새 `dpumesh-audit` Compose project만 종료했다. 기존 컨테이너, NIC/SF/EU 설정은 변경하지 않았다.

- [상세 JSON](2026-09-29_deathstarbench-replica-audit.json), [replica counts CSV](2026-09-29_deathstarbench-replica-audit.csv)
- DPU 원본 `/tmp/dsb-audit`, host 원본 `youngmin@192.168.100.1:/tmp/dsb-audit`.
- 진단 source `/tmp/dsb-audit/hotel` (host), 계측 코드 `audit.go`, harness `suite.py`, 분석 `analyze.py`를 raw archive에 포함한다.
- Raw archive: `2026-09-29_deathstarbench-replica-audit.tar.gz` (로컬, git 제외).

Raw archive SHA-256: `5f0c5c096be15a2019b9f3333e2c328d50c5fe28463d64197a95c93b51da8de6`.

후속 작업: [replica routing 구현 및 재검증](2026-09-29_deathstarbench-replica-routing.md).
이 문서의 수치와 원인 분석은 수정 전 audit 기록으로 보존한다.
