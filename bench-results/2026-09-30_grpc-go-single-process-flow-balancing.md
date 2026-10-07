# 단일 gRPC-go process의 flow 분산 검증

2026-09-30 22:43 UTC, **PASS**. 한 client process의 한 channel/Comch session에서 생성한 여러 connection이 DPU worker 4개로 분산됐고, 실제 64B echo가 성공했다.

| Client process 수 | Connection 수 | Worker 0/1/2/3에 배정된 client flow 수 | 5초 동안 검증한 RPC | RPC 오류 | 재연결 |
|---:|---:|---|---:|---:|---:|
| 1 | 4 | 1 / 1 / 1 / 1 | 5,411 | 0 | 0 |
| 1 | 8 | 2 / 2 / 2 / 2 | 6,144 | 0 | 0 |

- DPU: sharded, busy-poll, `dpu-dma`, `least-flows`, worker 0/1/2/3은 CPU 15/14/13/12에 pin.
- Host: server process 1개를 유지하고 client process는 각 경우 하나씩 순차 실행. Client PID 456713(4conn), 456735(8conn). 두 client를 동시에 실행하지 않았다.
- 모두 `DPUMESH_SERVER=DPUMesh0`. 다른 Comch alias를 사용해 분산한 것이 아니다.
- Connection당 concurrent RPC 4개, warmup 1초, measurement 5초. 모든 RPC 응답의 64B 전체를 비교했다. 각 connection의 preflight도 성공했고 native dial 횟수는 각각 정확히 1회다.
- Backend server는 `DPUMESH_BACKEND_POOL=1`, `DPUMESH_BACKEND_MAX=1`. 전체 실험에서 backend flow 하나(session 1/1, flow 1/1, worker 0)를 유지했다. 나머지 worker의 요청도 이 backend를 통해 정상 완료됐다.

## Dispatcher 증거

4-connection client의 flow 1..4는 모두 **session 2/1**이며 owner는 순서대로 **1, 2, 3, 0**이다.
8-connection client의 flow 1..8은 모두 **session 2/2**이며 owner는 순서대로 **1, 2, 3, 0, 1, 2, 3, 0**이다.
새 client process에서 session slot을 재사용할 때 epoch가 1→2로 증가했다.
Backend flow는 worker 0의 slot 0, client flow는 각 worker의 별도 slot에 배정됐다.
상세 key/location과 RPC 결과는 [JSON](2026-09-30_grpc-go-single-process-flow-balancing.json)에 저장했다.

## 실행 코드와 범위

새 dispatcher가 링크된 `linkerd2-proxy/target/debug/linkerd2-proxy`를 사용했다. 기존 release binary를 사용하지 않았다.
Go는 현재 dmesh-grpc-go snapshot에서 `channel-bench`를 새로 빌드했다. 기존 benchmark의 4-connection 입력 제한만 `/tmp` 복사본에서 8로 확장했다. 제품 코드는 변경하지 않았다.
Host native library는 기존 `/tmp/dsb-regression-v2-code/build/lib/libdpumesh.so.5`를 사용했다. 이번 dispatcher 변경에는 Host wire/API 변경이 없다.

이번 검증은 **flow 배정 및 데이터 전달의 기능 검사**다. 개발 빌드이고 backend 하나를 공유하므로 처리량 확장성이나 worker별 유효 CPU 부하 균등성을 입증하지 않는다. CPU 사용률에 따라 실시간 재배치하는 정책도 아니며 flow migration은 구현하지 않았다.

Client 실행 인자(connection 4개는 concurrency 16):

```sh
channel-bench -mode client -connections 8 -concurrency 32 \
  -warmup 1s -duration 5s -rpc-timeout 10s -timeout 45s
```

## 종료 및 자료

Client 두 개, server, proxy 모두 exit=0. Mock은 기록한 PID/시작 시각/executable을 확인하고 SIGTERM으로 종료했다. DPA thread assign/release는 **13/13**, 종료 후 DPU `dpaeumgmt info status`는 **processes=0**, EU groups=0이다. DOCA ERR, DPA fatal, proxy panic은 없었다. SDK의 alignment/empty DPA ops 경고는 있었다. NIC/SF/EU 설정 변경은 하지 않았다.

기존 stop script는 release executable만 허용하여 최초 정리를 거절했다. `/tmp` 복사본에서 예상 executable을 실제 debug binary로 지정한 뒤 동일한 PID 신원 검증을 유지하여 정상 종료했다.

원본 로그·실행 스크립트: DPU `/tmp/dmesh-flow-lb-20260930`, Host 동일 경로. Proxy 로그는 DPU의 `proxy/proxy.log`, Host 로그 복사본은 DPU의 `host/`에 있다.

SHA-256:

```text
fc7a11267aa3c0bf5716b52cb597ddf2acb35d290e4b0d371cec0a3ed5b8d0dd  proxy debug binary
893aad4ca65b8d1a98ae9a32f86bc57d4b3cb411e133620be89baf6d829c5059  host channel-bench
164bc7167087c5129c36077e26ca455b39b53c72584c67af098a3cc8c67fdbcf  host libdpumesh.so.5
```
