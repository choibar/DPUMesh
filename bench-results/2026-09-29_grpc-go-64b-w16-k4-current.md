# gRPC-go 64B echo — current build, 16 ARM cores × 4 connections

2026-09-29 UTC. **PASS, 3/3회. 중앙값 200,563.5 RPC/s.**
이전 [2026-09-26 동일 규모 결과](2026-09-26_grpc-go-64b-w16-k4-no-eu-partitions.md)의
200,568.6 RPC/s와 차이는 -0.0025%로, 이번 반복 변동보다 훨씬 작다.
현재 코드에서 뚜렷한 처리량 저하는 관찰되지 않았다. 이전과 바이너리는 다르므로
개별 변경의 인과 효과를 분리한 비교는 아니다.

## 결과

| 반복 | RPC/s | 검증한 완료 RPC | 최대 worker p99 ms | Host client pool busy | Host server pool busy | Proxy CPU |
|---|---:|---:|---:|---:|---:|---:|
| 1 | 200,563.5 | 2,005,635 | 53.880 | 97.16% | 99.06% | 1574.71% |
| 2 | 199,547.3 | 1,995,473 | 61.111 | 97.17% | 99.07% | 1562.00% |
| 3 | 201,423.2 | 2,014,232 | 56.034 | 97.22% | 99.06% | 1575.89% |

총 **6,015,340 RPC**의 64B 응답을 요청 전체와 비교했다.
모든 worker에서 RPC 오류 0, 재연결 0, connection당 native dial 정확히 1회였다.
Latency는 16개 worker 각각의 p99 중 최댓값이다. 전체 요청을 합친 p99가 아니다.
QPS는 16개 worker의 동일한 10초 measurement window 내 성공 완료 수를 합산했다.

## 조건과 자원

- Sharded + busy-poll, reverse `dpu-dma`. DPU 16 ARM workers, CPU 15→0에 pin.
- Worker당 client connection 4개 + backend connection 4개.
  총 client 64개, connection당 in-flight 64개, **총 in-flight 4,096개**.
- Unary raw-codec echo: application request/response 각각 64B. HTTP/2/gRPC framing은 별도.
- Host client 16개 process는 CPU0–7, server 16개는 CPU8–15를 공유.
  각 process GOMAXPROCS=8. Backend initial/max pool=4, RPC deadline=5초.
- 각 반복에서 client를 새로 시작하고 모든 connection의 검증 preflight 완료 후 공통
  start-file barrier로 warmup 3초 + measurement 10초. Server/proxy는 세 반복 동안 유지.
- Host PF `0b:00.1`, DPU PF `03:00.1`, EU partitions 실행 전/후 0개. NIC/SF/EU 변경 없음.
- 현재 shared DPA context **1개**, worker별 32-slot pool **16개(선할당 thread 512개)**.
  로그에서 동시 사용 peak는 **128 flows**. 전체 assign/release는 **256/256**으로 일치.
  선할당 thread 수와 활성 flow 수를 구분한다. 이 모드에서 host DPA context/thread는 사용하지 않는다.

Host client/server pool busy 중앙값은 **97.17% / 99.06%**다. 이는 해당 8-core pool의
`/proc/stat` 기준으로 주변 작업도 포함한다. 서비스 process만의 합산 CPU도 JSON/CSV에 저장했다.
Proxy CPU 중앙값은 **1,574.71%**(100%=core 1개). Worker/helper CPU는 전체 반복에서
92.89–99.78%, ARM core busy는 99.80–99.90%였다.
Busy-poll이므로 CPU 100%만으로 유효 RPC 연산의 포화를 단정할 수 없고,
host server 8-core pool도 거의 포화되어 있다. 이번 성능 구간에서 profiler는 사용하지 않았다.

## 빌드 및 정상 종료

현재 host library와 일치하는 Go `channel-bench`를 새로 빌드했다(Go 1.27.1, gRPC-go 1.71.0).
DPU는 현재 transport/device/proxy build를 사용했다. Shared DPA context와 bounded
polling/retrigger 변경이 포함되어 있다. 실험 중 transport/proxy 제품 코드는 수정하지 않았다.

- Client 48개 실행 전부 exit=0, server 16개 전부 exit=0, proxy exit=0.
- DOCA error, DPA fatal, proxy panic 없음. 기록된 DPA flow 전부 회수.
- 종료 후 `dpaeumgmt info status --dpa_device mlx5_0`: **processes=0**, partitions=0, root groups=0.
- Mock들은 해당 실행의 PID/시작 시각을 검사해 SIGTERM으로 정리했다.
- 전환 전에 실행 중이던 DeathStarBench 측정 및 전용 컨테이너를 중단·정리했다.

SHA-256:

```text
d5013c17757c89ae884ea25984ad1b6f3b3892686d24f56073faba28791fb5a4  /tmp/dmesh-w16-current-channel-bench
3e380bd87be7464159c8d5e0f5bb11edd9ad634a651c287d6a9295cc299fc7f0  /tmp/dsb-current/dpumesh/build/lib/libdpumesh.so.5
fa049bb2b50be259d2f9dbfbad26dba633dc061a681b088530d086d8b9362972  linkerd2-proxy/target/release/linkerd2-proxy
6bc2bafefbb2c433fd295854cb7d1c661e7ac9de3fc6532fdd1bfde36eb8eebe  src/transport/build/device/dpa_kernel.a
```

## 자료

[결과 JSON](2026-09-29_grpc-go-64b-w16-k4-current.json),
[반복별 CSV](2026-09-29_grpc-go-64b-w16-k4-current.csv).
원본은 DPU `/tmp/dmesh-w16-current-20260929`, host의 같은 경로에 있다.
실행 스크립트, worker별 결과/latency, host/DPU CPU samples, affinity, 라이브러리·binary 해시,
source diff, 종료 기록은 Git 제외 raw archive에 보존했다.

`2026-09-29_grpc-go-64b-w16-k4-current-raw.tar.gz` SHA-256: `ea5098df39b48313a4764e928b510d6c477610f7cfbe2a3692f6acb718562a6d`.
