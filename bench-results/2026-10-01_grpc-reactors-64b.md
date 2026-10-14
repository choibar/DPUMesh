# gRPC 64B echo: reactor 수와 프로세스당 상한 — 2026-10-01

판정:
- **stream C ABI 언어에서는 reactor 하나가 병목이 아니다.** .NET은 reactor 1개로 206k RPC/s(Go의 93%)를 냈고, 그때 reactor thread는 21%였다.
- **C++ native gRPC에서는 reactor 하나가 병목이다.** reactor 1개 106k, 4개 169k RPC/s(+59%)다.
- **`DPUMESH_REACTORS`로 reactor 수를 늘릴 수 있다.** 옵션 없이 만든 runtime이 모두 이 값을 따르며, stream C ABI도 포함된다.
- **프로세스 하나는 DPU proxy worker 하나에만 붙는다.** 그래서 프로세스당 처리량의 상한은 DPU core 하나다.

## reactor 수 (L4, DPU proxy worker 1개)

Proxy가 HTTP/2를 종단하지 않는 L4에서 host 쪽이 먼저 막히게 했다. Conn 4개 × 동시 64, 각 1회다. Reactor thread의 CPU는 측정 구간의 사용률이며, 100%가 core 하나다.

| 구성 | RPC/s | p50 / p99 | reactor thread |
|---|---:|---:|---|
| Go ↔ Go | 222,230 | 1.1 / 2.8 ms | (Go poller, 구분 안 됨) |
| C++ ↔ C++, reactor 1개 | 106,215 | 2.4 / 4.1 ms | client 88%, 서버 39% |
| C++ ↔ C++, reactor 4개 | 168,682 | 1.5 / 3.0 ms | client 각 41% |
| .NET ↔ .NET (stream C ABI), reactor 1개 | 206,943 | 1.0 / 4.1 ms | 서버 21% |
| .NET ↔ .NET (stream C ABI), reactor 4개 | 216,512 | 0.9 / 4.3 ms | 25% 이하 |

- `-reactors`로 직접 정한 C++ 실행(105,540 / 170,796)과 `DPUMESH_REACTORS`로 정한 실행이 같은 결과를 냈다.
- C++ native에서는 reactor thread가 gRPC endpoint의 수신과 송신 진행까지 맡는다. stream C ABI에서는 바이트를 binding에 넘기고 나머지는 언어 runtime이 처리한다. 같은 reactor인데 부하가 4배 이상 차이 나는 이유로 보인다(thread별 CPU로 본 추론이며 profile은 하지 않았다).

## 프로세스당 상한

`DMESH_NUM_WORKERS`가 W이면 proxy는 서로 독립된 Comch 서버 `DPUMesh0`…`DPUMesh<W-1>`를 띄운다. 각 서버는 자기 DPA pool과 driver를 가진다(`linkerd2-proxy/src/main.rs`). Host 라이브러리는 프로세스당 채널 하나로 그중 한 서버에 붙는다. 따라서 다음과 같다.

- 프로세스 하나의 처리량 상한은 DPU worker 하나다. L7, conn 4×64에서 Go·C++가 약 15.7k, .NET이 13.9k다(1회). L4에서는 약 22만이다.
- DPU core를 더 쓰려면 worker마다 host 프로세스를 따로 둬야 한다(리뷰어의 16 core 측정도 이 구성이다). 바쁜 서비스 프로세스 하나가 여러 DPU core를 쓰는 길은 지금 없다.
- reactor 수가 필요해지는 경우는 worker 하나가 내보내는 양을 host reactor 하나가 못 받을 때다. 이번 측정에서는 C++ native의 L4 부하가 그 경우였다.

## 변경

- `DmeshRuntime`은 Options 없이 만들면 `DPUMESH_REACTORS`(1–64, 기본 1)로 reactor 수를 정한다. `dms_runtime_open()`도 이 경로라 Node.js, .NET, Java, Python binding이 코드 수정 없이 따른다. 범위 밖의 값은 runtime 생성 오류다.
- Reactor와 callback executor thread의 이름을 `dmesh-reactor`, `dmesh-callback`으로 붙였다.
- 시험:
  - C++ ctest 6개가 `DPUMESH_REACTORS` 미설정, 2, 4에서 모두 통과했다.
  - 새 시험 "reactor count follows DPUMESH_REACTORS"는 미설정 1, `3`은 3, `0`·`65`·`2x`는 오류임을 확인한다.
  - EQ가 하나라고 가정하던 시험 두 개는 reactor 수와 비교하도록 고쳤다.
  - 네 언어의 loopback 시험이 reactor 2개와 4개에서 모두 통과했다(Node.js 7, .NET 19, Java 7, Python 6). 여러 reactor thread에서 오는 콜백을 받는 경로다.

## 조건

[transport·adapter 변경 전후 기록](2026-10-01_grpc-64b-perf.md)과 같고, L4는 `MOCK_OUTBOUND_OPAQUE=1`이다. Proxy는 CPU 11, mock은 CPU 3에 고정했다. Thread별 CPU는 `bench/grpc/run.py`가 측정 구간 시작과 끝에 `/proc/<pid>/task`에서 읽었다.

CSV: [2026-10-01_grpc-reactors-64b.csv](2026-10-01_grpc-reactors-64b.csv).
