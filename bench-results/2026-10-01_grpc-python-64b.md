# gRPC 64B echo: Python — 2026-10-01

판정: **Python 서버는 DPUMesh에서 자신의 TCP 처리량보다 13% 높다(6.3k 대 5.6k RPC/s). Python client는 Go 서버를 부를 때 Go client의 87%다.**

- Python 서버는 TCP에서도 약 5.6k RPC/s에서 막힌다(동기 서버와 GIL). DPU 경로의 천장(1×64에서 약 13k)보다 낮으므로, 서버는 Go 대비가 아니라 Python 자신의 TCP 수치와 비교했다.
- Online Boutique의 email·recommendation 서버(Go client → Python 서버)와 recommendation의 client 역할(Python client → Go 서버)을 각각 쟀다.
- 측정한 실행 모두 유효했다. Payload 불일치, RPC 오류, DPU ERR·크래시는 0건이다.

## 결과

L7, DPU proxy 1 core, 64B unary echo. Python끼리는 2회 중앙값, 나머지는 1회다. TCP는 DPU 없이 같은 host에서 잰 값이다.

| client → server | DPUMesh 1 conn × 64 | TCP 1 conn × 64 | DPUMesh / TCP | DPUMesh 4 conn × 64 | 1 conn × 1, p50 (DPUMesh / TCP) |
|---|---:|---:|---:|---:|---:|
| Python ↔ Python | 6,308 | 5,568 | 113% | 6,306 (TCP 5,528) | 1,454 / 1,298 µs |
| Go → Python | 6,256 | 5,529 | 113% | – | 1,237 µs / – |
| Python → Go | 11,237 | 17,862 | 63% | – | – |
| Go → Go | 12,861 | – | – | – | 622 µs (오전 측정) |

- **서버:** 처리량은 Python 서버 process가 정한다(약 1.5 core, 동기 처리와 GIL). DPUMesh에서 TCP보다 13% 높다. 요청 1개씩의 지연은 TCP보다 약 150 µs 길다. Python 자체의 요청당 비용(TCP에서 1.3 ms)이 대부분이다.
- **client:** Python client가 Go 서버를 부르면 11,237 RPC/s로, Go client(12,861)의 87%다. TCP에서는 17,862이므로 Python client 자체가 DPU 경로의 천장보다 빠르다. DPUMesh에서 생기는 13% 차이는 client 쪽 경로 비용으로 보이며, 원인은 보지 않았다.

## 조건

[transport·adapter 변경 전후 기록](2026-10-01_grpc-64b-perf.md)과 같다. Host 라이브러리와 DPU는 `feature/grpc-perf`이고, Python 3.10.12, DPUMesh patch를 넣은 grpcio 1.80.0이다(`build_wheel.sh`로 다시 빌드). Python 쪽은 [`integrations/grpc/python/bench/channel_bench.py`](../integrations/grpc/python/bench/channel_bench.py)이고, 서버는 `grpc.server`(worker thread 10개), client는 conn마다 channel 하나와 future 콜백이다.

CSV: [2026-10-01_grpc-python-64b.csv](2026-10-01_grpc-python-64b.csv).
