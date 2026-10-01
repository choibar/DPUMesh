# gRPC 64B echo: .NET — 2026-10-01

판정: **.NET 서버는 Go client에서 Go 서버의 98%다(conn 1×64). .NET끼리는 conn 1×64에서 91%, conn 4×64에서 89%다.**

- Online Boutique의 cartservice처럼 Go client가 .NET 서버를 부르는 구성은 Go와 같은 수준이다.
- 측정한 실행 모두 유효했다. Payload 불일치, RPC 오류, DPU ERR·크래시는 0건이다.

## 결과

L7, DPU proxy 1 core, 64B unary echo. .NET끼리는 3회 중앙값, 나머지는 1회다(Go끼리는 같은 시간대에 잰 값).

| client → server | 1 conn × 64 | Go 대비 | p50 / p99 | 4 conn × 64 | Go 대비 | 1 conn × 1, p50 |
|---|---:|---:|---:|---:|---:|---:|
| Go → Go | 12,830 | 100% | 5.0 / 6.4 ms | 15,688 | 100% | 622 µs (오전 측정) |
| Go → .NET | 12,628 | 98% | 5.1 / 6.3 ms | – | – | 736 µs |
| .NET → Go | 11,881 | 93% | 5.5 / 7.4 ms | – | – | – |
| **.NET → .NET** | **11,650** | **91%** | 5.6 / 7.6 ms | **13,931** | **89%** | 888 µs |

TCP(.NET끼리, DPU 없음, 1회): 1×64 145,760, 4×64 337,511 RPC/s.

- **서버:** Go client에서 Go 서버와 거의 같다. 1×1 지연(736 µs)은 같은 stream reactor를 쓰는 C++ 서버(735 µs)와 같다.
- **client:** .NET client가 Go 서버를 부르면 93%다. .NET끼리의 차이는 주로 client 쪽이다.
- **4×64:** .NET끼리 89%로 기준(90%)에 조금 못 미친다. 이 조건에서는 Go client → .NET 서버를 재지 않았다.
- TCP 처리량이 DPU 경로보다 10–20배 높으므로 .NET runtime이 한계는 아니다.

## 조건

[transport·adapter 변경 전후 기록](2026-10-01_grpc-64b-perf.md)과 같다. Host 라이브러리와 DPU는 `feature/grpc-perf`이고, .NET 10.0.12, `Grpc.AspNetCore.Server`와 `Grpc.Net.Client` 2.83.0이다. .NET 쪽은 [`integrations/grpc/dotnet/bench/ChannelBench`](../integrations/grpc/dotnet/bench/ChannelBench/Program.cs)이고, 서버는 Kestrel(로그 없음), client는 연결마다 `GrpcChannel` 하나다.

CSV: [2026-10-01_grpc-dotnet-64b.csv](2026-10-01_grpc-dotnet-64b.csv).
