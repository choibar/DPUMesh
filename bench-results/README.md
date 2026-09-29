# bench-results

One Markdown file per benchmark run, written by the `bench` subagent.
Filename: `YYYY-MM-DD_HHMMSS_<label>.md`. Each file records the environment
(git SHA, binary timestamps, DOCA version, node/PCI/EU-partition state, the
exact env + harness command) and the result (throughput, p50/p99, per-core
busy%, proxy L7 metrics, raw wrk2/h2load lines), then a one-line verdict.
A run whose conditions were not written down is not a result.

## 2026-09-25–26: gRPC-go 64B echo와 DPA 자원 검증

이번 기록은 보고서, 반복별 CSV, 그림과 작은 결과 JSON만 저장소에 포함한다.
실패와 미완료 조건도 결과의 일부이며, 초기화 실패를 0 RPC/s로 집계하지 않는다.

| 보고서 | 조건 | 상태와 해석 |
| --- | --- | --- |
| [1 ARM core, 두 reverse mode](2026-09-25_grpc-go-64b-1core.md) | conn=1–4, **전체 동시 RPC 64개** | 24회 유효 측정. 중단 실행은 제외했고, 비정상 중단 후 native QP 정리 지연은 미해결로 기록했다. |
| [1 ARM core, dpu-dma P4/C256](2026-09-26_grpc-go-64b-1core-p4-c256.md) | conn=4, **connection당 동시 RPC 64개** | PASS, 3/3회. 앞선 총 동시 요청 64개 조건과 구분한다. |
| [dpu-dma ARM scaling](2026-09-26_grpc-go-64b-arm-scaling.md) | W=1/2/4/8/12/16, K=1/2/4 | INCOMPLETE, 51/54회. 기존 EU partition 상태에서 W16/K4는 preflight 실패했다. |
| [dpu-dma W16/K4 재실험](2026-09-26_grpc-go-64b-w16-k4-no-eu-partitions.md) | EU partition 0개, DPU에 전체 190 EU 가용 | 3/3회 성공, 중앙값 200,568.6 RPC/s. 기존 suite와 EU 배치가 다르다. |
| [host-dpa ARM scaling 및 모드 비교](2026-09-26_grpc-go-64b-host-dpa-arm-scaling.md) | W=1/2/4/8/12/16, K=1/2/4 | INCOMPLETE, 34/54회. W8/K2는 정상 종료 1회뿐이고 W12/16은 process 생성 실패했다. |
| [host-dpa PF0/PF1 분산](2026-09-26_grpc-go-64b-host-dpa-pf01.md) | W=9/12/16, K=1/2/4 | INCOMPLETE, 5/27회 유효. W9/K2는 teardown 실패, W9/K4는 2/5회만 정상 종료했다. PF 분산도 W12/16의 process 한도를 해결하지 못했다. |
| [DPA process 공유 한도](2026-09-26_dpa-process-limit.md) | DPU 1 + host 27, 슬롯 해제 후 양쪽에서 재생성 | 현재 장비의 동시 base process 합계 28개를 확인했다. Application DPA thread 없이 시험했다. |
| [DPU-local SF standalone 시험](2026-09-26_dpu-local-sf-dpa-test.md) | 기존 SF에 임시 EU 8개 배정 | Standalone `doca_dpa_start()` 실패. SF의 DPA 사용 불가나 28개 제한 검증으로 해석하지 않는다. |
| [DPU-local SF extended 시험](2026-09-26_dpu-local-sf-extended-test.md) | PF base → SF extension, 64B heap 할당·해제 | 성공 및 정리 확인. SF datapath/EU scheduling이나 28개 경계의 추가 생성 시험은 아니다. |

W는 DPU ARM worker 수, K는 worker당 client connection 수다. Scaling 실험은
connection당 동시 RPC 64개이며 backend pool도 K개다. Mode 비교는 EU 배치가
다른 실험 사이의 참조 비교다. Busy polling에 의한 CPU 100%만으로 ARM 연산
병목을 확정하지 않는다. 상세 조건·정상 종료 여부·artifact SHA는 각 보고서에 있다.

## 2026-09-29: host adapter

| 보고서 | 조건 | 상태와 해석 |
| --- | --- | --- |
| [Go 어댑터 재구성 전후](2026-09-29_grpc-go-adapter-rework.md) | rapids4, `dpu-dma`, gRPC 64B 동시 1/64, raw 109B echo | 동시 64 처리량 약 +20%, 분리 goroutine raw echo p50 약 −45%; 단일 goroutine p50은 동일. 장비 smoke와 race 테스트 PASS |
| [preload gather write](2026-09-29_preload-gather-write.md) | rapids4, `dpu-dma`, 109B 왕복, preload `tcp_echo` | `writev(9,100)` p50 569 µs → 55–62 µs; 변경 전과 후 교대 실행에서 회귀 없음. 동시 연결 4개/pool 2에서 1개 실패는 기존 문제 |

## 자료 보존

보고서의 상대 링크는 Git에 포함된 파일을 가리킨다. 아래 원본 파일은 이 작업에서
삭제하거나 수정하지 않고 **로컬에만 보존하며 Git에는 포함하지 않는다**.

- 대형 결과 JSON: `2026-09-26_grpc-go-64b-arm-scaling.json`,
  `2026-09-26_grpc-go-64b-w16-k4-no-eu-partitions.json`,
  `2026-09-26_grpc-go-64b-host-dpa-arm-scaling.json`,
  `2026-09-26_grpc-go-64b-host-dpa-pf01.json`,
  `2026-09-26_dpa-process-limit.json`.
- 로그·실행 스크립트 archive:
  `2026-09-26_grpc-go-64b-host-dpa-arm-scaling-raw.tar.gz`,
  `2026-09-26_grpc-go-64b-host-dpa-pf01-raw.tar.gz`,
  `2026-09-26_dpa-process-limit-raw.tar.gz`,
  `2026-09-26_dpu-local-sf-dpa-test-raw.tar.gz`,
  `2026-09-26_dpu-local-sf-extended-test-raw.tar.gz`.
- W16/K4 재실험의 로컬 evidence 디렉터리:
  `2026-09-26_grpc-go-64b-w16-k4-no-eu-partitions-evidence/`.
- 과거 비교에 사용한 로컬 보고서:
  `2026-09-10_grpc-echo-1core-selective-h2-AB.md`.

위 경로는 모두 이 `bench-results/` 디렉터리 기준이다. 저장소 checkout만으로는
전체 원본 로그를 받을 수 없다. 작은 JSON 네 개는 1-core 두 실험과 SF 두 실험의
결과를 위해 포함한다. CSV는 저장소용으로 CRLF 줄바꿈만 LF로 정규화했고,
파싱한 모든 cell 값이 원본과 동일함을 확인했다. PNG와 작은 JSON의 내용은 그대로다.

보고서의 `/tmp/...`와 실행 명령은 **실험 당시의 로컬 경로**다. 공개 자료 정리 시점에
이전 1-core/scaling/process-limit 실험의 `/tmp` 디렉터리는 없었고, 위 archive와
결과 파일이 남아 있었다. SF 두 시험의 `/tmp` 디렉터리는 존재했지만 임시 경로의
지속성을 보장하지 않는다. 원본이 없는 경로를 클릭 가능한 저장소 링크로 표시하지 않는다.
