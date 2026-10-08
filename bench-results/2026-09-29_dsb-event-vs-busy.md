# DSB DPU event-driven vs busy-poll — 2026-09-29

Event-driven 모드를 구현하고 같은 release 바이너리로 비교했다. **이번 DSB 조건에서는 busy-poll과 성능이 같지 않다.** 18K 목표 부하의 처리량 중앙값은 16,797 → 14,235 req/s (**−15.25%**), DPU proxy CPU는 15.18 → 10.79코어 (**−28.89%**)였다. Idle proxy CPU는 15.51 → 0.13코어 (**−99.14%**)로 줄었다. 지연시간도 악화되어 최대 처리량을 우선한다면 busy-poll을 유지하는 것이 현재 결과에 맞다.

## 변경 및 검증 범위

- DPU Rust proxy의 event 경로를 수정했다. Host DPU-DMA Go timer와 native 라이브러리, DSB 서비스 코드는 같은 바이너리를 사용한다.
- control/data PE는 `PROGRESS_ALL`이다. 설치된 DOCA SDK `doca_pe.h`에 따라 Linux에서는 request_notification이 이전 알림을 clear하므로, Rust driver의 명시적 clear 호출과 첫 teardown 이후 영구적인 1ms polling 전환을 제거했다.
- TX/FIN뿐 아니라 RX segment 소비에 따른 credit 반환도 driver를 깨운다. IO registration queue도 직접 기다린다. local work가 계속 ready인 경우 H2 task를 굶기지 않도록 event wake 뒤에 yield한다.
- private reverse DMA PE/setup을 위한 1ms safety timer는 남는다. 완전히 timer가 없는 모드라고 주장하지 않는다.
- launcher는 `DSB_PROXY_POLL_MODE=busy|event`, topology는 `--poll-mode busy|event`를 지원한다. 명시적 mode 불일치는 장치 실행 전 거부하며 기본값은 busy다.
- 첫 busy 워밍업에서 upstream go-geoindex의 전역 캐시 map 동시 접근으로 geo-1이 종료됐다. 실패 실행은 제외하고, KNearest를 process-wide mutex로 보호한 동일 geo 바이너리로 두 모드를 재실험했다. 동시 검색 race 테스트를 통과했다. 종료 시 /proc task가 사라지는 observer 예외도 처리했다.
- Rust DOCA crate 15 tests, topology 8 tests 및 Host channel-smoke race/build를 통과했다. 실제 event mode smoke에는 40 close/reopen, sibling 통신, channel 재생성 및 payload 1/8064/8065/8192/8193/65537/1048577B를 포함했다.

## 비교 조건

- 동일 release proxy 바이너리에서 polling mode만 전환했다. Rust flags `--cfg tokio_unstable -C target-cpu=native`, 기존 release LTO 설정. 바이너리 SHA-256은 JSON에 기록했다.
- DPU 16 sharded workers, dpu-dma, frontend workers 14/15/12/13. Host CPU 0–15, GOMAXPROCS=4, 25 앱 프로세스. replica는 reservation/rate/search 각 4, profile/geo/recommendation 각 2, 나머지 각 1, frontend 4.
- wrk2는 r4 CPU 0–3, frontend당 `-t4 -c64 -D exp`. 6K warmup 35초, 14K 및 18K는 각각 30초×3회. 24K는 30초×1회, recovery 6K는20초. 두 mode 사이 앱/proxy를 종료하고 DB/cache 데이터를 초기화했다.
- 순서는 busy→event다. 정규 측정 중 perf/추가 INFO 로그/빌드는 실행하지 않았다. 표는 3회 중앙값(24K는 1회), p99는 각 trial의 frontend별 p99 최댓값의 중앙값이다. 전체 합산 histogram의 p99와 다르다.
- Host 앱 CPU와 DPU proxy CPU는 각각 user+system process time 합계이며 1core=100%. DPU physical busy에는 다른 작업/커널도 포함되며 idle+iowait를 제외한다.

## 결과

| 모드 | 목표 HTTP/s | 실제 HTTP/s | Host 앱 cores | DPU proxy cores | DPU physical busy cores | p99 ms | n |
|---|---:|---:|---:|---:|---:|---:|---:|
| busy | idle | 0 | 0.57 | 15.51 | 16.00 | — | 1 |
| busy | r14000 | 13989 | 11.73 | 15.24 | 15.86 | 89.47 | 3 |
| busy | r18000 | 16797 | 12.06 | 15.18 | 15.84 | 3450.00 | 3 |
| busy | r24000 | 16986 | 12.11 | 15.19 | 15.84 | 9740.00 | 1 |
| busy | recovery | 5967 | 8.41 | 15.34 | 15.93 | 174.46 | 1 |
| event | idle | 0 | 0.57 | 0.13 | 0.57 | — | 1 |
| event | r14000 | 13744 | 11.51 | 10.72 | 11.36 | 1630.00 | 3 |
| event | r18000 | 14235 | 11.59 | 10.79 | 11.43 | 7610.00 | 3 |
| event | r24000 | 14257 | 11.64 | 10.84 | 11.38 | 13030.00 | 1 |
| event | recovery | 5984 | 8.50 | 7.70 | 8.21 | 26.70 | 1 |

동일 offered load에서 event / busy 비교:

| 부하 | 처리량 변화 | DPU proxy CPU 변화 | event RPS 범위 |
|---|---:|---:|---|
| r14000 | -1.75% | -29.62% | 13691–13805 |
| r18000 | -15.25% | -28.89% | 14222–14251 |
| r24000 | -16.07% | -28.63% | 14257–14257 |

## 18K 코어별 physical busy

| 코어 | busy mode % | event mode % |
|---:|---:|---:|
| 0 | 98.76 | 99.26 |
| 1 | 98.81 | 98.35 |
| 2 | 98.74 | 99.37 |
| 3 | 98.80 | 99.33 |
| 4 | 98.90 | 57.89 |
| 5 | 98.92 | 53.21 |
| 6 | 99.05 | 82.90 |
| 7 | 99.24 | 95.43 |
| 8 | 98.95 | 83.25 |
| 9 | 99.02 | 82.15 |
| 10 | 99.49 | 33.42 |
| 11 | 99.49 | 34.51 |
| 12 | 99.47 | 36.17 |
| 13 | 99.19 | 54.17 |
| 14 | 98.96 | 66.60 |
| 15 | 98.91 | 69.65 |

18K event 실행에서 코어 0–3(frontend workers 15–12)은 98.35–99.37% busy였지만, 나머지 코어는 33.42–95.43%였다. 작업이 집중된 frontend worker가 포화되는 현상은 event 모드에서도 남는다. Event 경로에는 PE re-arm과 notification 대기/깨우기 비용이 추가되므로 성능 차이의 후보지만, 이번 비교에서는 perf를 켜지 않았으므로 각 비용의 기여율을 확정하지 않는다. CPU 감소에는 처리량 감소의 영향도 포함된다. 18K에서 완료 HTTP당 DPU proxy CPU는 903 → 758µs로 약 16.1% 줄었다.

## 오류/수명 및 한계

- busy: HTTP/socket success=True; DPA lifecycle {"contexts": 1, "pool_assign": 87, "pool_release": 87}. 각 trial의 내부 RPC error counter는 JSON/CSV에 별도 기록한다.
- event: HTTP/socket success=True; DPA lifecycle {"contexts": 1, "pool_assign": 87, "pool_release": 87}. 각 trial의 내부 RPC error counter는 JSON/CSV에 별도 기록한다.
- 내부 RPC error counter는 0이 아니다. 14K/18K/24K/recovery에서 busy는 각각 109/13/0/1, event는 50/0/0/11건이었다. 워밍업 포함 총합은 busy 124건, event 68건이다. 외부 HTTP 성공과 구분해야 한다. 기존 counter가 status code를 기록하지 않으므로 취소/전송 오류를 구분할 수 없다.
- 두 유효 실행에서 Host 앱 25개씩 exit 0, proxy exit 0 및 DPA 할당/반환 일치를 확인했다. 실패 실행을 포함한 기록 PID에서 살아 있는 테스트 프로세스가 없고, r4 wrk 및 Compose 컨테이너도 정리됐다.
- Event smoke 종료 후 1-worker proxy의 idle 사용량은 9.04초 동안 0.77% CPU였다. 연결을 닫은 뒤에도 영구 busy-poll로 돌아가지 않았다.
- 두 번째 시도는 스크립트가 이전 인프라 주소를 복사해 RPC 검증에 실패했다. 측정 전 실패했으며 비교에서 제외했다. 새 시도는 설정 파일 일치와 RPC 응답 검증 후 진행했다.
- 낮은 부하에서 offered rate를 모두 처리하는 것은 최대 처리량 동등성의 증거가 아니다. 포화 부하 처리량과 p99 및 CPU를 함께 비교한다. 3회 반복은 관측 차이/범위를 제공하며 엄밀한 통계적 동등성 검정은 아니다.
- 상태/부하에 따라 event mode도 CPU를 계속 사용할 수 있다. ready 데이터나 backpressure가 있는 동안 runnable한 것은 정상이며, idle까지 spin하는 것과 구분한다.
- NIC/SF/EU/sysctl 설정은 변경하지 않았다. 모든 테스트 자원은 PID birth에 따라 정리한다. 원본 로그 및 실행 스크립트는 `/tmp/dsb-event-20260929`에 보존했다.

## 사용법

```sh
python3 integrations/deathstarbench/topology.py RUN_DIR \
  --workers 16 --cpus 0-15 --pci 0b:00.1 \
  --replicas reservation:4,rate:4,search:4,profile:2,geo:2,recommendation:2 \
  --frontends 4 --frontend-workers 14,15,12,13 --poll-mode event
DSB_PROXY_POLL_MODE=event DSB_ROUTES_FILE=RUN_DIR/topology.json \
  bash integrations/deathstarbench/proxy-start.sh 16 PROXY_RUN_DIR
```

topology 생성은 Host에서, proxy 실행은 DPU에서 한다. Host topology JSON을 DPU로 복사하고 해당 경로를 DSB_ROUTES_FILE로 지정한다. busy 비교는 두 곳의 event를 busy로 바꾼다.

## 원본 자료

로컬 archive: `2026-09-29_dsb-event-vs-busy-raw.tar.gz`

SHA-256: `8cbed0b3d73e5bb7397f798e5125dcda8c87a18e4588552ff00f22e3215df524`
