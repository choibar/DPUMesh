# Verification

`make test` runs the host-only checks and the ABI contract; no device is opened.

| Test | Covers |
|---|---|
| `peer_imm_bootstrap_test` | Production IMM+DPA ARM adapter: exact 64B native HELLO, rejection of legacy/corrupt metadata, IPsec-before-connect, delayed local SEND readiness barrier, 128B scratch, mirrored-ring wrap and source retention across failed descriptor publication/QP destruction |
| `peer_dpa_runtime_test` | Production peer scheduler: per-context work budgets, round-robin fairness, detached context skipping, last STOP and idle yield |
| `peer_dmesh4_test` | Control HELLO/PROPOSE headers accept the current protocol version and reject legacy/future versions; actual four-byte header and one-byte/max DATA, namespace isolation, delayed reader/NIC/RELEASE_ACK barriers, production handle-return gate, 65,537 complete slot reuses without stream generations |
| `peer_rdma_layout_test` and its 2/8/129-node builds; `test-peer-config` | Shared D2D lane capacities, derived CQ/hash bounds, counter notification bounds, invalid settings and production per-peer admission including retained slots, using real transport structs and live lists |
| `peer_write_completion_test` | Compact 8B DPA fences and 2B ARM WRITE lengths, cumulative completion and arena reference release, 64KiB frames, ID wrap, FIFO growth/reuse, failed-publication rollback and invalid completion rejection |
| `peer_connection_test` | Direct arena TX source addresses and exclusive header headroom, pressure retry, ACK-before-NIC completion, connection/token reuse with outstanding NIC holds, failed-post destroy fence, connection/worker arena caps, RX wrap and partial ACK |
| `peer_write_completion_dpa_test` | Production IMM+DPA: 64B bootstrap, zero-byte RQ reposts, DATA/credit counter wrap and validation, WRITE coalescing, delayed CQEs, logical/physical/fence-ring wrap, SQ/fence backpressure, epoch wake retry/suppression and STOP writeback |
| `peer_fence_stats_test.py` | Batch-weighted fence summaries, duplicate log suppression, whole-window trimming and histogram percentile ranges |
| `native_header_contract_test.py` | Public headers compile as C and C++ |
| `abi_contract_test.sh` | Exported symbols of `libdpumesh.so.6` and the preload shim match `fixtures/native_abi_lp64.txt` |
| `native_api_contract_test`, `preload_api_contract_test` | Façade argument validation without a transport |
| `native_core_transport_test` | Channel, accept, QP, reservation, custody ACK, held RX buffers, FIN and teardown over the memory carrier in `support/` |
| `native_listen_test` | The channel as listening socket: accept returns a QP with its own fd; with flow wake a QP drains and arms its own stripe only and gets its control news and the DPU's PREPARE/SCALE requests, without it the channel hands each QP its receives |
| `native_signaled_test` | A QP's idle wake with signaled receives: quiet sends, the backstop left alone, custody sleeps, a waiting sender or a retiring flow polls, the re-check drain returns |
| `native_writable_test` | Writable-buffer accounting of the core; a drain quiets only its own thread's notifies of its EQ |
| `native_polling_test` | A QP's idle wake (the periodic backstop from the fd's hand-out, sleep on its own stripe, re-polls while a flow retires, the backstop's missed-wake count, which counts only when it hands the poller events); the channel arms the session and re-polls after a DOORBELL |
| `native_loss_test` | A lost session ends the process with status 75 by default; with `DPUMESH_EXIT_ON_LOSS=0` accepts, polls and dials report the sticky errno and the channel's and QPs' fds stay readable |
| `rx_consumed_pos_test` | Production proxy shim: a pool thread's DPU RX consumed-position batching under the DPA SDK lock, failed-copy retry and multiple staging wraps; a mux flow's release store on every change |
| `carrier_logic_test` | Forward chunking to the DPUMesh copy rule, the in-order release window, async open and close, held flow ids, and a flow's DPU requests announced once and answered on the open client flow only |
| `service_resolve_test` | Service target grammar, DNS answer caching and invalidation, address ids, fail-closed `DPUMESH_TARGETS` membership |
| `topology_test` | Topology header |
| `session_protocol_test` | Versioned Comch envelopes, malformed frames, unaligned input, flow identities, TAILS/SHUTDOWN and registration extension shapes with upstream message IDs preserved |
| `session_flow_test` | Shared-session flow lookup, generation checks, independent DPA pool ownership, disconnect fan-out, native/benchmark TLS allocation and failed TLS setup ownership |
| `channel_session_test` | Production host session code with mock Comch: one client for multiple flows, the session's tail table exported once with each flow on its own slot, stale replies, isolated close/errors, failed-close retention and session failure |
| `channel_flow_wake_test` | Flow wake against a fake SDK: every slot's sync event over its word of the tail table page, one wait task at a time armed on the word's value and completed when it changes, all or none, the sync event destroyed before its PE and retained when it does not stop |
| `session_server_test` | Production DPU session progress: HELLO timeout, pending/expired registration, TAILS after registration and kept once per session and copied to its flows, distinct OPEN metadata, stale requests, close failure/retry, reverse-DPA completion before resource cleanup on Host and ARM, reader detachment, no-teardown and quarantine |
| `backend_dispatch_test` | Backend dispatch: lazy allocation, pinned owner, dedup, backpressure, stale fencing; a retired listener address stays DMA-known for `DMESH_LISTENER_GRACE_MS` only |
| `dispatch_request_test` | DPU requests on their flow: PREPARE on the flow a migration moves, SCALE on the flow it was asked on, then another client flow to the destination, or waiting for one, and sent again until acknowledged; SCALE_ACK matched by token |
| `dma_cleanup_test` | CPU DMA stop/drain failures, submitted-task ownership, callback chaining suppression; against a fake reverse DPA thread, batches posted back to back, the thread's full ring and the host's free slots holding the next one, the cursor pull (a host cursor past the published batches clamped), the end of stream after the last batch and TX completion from the thread's completion line, a failed copy failing the flow |
| `dpa_cleanup_test` | DPA issued/completed close fence, kernel error retention, MsgQ/context/thread cleanup failures and retryable ownership |
| `dpa_poll_test.py` | Production kernel with delayed DMA/CQEs: source lifetime, bounded pipelining, descriptor-sized RX space checks including wrap padding and the full/empty boundary, CQ draining while RX is full, consume/resume, shutdown, errors and TLS across retriggers; the pod mux thread: one read per tail line, only rings whose tail moved visited, each ring's copies and messages to its worker's consumer, a worker without credit holding back only its rings, tail errors, close, refill, the staging gate reading the reader's release from ARM memory only when it holds a copy back, and stop |
| `ec_rev_poll_test.py` | Reverse DPA thread's production kernel with delayed copies: each flow's copies on its own queue, a flow whose copy never completes holding back no other flow, per-flow order with the end of stream last, a retired queue's late completions reaching no flow, a failed copy failing only its flow, out-of-sequence entries and a line of another flow, a full window of batches in flight |

The DPA WRITE test includes the production device implementation with host SDK
substitutes in `support/dpa_sdk/`. These headers apply only to that test target.
Layout variants and invalid-configuration compiler checks are built by Make.
Archived SEND, Sync and ARM-only IMM tests reside in
`src/transport/peer/legacy/tests` and are excluded from the test/build graph.

The session tests do not open a device. They exercise production control and cleanup code with mock SDK objects.
The private Comch protocol (session protocol v10, `src/transport/common/session_protocol.h`)
changes independently of the public ABI, so host and DPU must be rebuilt together. It
requires completion-fenced TX reclamation: the host's forward-ring `consumer_head`
advances only on the copy's completion, and the proxy's reverse TX source is held until
`pushed_bytes` advances, after the reverse DPA thread stored the batch's descriptor. The
session carries the host's PING (the only way an idle host notices a DPU proxy that
exited), the migration spare (PREPARE/SPARE_READY), flow scaling (SCALE/SCALE_ACK) and
the SHUTDOWN half close, PREPARE and SCALE on the client flow they concern, and the
session's TAILS page; no data-path wake crosses it.

`support/native_memory_transport.c` is a deterministic loopback carrier for the
core: it implements the private carrier contract in memory, can hold custody
ACKs and is never linked into the library. The device-backed carrier
(`src/core/carrier.c`) requires hardware validation.

## Proxy reader fence

After `ninja -C src/transport/build`, run the DOCA library tests from
`linkerd2-proxy` with the installed DOCA SDK:

```sh
cd linkerd2-proxy
RUSTFLAGS='--cfg tokio_unstable' cargo test -p dmesh-doca --offline --locked --lib
```

The tests verify that C receives the reader-detachment acknowledgement only
after Rust IO can no longer access staging, including acknowledgement retry.
They also verify that publication does not free TX space, partial completions
release only their own prefix across wrap, FIN waits for completion, and a
detached handle cannot release a replacement flow's staging.
Peer TX tests also cover chunk sealing across publication retries, direct writes
with untouched header headroom, chunk starvation/wakeup, and detach/FIN fencing.
