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
| `abi_contract_test.sh` | Exported symbols of `libdpumesh.so.5` and the preload shim match `fixtures/native_abi_lp64.txt` |
| `native_api_contract_test`, `preload_api_contract_test` | Façade argument validation without a transport |
| `native_core_transport_test` | Channel, EQ, QP, reservation, custody ACK, held RX buffers, FIN and teardown over the memory carrier in `support/` |
| `native_writable_test` | Writable-buffer accounting of the core |
| `native_polling_test` | Repeated custody ACKs permit sleeping by default; an explicit spin window remains available |
| `native_loss_test` | A lost session ends the process with status 75 by default; with `DPUMESH_EXIT_ON_LOSS=0` polls and dials report the sticky errno and EQ fds stay readable |
| `rx_consumed_pos_test` | Production proxy shim: DPU RX consumed-position batching under the DPA SDK lock, failed-copy retry and multiple staging wraps |
| `carrier_logic_test` | Forward chunking to the DPUMesh copy rule and the in-order release window of the carrier |
| `service_resolve_test` | Service target grammar, DNS answer caching and invalidation, address ids, fail-closed `DPUMESH_TARGETS` membership |
| `topology_test` | Topology header |
| `session_protocol_test` | Versioned Comch envelopes, malformed frames, unaligned input and flow identities |
| `session_flow_test` | Shared-session flow lookup, generation checks, independent DPA pool ownership, disconnect fan-out, native/benchmark TLS allocation and failed TLS setup ownership |
| `channel_session_test` | Production host session code with mock Comch: one client for multiple flows, stale replies, isolated close/errors, failed-close retention and session failure |
| `session_server_test` | Production DPU session progress: HELLO timeout, distinct OPEN metadata, stale requests, close failure/retry, reader detachment, no-teardown and quarantine |
| `backend_dispatch_test` | Backend dispatch: lazy allocation, pinned owner, dedup, backpressure, stale fencing; a retired listener address stays DMA-known for `DMESH_LISTENER_GRACE_MS` only |
| `dispatch_wake_test` | Dispatcher idle wake: ARM reaches each listed flow's worker, one DOORBELL per ARM, immediate ring, stale epochs/generations ignored, full worker queue rings at once, malformed ARM fails the session |
| `dma_cleanup_test` | CPU DMA stop/drain failures, submitted-task ownership, callback chaining suppression, TX completion cursors and descriptor-publication failure |
| `dpa_cleanup_test` | DPA issued/completed close fence, kernel error retention, MsgQ/context/thread cleanup failures and retryable ownership |
| `dpa_poll_test.py` | Production kernel with delayed DMA/CQEs: source lifetime, bounded pipelining, descriptor-sized RX space checks including wrap padding and the full/empty boundary, CQ draining while RX is full, consume/resume, shutdown, errors and TLS across retriggers |

The DPA WRITE test includes the production device implementation with host SDK
substitutes in `support/dpa_sdk/`. These headers apply only to that test target.
Layout variants and invalid-configuration compiler checks are built by Make.
Archived SEND, Sync and ARM-only IMM tests reside in
`src/transport/peer/legacy/tests` and are excluded from the test/build graph.

The session tests do not open a device. They exercise production control and cleanup code with mock SDK objects.
Real DMA completion, hardware context teardown and sibling traffic during close
were verified on matching host/DPU/proxy builds in both reverse modes on
2026-09-25. See the [hardware validation report](../docs/2026-09-25_channel-comch-grpc-validation.md)
for the actual topology, lifecycle assertions and limits of that run. The Comch envelope changes the private wire protocol,
so host and DPU must be rebuilt together even though the public ABI remains 5.
Session protocol v3 additionally requires completion-fenced TX reclamation:
Host forward-ring `consumer_head` advances only on producer CQ completion;
the proxy's reverse TX source is held until `completed_bytes` (host-DPA) or
`pushed_bytes` (DPU-DMA) advances. The historical hardware report predates v3
and does not validate this completion protocol on devices. v4 adds the
ARM/DOORBELL idle wake on top of v3. v5 adds the PING an idle host sends, the only way it
notices a DPU proxy that exited.

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
