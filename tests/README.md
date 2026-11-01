# Verification

`make test` runs the host-only checks and the ABI contract; no device is opened.

| Test | Covers |
|---|---|
| `native_header_contract_test.py` | Public headers compile as C and C++ |
| `abi_contract_test.sh` | Exported symbols of `libdpumesh.so.6` and the preload shim match `fixtures/native_abi_lp64.txt` |
| `native_api_contract_test`, `preload_api_contract_test` | Façade argument validation without a transport |
| `native_core_transport_test` | Channel, accept, QP, reservation, custody ACK, held RX buffers, FIN and teardown over the memory carrier in `support/` |
| `native_listen_test` | The channel as listening socket: accept returns a QP with its own fd; with flow wake a QP drains and arms its own stripe only and gets its control news, without it the channel hands each QP its receives |
| `native_signaled_test` | A QP's idle wake with signaled receives: quiet sends, the backstop left alone, custody sleeps, a waiting sender or a retiring flow polls, the re-check drain returns |
| `native_writable_test` | Writable-buffer accounting of the core; a drain quiets only its own thread's notifies of its EQ |
| `native_polling_test` | A QP's idle wake (the periodic backstop from the fd's hand-out, sleep on its own stripe, re-polls while a flow retires, the backstop's missed-wake count, which counts only when it hands the poller events); the channel arms the session and re-polls after a DOORBELL |
| `native_loss_test` | A lost session ends the process with status 75 by default; with `DPUMESH_EXIT_ON_LOSS=0` accepts, polls and dials report the sticky errno and the channel's and QPs' fds stay readable |
| `rx_consumed_pos_test` | Production proxy shim: DPU RX consumed-position batching under the DPA SDK lock, failed-copy retry and multiple staging wraps |
| `carrier_logic_test` | Forward chunking to the DPUMesh copy rule and the in-order release window of the carrier |
| `service_resolve_test` | Service target grammar, DNS answer caching and invalidation, address ids, fail-closed `DPUMESH_TARGETS` membership |
| `topology_test` | Topology header |
| `session_protocol_test` | Versioned Comch envelopes, malformed frames, unaligned input and flow identities |
| `session_flow_test` | Shared-session flow lookup, generation checks, independent DPA pool ownership, disconnect fan-out, native/benchmark TLS allocation and failed TLS setup ownership |
| `channel_session_test` | Production host session code with mock Comch: one client for multiple flows, stale replies, isolated close/errors, failed-close retention and session failure |
| `channel_flow_wake_test` | Flow wake consumers against a Comch connection with few internal tasks: started one at a time, stopped through the control PE, all or none |
| `session_server_test` | Production DPU session progress: HELLO timeout, distinct OPEN metadata, stale requests, close failure/retry, reader detachment, no-teardown and quarantine |
| `backend_dispatch_test` | Backend dispatch: lazy allocation, pinned owner, dedup, backpressure, stale fencing; a retired listener address stays DMA-known for `DMESH_LISTENER_GRACE_MS` only |
| `dispatch_wake_test` | Dispatcher idle wake: ARM reaches each listed flow's worker, one DOORBELL per ARM, immediate ring, stale epochs/generations ignored, full worker queue rings at once, malformed ARM fails the session |
| `dma_cleanup_test` | CPU DMA stop/drain failures, submitted-task ownership, callback chaining suppression, TX completion cursors and descriptor-publication failure |
| `dpa_cleanup_test` | DPA issued/completed close fence, kernel error retention, MsgQ/context/thread cleanup failures and retryable ownership |
| `dpa_poll_test.py` | Production kernel with delayed DMA/CQEs: source lifetime, bounded pipelining, descriptor-sized RX space checks including wrap padding and the full/empty boundary, CQ draining while RX is full, consume/resume, shutdown, errors and TLS across retriggers |

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
notices a DPU proxy that exited. v6 adds the SHUTDOWN half close.

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
