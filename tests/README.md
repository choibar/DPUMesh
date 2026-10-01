# Verification

`make test` runs the host-only checks and the ABI contract; no device is opened.

| Test | Covers |
|---|---|
| `native_header_contract_test.py` | Public headers compile as C and C++ |
| `abi_contract_test.sh` | Exported symbols of `libdpumesh.so.5` and the preload shim match `fixtures/native_abi_lp64.txt` |
| `native_api_contract_test`, `preload_api_contract_test` | Façade argument validation without a transport |
| `native_core_transport_test` | Channel, EQ, QP, reservation, custody ACK, held RX buffers, FIN and teardown over the memory carrier in `support/` |
| `native_writable_test` | Writable-buffer accounting of the core |
| `native_polling_test` | Repeated custody ACKs permit sleeping by default; an explicit spin window remains available |
| `rx_watermark_test` | Production proxy shim watermark batching, failed-copy retry and multiple staging wraps |
| `tx_staging_custody_test` | Push retries retain source bytes until descriptor DMA completes, including failure paths |
| `carrier_logic_test` | Forward chunking to the DPUMesh copy rule and the in-order release window of the carrier |
| `service_resolve_test` | Service target grammar, DNS answer caching and invalidation, address ids, fail-closed `DPUMESH_TARGETS` membership |
| `topology_test` | Topology header |
| `session_protocol_test` | Versioned Comch envelopes, malformed frames, unaligned input and flow identities |
| `session_flow_test` | Shared-session flow lookup, generation checks, independent DPA pool ownership and disconnect fan-out |
| `channel_session_test` | Production host session code with mock Comch: one client for multiple flows, stale replies, isolated close/errors, failed-close retention and session failure |
| `session_server_test` | Production DPU session progress: HELLO timeout, distinct OPEN metadata, stale requests, close failure/retry, reader detachment, no-teardown and quarantine |
| `dma_cleanup_test` | CPU DMA stop/drain failures, submitted-task ownership, callback chaining suppression and retry after partial cleanup |
| `dpa_cleanup_test` | DPA issued/completed close fence, MsgQ/context/thread cleanup failures, same-EU helper notification lifetime and retryable ownership |

The session tests do not open a device. They exercise production control and cleanup code with mock SDK objects.
Real DMA completion, hardware context teardown and sibling traffic during close
were verified on matching host/DPU/proxy builds in both reverse modes on
2026-09-25. See the [hardware validation report](../docs/2026-09-25_channel-comch-grpc-validation.md)
for the actual topology, lifecycle assertions and limits of that run. The Comch envelope changes the private wire protocol,
so host and DPU must be rebuilt together even though the public ABI remains 5.

`support/native_memory_transport.c` is a deterministic loopback carrier for the
core: it implements the private carrier contract in memory, can hold custody
ACKs and is never linked into the library. The device-backed carrier
(`src/core/carrier.c`) requires hardware validation.

## Proxy reader fence

After `ninja -C src/transport/build`, run `cargo check -p dmesh-doca --offline`
from `linkerd2-proxy`. The 12 library tests (including three reader-fence tests)
pass with the installed DOCA 3.5 SDK. Its existing Rust link order requires
repeating these shared libraries after the transport archives:

```sh
cd linkerd2-proxy
RUSTFLAGS='-C link-arg=-L/opt/mellanox/doca/lib/aarch64-linux-gnu -C link-arg=-L/opt/mellanox/flexio/lib -C link-arg=-ldoca_common -C link-arg=-ldoca_dpa -C link-arg=-lflexio' cargo test -p dmesh-doca --offline
```

This is a test invocation workaround; the proxy build configuration is unchanged.
The tests verify that C receives the reader-detachment acknowledgement only
after Rust IO can no longer access staging, including acknowledgement retry.
