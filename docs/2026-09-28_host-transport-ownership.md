# Native host transport ownership — 2026-09-28

Native `channel.c` and the Comch client no longer use `struct objects` or
`struct dmesh_conn`. The public ABI remains 5; the session protocol, per-flow
rings, and DPA thread allocation policy are unchanged.

## Ownership

- `channel_dev` owns a small `dmesh_comch_client` control endpoint and the
  base/optional extended DPA device/context handles. The host no longer
  allocates a DPU thread pool merely to store the DPA context pointer.
- `channel_conn` owns the forward ring and reverse export metadata. Forward
  buffer views and routing metadata exist only while building OPEN metadata;
  registered memory remains channel-owned.
- Host-DPA flows own a `dmesh_dpa_endpoint`: PE, thread, msgqs, buffer array,
  and completed receive segments. This replaces the former `rc` and `ro`.
- SDK DPA callbacks receive the small Comch resource object. A completion
  sink dispatches to the host endpoint or to a DPU/legacy connection adapter.
  Shared checked teardown helpers take explicit thread, Comch and PE inputs.
- Shutdown still fences DMA, retires callback contexts, destroys thread/buffer
  array resources and imported mappings, and then closes the remote flow.
  Failures retain the owning flow and remaining resources for a retry.

`objects` remains available to the DPU and standalone legacy programs. Their
Comch entry points are adapters in `host/comch_client_legacy.c`. The old Go
hostlib now uses the common host server stubs instead of stale private copies.
The native channel test rejects any inclusion of `object.h`.

## Management structure sizes

Measured using `sizeof` on aarch64; baseline is parent commit `206d2ae`.
These are structure sizes, not RSS or total connection memory. Registered
buffers, rings, completion queues, SDK allocations, and the separate thread
and Comch resource structures are excluded from the per-flow comparison.

| Compared allocation group | Before | After |
|---|---:|---:|
| Flow: `channel_conn` + former `objs` | 40,928 B | 1,744 B |
| Host-DPA flow: above + former `rc`/`ro`, now endpoint | 81,704 B | 1,800 B |
| Channel Comch control holder | 40,256 B | 72 B |

The reverse endpoint is 56 B. Channel-level DPA handles also replace an
additional 40,256 B `objects` and a 1,808 B thread pool. The core port table
and carrier connection objects are outside this change.

## Validation

Before publication, this change was rebased onto `main` at `1a464ba` and
validated with proxy submodule `c09915b`. The existing DNS service resolution
and proxy-closed flow fixes are retained. In particular, DPU quiescence still
advances one nonblocking step at a time; the endpoint helpers preserve the
stop handshake, deadline, completion fence, and failed-context checks.

- aarch64 `make -j4 test`: all 14 native/mock tests plus public header and ABI
  checks passed. Tests include Comch callback ownership, copied send-buffer
  lifetime, host endpoint completion delivery, DMA fences, and cleanup retry.
- x86_64 `make -j4 test`: passed from an isolated source snapshot under
  `/tmp/dpumesh-host-objects-integrated-20260928/source` on the host.
- x86_64 `go test -race ./...` in `integrations/grpc/go`: passed against that
  snapshot's native library.
- DPU transport and standalone DMA benchmark builds passed.
- Standalone legacy Go hostlib Meson/Ninja build passed.
- `cargo test -p dmesh-doca --offline`: 13 tests passed with SDK link flags.
- Full `linkerd2-proxy` release build with ThinLTO and DOCA enabled: passed.

Build/test logs are in `/tmp/host-objects-*.log` on the DPU and
`/tmp/dpumesh-host-objects-integrated-20260928/` on the x86 host. No hardware DMA echo,
performance measurement, device configuration change, or deployment was
performed for this refactor.
