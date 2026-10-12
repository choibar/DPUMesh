# DPUMesh

| Directory | Side | Contents |
|---|---|---|
| `src/transport/` | both | The C DOCA transport, a Meson project and the only tree that includes DOCA headers: `common/` (both sides), `dpu/`, `host/`, `device/` (DPA kernel). Built into static archives that everything in `apps/`, the proxy and the host library link. |
| `linkerd2-proxy/` | DPU | The linkerd2-proxy fork with the DMA transport (git submodule). Its build links `src/transport/build/libdmesh_*.a`, so the two directories stay side by side. |
| `include/`, `src/core`, `src/facade` | host | The DPUmesh host library, DOCA-free: public API (`include/dpumesh`), core and carrier (`src/core`), native API and POSIX preload façades (`src/facade`). |
| `integrations/grpc/` | host | C++ and Go gRPC adapters over the host library. |
| [integrations/deathstarbench/](integrations/deathstarbench/README.md) | both | HotelReservation port, topology and sharded DMA test harness for the current Go API. |
| `examples/` | host | One working example per API: native, preload, gRPC. |
| `tests/` | host | Host library unit and contract tests, run by `make test`. |
| `design/` | host | Host library documents: `HOST.md` (layout, wire mapping, limits, configuration), `API.md`, `GRPC.md`. |
| `Makefile`, `.env.example` | host | Host library build and the environment variables it reads. |
| `apps/` | — | Programs over the transport: `dma_bench` (the DMA benchmark pair `dpumesh_host` over the host library and `dpumesh_dpu` over the shim, plus the legacy worker pair `dpumesh_v0_*`), `dmesh-router-cpp`, `dmeshgo` (old Go host library), `hpack-h2-bench`. |
| `docs/` | — | Working notes. |
