# gRPC 64-byte echo benchmark

Measures one gRPC implementation over DPUMesh, or over kernel TCP as its
baseline, with the channel-bench fixture: verified unary echo of 64 bytes,
`/dmesh.ChannelBench/Echo`, a fixed total concurrency split over 1–4
connections. Every language's fixture speaks the same wire and takes the same
flags ([Go](../../integrations/grpc/go/cmd/channel-bench/README.md),
[C++](../../integrations/grpc/cpp/README.md#channel-benchmark)), so any client
can drive any server.

| File | Runs on | Does |
|---|---|---|
| `run.py` | host | one run: a fresh DPU proxy (DPUMesh only), the server, the client; writes `results/<tag>.json` and the logs |
| `matrix.sh` | host | the recorded set: `REPS` rounds over `CONDITIONS` |
| `summarize.py` | host | medians per label and condition, and a per-run CSV |
| `dpu/build.sh` | DPU | builds the transport archives, linkerd2-proxy and the mocks of the checkout it sits in |
| `dpu/{env,start,stop}.sh` | DPU | the proxy and mock control plane, pinned and on their own ports |

## Running

The DPU needs a DPUMesh checkout with this directory, built by
`dpu/build.sh`; `--dpu-dir` names it relative to the DPU home. The host needs
`make lib` and the fixture under test.

```sh
H=bench/grpc
# Go over DPUMesh, L7 proxy, the default set (1-4 connections x 64, 4 x 256, 1 x 1; 3 rounds)
bash $H/matrix.sh go-l7 "$PWD/bin/channel-bench" "$PWD/bin/channel-bench" \
    --l7 --env GOMAXPROCS=8 --dpu-env DPUMESH_DPA_EU_BASE=64
# The same fixture over kernel TCP
bash $H/matrix.sh go-tcp "$PWD/bin/channel-bench" "$PWD/bin/channel-bench" \
    --transport tcp --env GOMAXPROCS=8
python3 $H/summarize.py --csv results.csv $H/results
```

`run.py` appends `-mode`, `-connections`, `-concurrency`, `-warmup`,
`-duration` and, for TCP, `-tcp` to the commands. Defaults follow the
2026-09-25 single-core measurement: one proxy worker pinned to one DPU core
with busy polling, `dpu-dma`, the backend pool equal to the connection count,
3 s warmup and 10 s measurement. On the host the client and server are pinned
to 8 cores each (`--client-cpus 18-25 --server-cpus 26-33`, the NUMA node of
the BlueField function `94:00.0`).

A run is valid only if the client verified every response with one dial per
connection, the server shut down cleanly, and the DPU proxy logged no error,
DPA crash or panic and stopped on SIGTERM. CPU figures cover the client's
measurement window: the client's own, the server process, the DPU proxy
(100% is one core) and the busy share of each pinned host core set.

## Rig notes

- The DPU functions are set by `DPU_PCI`/`REP_PCI` (`--dpu-env`, default
  `03:00.0`/`94:00.0`) and the host function by `--pci`.
- `PROXY_CPUS` and `MOCK_CPUS` (`--dpu-env`) pin the proxy and the mocks;
  pick cores no other job uses.
- When the other PF runs another DPA job, set `DPUMESH_DPA_EU_BASE` to a
  disjoint range (64 on rapids4).
- `--l7` keeps the proxy's HTTP/2 termination; without it the policy is
  opaque (L4).
