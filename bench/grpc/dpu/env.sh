# Environment of the DPU linkerd2-proxy and its mock control plane for the
# gRPC benchmarks (../README.md). The proxy is the linkerd2-proxy submodule of
# the DPUMesh checkout this directory sits in, built by build.sh.
B=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
P=$(cd "$B/../../../linkerd2-proxy" && pwd)
export LINKERD2_PROXY_ROOT=$P
# dev-proxy-env.sh defaults the log to linkerd=debug, which logs every request.
export LINKERD2_PROXY_LOG=${LINKERD2_PROXY_LOG:-warn}
source "$P/scripts/dev-proxy-env.sh" >/dev/null
export MOCK_IDENTITY_ADDR=127.0.0.1:17088 MOCK_POLICY_ADDR=127.0.0.1:17087 MOCK_DESTINATION_ADDR=127.0.0.1:17089
# Every flow goes to its original destination; BENCH_L7=1 keeps HTTP/2
# termination, otherwise the policy is opaque (L4).
export MOCK_POLICY_ECHO_TARGET=1
[ "${BENCH_L7:-0}" = 1 ] || export MOCK_OUTBOUND_OPAQUE=1
export LINKERD2_PROXY_IDENTITY_SVC_ADDR=127.0.0.1:17088 LINKERD2_PROXY_DESTINATION_SVC_ADDR=127.0.0.1:17089 LINKERD2_PROXY_POLICY_SVC_ADDR=127.0.0.1:17087
export LINKERD2_PROXY_POLICY_WORKLOAD=grpc-bench
export LINKERD2_PROXY_OUTBOUND_LISTEN_ADDR=127.0.0.1:17140 LINKERD2_PROXY_INBOUND_LISTEN_ADDR=127.0.0.1:17143
export LINKERD2_PROXY_ADMIN_LISTEN_ADDR=127.0.0.1:17191 LINKERD2_PROXY_CONTROL_LISTEN_ADDR=127.0.0.1:17190
export LINKERD2_PROXY_INBOUND_DEFAULT_POLICY=all-unauthenticated
export LINKERD2_PROXY_DOCA_DEV_PCI_ADDR=${DPU_PCI:-03:00.0} LINKERD2_PROXY_DOCA_REP_PCI_ADDR=${REP_PCI:-94:00.0}
export LINKERD2_PROXY_DOCA_SERVER_NAME=${DPUMESH_SERVER:-DPUMeshBench0}
export LINKERD2_PROXY_CORES=1 DMESH_NUM_WORKERS=${DMESH_NUM_WORKERS:-1} DMESH_SHARDED=1 DMESH_BUSY_POLL=${DMESH_BUSY_POLL:-1}
# DPA thread placement (transport default 0). A node whose other PF runs
# another DPA job needs a disjoint range, e.g. 64.
export DPUMESH_DPA_EU_BASE=${DPUMESH_DPA_EU_BASE:-0}
# Service profiles stay at the dev default (127.0.0.0/24): the bench service
# 10.99.1.60 gets no profile lookup, as in the 2026-09-25 measurement. Set
# LINKERD2_PROXY_DESTINATION_PROFILE_NETWORKS to include it to measure that.
unset DMESH_NO_TEARDOWN
R=$B/run
mkdir -p "$R"
