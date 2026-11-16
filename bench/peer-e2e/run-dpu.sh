#!/bin/bash
# Foreground DPU process for the native Host -> L7 -> RDMA -> L7 -> Host fixture.
# Kubernetes membership/Pod identity is real. The benchmark's L7 policy and
# identity control-plane services are the target repo's development mocks.
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
export LD_LIBRARY_PATH="$root/lib:/usr/local/lib/dpumesh:${LD_LIBRARY_PATH:-}"
: "${DPUMESH_NODE_NAME:?paired Host Kubernetes node name}"
: "${DPUMESH_PEER_RDMA_DEVICE:?verbs device}"
: "${DPUMESH_PEER_GID_INDEX:?RoCE GID index}"
export DMESH_PEER_REGISTRATION=1 DMESH_SHARDED=1
export DPUMESH_PEER_TRANSPORT=${DPUMESH_PEER_TRANSPORT:-rdma-write-imm}
export DMESH_PEER_DPA=${DMESH_PEER_DPA:-1}
export DMESH_NUM_WORKERS=${DMESH_NUM_WORKERS:-1}
export DMESH_BUSY_POLL=${DMESH_BUSY_POLL:-0}
export DPUMESH_CLUSTER_ID=${DPUMESH_CLUSTER_ID:-dpumesh-test}
export DPUMESH_TOPOLOGY_FILE=${DPUMESH_TOPOLOGY_FILE:-/etc/dpumesh/feeds/topology.v1}
export DPUMESH_CONTROLLER_KEY_DIR=${DPUMESH_CONTROLLER_KEY_DIR:-/etc/dpumesh/controller.pub.keys}
export DPUMESH_NODE_KEY=${DPUMESH_NODE_KEY:-/etc/dpumesh/node-static.key}
export DPUMESH_PEER_CRYPTO=${DPUMESH_PEER_CRYPTO:-unix:/run/dpumesh-runtime/crypto.sock}
export DPUMESH_LOCAL_ADDRESS=${DPUMESH_LOCAL_ADDRESS:-192.168.100.2}
export DPUMESH_LOCAL_PORT=${DPUMESH_LOCAL_PORT:-4791}
export DPUMESH_LOCAL_CA=${DPUMESH_LOCAL_CA:-/etc/dpumesh/local-tls/ca.crt}
export DPUMESH_LOCAL_CERT=${DPUMESH_LOCAL_CERT:-/etc/dpumesh/local-tls/dpu-server.crt}
export DPUMESH_LOCAL_KEY=${DPUMESH_LOCAL_KEY:-/etc/dpumesh/local-tls/dpu-server.key}
export DPUMESH_LOCAL_HOST_URI="spiffe://dpumesh.io/node/$DPUMESH_NODE_NAME"
export LINKERD2_PROXY_ROOT="$root/linkerd2-proxy"
# Match jukebox's shared-subscriber baseline, rather than suppressing its spans
# by accidentally using the native benchmark's narrower warn-only filter.
export LINKERD2_PROXY_LOG=${LINKERD2_PROXY_LOG:-warn,linkerd=info,hickory=error}
source "$LINKERD2_PROXY_ROOT/scripts/dev-proxy-env.sh" >/dev/null
export MOCK_IDENTITY_ADDR=127.0.0.1:17088 MOCK_POLICY_ADDR=127.0.0.1:17087 MOCK_DESTINATION_ADDR=127.0.0.1:17089
export MOCK_IDENTITY_DATA_DIR="$LINKERD2_PROXY_ROOT/linkerd/app/integration/src/data"
export MOCK_POLICY_ECHO_TARGET=1
unset MOCK_OUTBOUND_OPAQUE
export LINKERD2_PROXY_IDENTITY_SVC_ADDR=$MOCK_IDENTITY_ADDR
export LINKERD2_PROXY_POLICY_SVC_ADDR=$MOCK_POLICY_ADDR
export LINKERD2_PROXY_DESTINATION_SVC_ADDR=$MOCK_DESTINATION_ADDR
export LINKERD2_PROXY_POLICY_WORKLOAD=native-peer-bench
export LINKERD2_PROXY_OUTBOUND_LISTEN_ADDR=127.0.0.1:17140 LINKERD2_PROXY_INBOUND_LISTEN_ADDR=127.0.0.1:17143
export LINKERD2_PROXY_ADMIN_LISTEN_ADDR=0.0.0.0:17191 LINKERD2_PROXY_CONTROL_LISTEN_ADDR=127.0.0.1:17190
export LINKERD2_PROXY_INBOUND_DEFAULT_POLICY=all-unauthenticated
export LINKERD2_PROXY_DOCA_DEV_PCI_ADDR=${DPU_PCI:-03:00.0}
export LINKERD2_PROXY_DOCA_REP_PCI_ADDR=${REP_PCI:-0b:00.0}
export LINKERD2_PROXY_DOCA_SERVER_NAME=DPUMesh0 LINKERD2_PROXY_CORES=1
unset DMESH_NO_TEARDOWN
run=${PEER_BENCH_RUN_DIR:-/run/dpumesh-runtime/native}
mkdir -p "$run"
for m in mock-identity mock-policy mock-destination; do
    taskset -c "${MOCK_CPUS:-0-1}" "$LINKERD2_PROXY_ROOT/target/release/$m" >"$run/$m.log" 2>&1 &
done
ulimit -l unlimited
exec taskset -c "${PROXY_CPUS:-4}" "$LINKERD2_PROXY_ROOT/target/release/linkerd2-proxy"
