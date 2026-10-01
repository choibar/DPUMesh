#!/bin/bash
# build.sh: on the DPU, build the DPUMesh transport archives, then
# linkerd2-proxy (release, default features) and the mock control plane, in
# the DPUMesh checkout this directory sits in.
set -eux
D=$(cd "$(dirname "$0")/../../.." && pwd)
cd "$D/src/transport"
[ -d build ] || meson setup build
ninja -C build
cd "$D/linkerd2-proxy"
export PATH=$HOME/.cargo/bin:$PATH RUSTFLAGS="--cfg tokio_unstable -C target-cpu=native"
nice -n 19 cargo rustc -j 8 -p linkerd2-proxy --release --bin linkerd2-proxy -- \
    -C lto=thin -C codegen-units=16 \
    -C link-arg=-L/opt/mellanox/doca/lib/aarch64-linux-gnu -C link-arg=-L/opt/mellanox/flexio/lib \
    -C link-arg=-ldoca_common -C link-arg=-ldoca_dpa -C link-arg=-lflexio
nice -n 19 cargo build -j 8 --release -p linkerd-app-integration \
    --bin mock-identity --bin mock-destination --bin mock-policy
