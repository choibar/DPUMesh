#!/bin/bash
# Build in an isolated DPUMesh checkout on the ARM DPU. No deployment changes.
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_INCREMENTAL=0
export RUSTFLAGS='--cfg tokio_unstable -C target-cpu=native'
export JEMALLOC_SYS_WITH_LG_PAGE=16
export CARGO_PROFILE_RELEASE_LTO=thin
export CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16
jobs=${BUILD_JOBS:-2}
if [ ! -f "$root/src/transport/build/build.ninja" ]; then
    meson setup "$root/src/transport/build" "$root/src/transport" --buildtype=release
fi
ninja -C "$root/src/transport/build" -j "$jobs"
cd "$root/linkerd2-proxy"
cargo +1.90.0 build --locked --release -j "$jobs" -p linkerd2-proxy
cargo +1.90.0 build --locked --release -j "$jobs" -p linkerd-app-integration \
    --bin mock-identity --bin mock-destination --bin mock-policy
