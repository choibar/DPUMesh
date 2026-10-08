#!/usr/bin/env bash
# ladder benches (C side). Run on the DPU, pin a core: taskset -c 12 ./<bin> ...
cd "$(dirname "$0")"
gcc -O2 -Wall -I. -o nghttp2_bench nghttp2_bench.c /lib/aarch64-linux-gnu/libnghttp2.so.14
gcc -O2 -Wall -I../../../src/transport/device -I../../../src/transport/device/benchmarks -o dsb_bench dsb_bench.c
echo "built: nghttp2_bench (h2 termination pair), dsb_bench (walker/lean-C hpack)"
