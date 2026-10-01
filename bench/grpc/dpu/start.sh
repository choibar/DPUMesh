#!/bin/bash
# start.sh mocks | proxy <tag>: start the mock control plane or the proxy in
# the background, pinned to MOCK_CPUS and PROXY_CPUS. The proxy logs to
# run/proxy-<tag>.log; `start.sh proxy` returns once its admin endpoint is
# ready, or fails after 30 s.
set -eu
source "$(dirname "$0")/env.sh"
cd "$P"
case "$1" in
    mocks)
        for m in mock-identity mock-policy mock-destination; do
            nohup taskset -c "${MOCK_CPUS:-7,8}" target/release/$m > "$R/$m.log" 2>&1 < /dev/null &
            echo $! > "$R/$m.pid"
        done ;;
    proxy)
        nohup taskset -c "${PROXY_CPUS:-11}" target/release/linkerd2-proxy > "$R/proxy-$2.log" 2>&1 < /dev/null &
        pid=$!
        echo $pid > "$R/proxy.pid"
        for _ in $(seq 1 300); do
            kill -0 $pid 2>/dev/null || { echo "proxy exited" >&2; exit 1; }
            curl -sf --max-time 1 "http://$LINKERD2_PROXY_ADMIN_LISTEN_ADDR/ready" > /dev/null && exit 0
            sleep 0.1
        done
        echo "proxy not ready after 30s" >&2; exit 1 ;;
    *) echo "usage: $0 mocks | proxy <tag>" >&2; exit 2 ;;
esac
