#!/bin/bash
# stop.sh <name...>: SIGTERM each recorded pid (proxy, mock-identity, ...),
# wait up to 10 s, and report how it ended.
R=$(cd "$(dirname "$0")" && pwd)/run
for n in "$@"; do
    p=$(cat "$R/$n.pid" 2>/dev/null) || continue
    kill -TERM "$p" 2>/dev/null || { echo "$n: not running"; continue; }
    for _ in $(seq 1 20); do kill -0 "$p" 2>/dev/null || break; sleep 0.5; done
    if kill -0 "$p" 2>/dev/null; then echo "$n: still alive after 10s, KILL"; kill -KILL "$p"; else echo "$n: exited"; fi
done
