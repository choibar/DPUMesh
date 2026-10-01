#!/bin/bash
# matrix.sh <label> <server-cmd> <client-cmd> [run.py options...]: the
# recorded channel-bench set, REPS rounds of every CONDITIONS entry
# (<connections>x<total concurrency>), rounds outermost so drift spreads over
# all conditions. Tags are <label>-<condition>-r<round>; results land in
# run.py's --out (default results/). A failed run is recorded and the set
# goes on.
set -u
H=$(cd "$(dirname "$0")" && pwd)
label=$1 server=$2 client=$3
shift 3
fails=0
for r in $(seq 1 "${REPS:-3}"); do
    for cond in ${CONDITIONS:-1x64 2x64 3x64 4x64 4x256 1x1}; do
        python3 "$H/run.py" --tag "$label-$cond-r$r" --label "$label" \
            --server "$server" --client "$client" \
            --connections "${cond%x*}" --concurrency "${cond#*x}" "$@" || fails=$((fails + 1))
    done
done
echo "matrix $label: $fails failed run(s)"
[ "$fails" = 0 ]
