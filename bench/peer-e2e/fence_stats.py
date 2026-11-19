#!/usr/bin/env python3
"""Summarize WRITE_IMM_FENCE_STATS logs; all depths count unretired batches.

The mean and histogram are weighted by batch submissions, not elapsed time.
Percentiles are histogram ranges, never exact percentile measurements.
"""
import argparse
import json
from pathlib import Path
import sys

BUCKETS = [("hist_1", [1, 1]), ("hist_2_4", [2, 4]), ("hist_5_8", [5, 8]),
           ("hist_9_16", [9, 16]), ("hist_17_plus", [17, None])]


def summarize(lines, start_ns=None, end_ns=None):
    groups = {}
    seen = set()
    for line in lines:
        marker = next((m for m in ("WRITE_IMM_FENCE_STATS ", "WRITE_SYNC_FENCE_STATS ")
                       if m in line), None)
        if marker is None:
            continue
        fields = dict(part.split("=", 1) for part in line.split(marker, 1)[1].split() if "=" in part)
        if "samples" not in fields:
            continue
        if fields.get("sample") != "post_commit":
            raise ValueError("unsupported fence sampling method")
        stamp = int(fields["unix_ns"])
        window_start = stamp - round(float(fields["window_ms"]) * 1000000)
        # Keep whole windows, excluding intervals straddling warmup/end trims.
        if start_ns is not None and window_start < start_ns:
            continue
        if end_ns is not None and stamp > end_ns:
            continue
        identity = tuple(fields[k] for k in ("mode", "worker", "eu", "cookie", "lane", "generation"))
        if (identity, stamp) in seen:
            continue
        seen.add((identity, stamp))
        row = groups.setdefault(identity, {
            **dict(zip(("mode", "worker", "eu", "cookie", "lane", "generation"), identity)),
            "windows": 0, "samples": 0, "depth_sum": 0, "max": 0, "full_stalls": 0,
            "histogram": {name: 0 for name, _ in BUCKETS},
        })
        row["windows"] += 1
        row["samples"] += int(fields["samples"])
        row["depth_sum"] += int(fields["depth_sum"])
        row["max"] = max(row["max"], int(fields["max"]))
        row["full_stalls"] += int(fields["full_stalls"])
        row["last_current"] = int(fields["current"])
        row["capacity"] = int(fields["capacity"])
        for name, _ in BUCKETS:
            row["histogram"][name] += int(fields[name])
    for row in groups.values():
        row["avg"] = row["depth_sum"] / row["samples"] if row["samples"] else None
        row["histogram_complete"] = sum(row["histogram"].values()) == row["samples"]
        for percent in (50, 95, 99):
            value = None
            if row["histogram_complete"] and row["samples"]:
                threshold = (row["samples"] * percent + 99) // 100
                count = 0
                for name, bounds in BUCKETS:
                    count += row["histogram"][name]
                    if count >= threshold:
                        value = [bounds[0], bounds[1] if bounds[1] is not None else row["capacity"]]
                        break
            row[f"p{percent}_bucket"] = value
    return list(groups.values())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logs", nargs="*", type=Path, help="runtime logs; stdin if omitted")
    parser.add_argument("--start-unix-ns", type=int)
    parser.add_argument("--end-unix-ns", type=int)
    args = parser.parse_args()
    results = []
    for path in args.logs or [None]:
        stream = path.open() if path else sys.stdin
        try:
            rows = summarize(stream, args.start_unix_ns, args.end_unix_ns)
            for row in rows:
                row["log"] = str(path) if path else "stdin"
            results.extend(rows)
        finally:
            if path:
                stream.close()
    if not results:
        parser.error("no fence statistics in the selected log windows; enable DPUMESH_DPA_FENCE_STATS_MS")
    print(json.dumps({"sampling": "post_commit_batch_weighted", "lanes": results}, indent=2))


if __name__ == "__main__":
    main()
