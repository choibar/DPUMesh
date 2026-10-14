#!/usr/bin/env python3
"""Medians of run.py results per label and condition.

  summarize.py [--csv out.csv] <results dir or .json files...>

Prints a Markdown table: for each (label, transport, L4/L7, connections,
concurrency) the valid runs out of all runs and the medians of RPC/s,
latency p50/p99, client/server/DPU-proxy CPU. Invalid runs are counted, not
averaged. --csv writes one row per run.
"""
import argparse
import csv
import glob
import json
import os
import statistics

FIELDS = ("tag", "label", "transport", "l7", "connections", "concurrency", "ok", "qps",
          "p50_us", "p99_us", "client_cpu_pct", "server_cpu_pct", "proxy_cpu_pct",
          "client_cores_busy_pct", "server_cores_busy_pct", "problems")


def rows(paths):
    files = []
    for p in paths:
        files += sorted(glob.glob(os.path.join(p, "*.json"))) if os.path.isdir(p) else [p]
    for path in files:
        with open(path) as f:
            r = json.load(f)
        if "tag" not in r:
            continue
        c = r.get("client") or {}
        yield {"tag": r["tag"], "label": r.get("label", ""), "transport": r["transport"],
               "l7": {True: "L7", False: "L4", None: "-"}[r.get("l7")],
               "connections": r["connections"], "concurrency": r["concurrency"], "ok": r["ok"],
               "qps": c.get("qps"), "p50_us": c.get("latency_p50_us"), "p99_us": c.get("latency_p99_us"),
               "client_cpu_pct": c.get("client_process_cpu_pct"), "server_cpu_pct": r.get("server_cpu_pct"),
               "proxy_cpu_pct": r.get("proxy_cpu_pct"), "client_cores_busy_pct": r.get("client_cores_busy_pct"),
               "server_cores_busy_pct": r.get("server_cores_busy_pct"), "problems": "; ".join(r["problems"])}


def med(values):
    values = [v for v in values if v is not None]
    return statistics.median(values) if values else None


def fmt(v, digits=0):
    return "–" if v is None else f"{v:,.{digits}f}"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv")
    ap.add_argument("paths", nargs="+")
    a = ap.parse_args()
    all_rows = list(rows(a.paths))
    if a.csv:
        with open(a.csv, "w", newline="") as f:
            w = csv.DictWriter(f, FIELDS)
            w.writeheader()
            w.writerows(all_rows)
    groups = {}
    for r in all_rows:
        key = (r["label"], r["transport"], r["l7"], r["connections"], r["concurrency"])
        groups.setdefault(key, []).append(r)
    print("| label | transport | mode | conns | concurrency | valid | RPC/s | p50 µs | p99 µs "
          "| client CPU % | server CPU % | proxy CPU % |")
    print("|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for key in sorted(groups):
        g = groups[key]
        ok = [r for r in g if r["ok"]]
        m = {k: med(r[k] for r in ok) for k in ("qps", "p50_us", "p99_us", "client_cpu_pct",
                                                "server_cpu_pct", "proxy_cpu_pct")}
        print(f"| {key[0]} | {key[1]} | {key[2]} | {key[3]} | {key[4]} | {len(ok)}/{len(g)} "
              f"| {fmt(m['qps'])} | {fmt(m['p50_us'])} | {fmt(m['p99_us'])} | {fmt(m['client_cpu_pct'])} "
              f"| {fmt(m['server_cpu_pct'])} | {fmt(m['proxy_cpu_pct'])} |")


if __name__ == "__main__":
    main()
