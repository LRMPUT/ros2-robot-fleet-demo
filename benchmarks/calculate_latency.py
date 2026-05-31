#!/usr/bin/env python3
"""Compute latency statistics from a run_tests.py result file.

Reports count / avg / P50 / P95 / P99 / min / max / std for a chosen stage
pair, plus the breakdown of each consecutive hop. Mirrors the GeoFlink
benchmark's calculate_latency output so the numbers line up side by side.

Stages: t0 (event) -> t1 (ingest) -> t2 (ksqlDB) -> t3 (arrival). Timestamps in
the file are nanoseconds (t0/t1 from the dispatcher `_ts` envelope); all
reported deltas are converted to milliseconds.

Usage:
    python calculate_latency.py results/run_10.txt
    python calculate_latency.py results/run_10.txt --from t0 --to t3
    python calculate_latency.py results/run_50.txt --warmup 5
"""
from __future__ import annotations

import argparse
import statistics as stats

import config

COLS = {
    "t0": "t0_event_ns",
    "t1": "t1_ingest_ns",
    "t2": "t2_ksql_ns",
    "t3": "t3_arrival_ns",
}
ORDER = ["t0", "t1", "t2", "t3"]
NS_PER_MS = 1_000_000


def _load(path: str):
    rows = []
    with open(path, encoding="utf-8") as f:
        header = f.readline().split()
        idx = {name: header.index(name) for name in COLS.values() if name in header}
        for line in f:
            parts = line.split()
            if len(parts) < len(header):
                continue
            try:
                row = {k: int(parts[idx[v]]) for k, v in COLS.items()}
                row["robot_id"] = parts[0]
                rows.append(row)
            except (KeyError, ValueError, IndexError):
                continue  # skip malformed / partial rows
    return rows


def _drop_warmup(rows, warmup_s: int):
    """Drop rows whose arrival (t3) is within warmup_s of the first arrival."""
    if not rows or warmup_s <= 0:
        return rows
    start = min(r["t3"] for r in rows)
    cutoff = start + warmup_s * 1000 * NS_PER_MS
    return [r for r in rows if r["t3"] >= cutoff]


def _summary(name: str, deltas_ns: list[int]) -> None:
    if not deltas_ns:
        print(f"  {name:<14} (no samples)")
        return
    # Convert ns deltas to ms for reporting.
    deltas = sorted(d / NS_PER_MS for d in deltas_ns)

    def pct(p):
        idx = min(len(deltas) - 1, int(round(p / 100 * (len(deltas) - 1))))
        return deltas[idx]

    print(f"  {name:<14} "
          f"n={len(deltas):<6} "
          f"avg={stats.mean(deltas):8.2f}  "
          f"p50={pct(50):8.2f}  "
          f"p95={pct(95):8.2f}  "
          f"p99={pct(99):8.2f}  "
          f"min={min(deltas):8.2f}  "
          f"max={max(deltas):8.2f}  "
          f"std={(stats.pstdev(deltas) if len(deltas) > 1 else 0.0):8.2f}  (ms)")


def report(path: str, frm: str, to: str, warmup_s: int) -> None:
    rows = _load(path)
    total = len(rows)
    rows = _drop_warmup(rows, warmup_s)

    print(f"\n=== latency report: {path} ===")
    print(f"rows={total}  after warmup({warmup_s}s)={len(rows)}\n")

    if not rows:
        print("no rows to analyze.")
        return

    # End-to-end (or selected) pair.
    a, b = COLS[frm], COLS[to]
    e2e = [r[to] - r[frm] for r in rows]
    print(f"end-to-end  {frm} -> {to}  ({a} -> {b}):")
    _summary(f"{frm}->{to}", e2e)

    # Per-hop breakdown for every consecutive stage pair.
    print("\nper-hop breakdown:")
    for i in range(len(ORDER) - 1):
        s0, s1 = ORDER[i], ORDER[i + 1]
        hop = [r[s1] - r[s0] for r in rows]
        _summary(f"{s0}->{s1}", hop)

    # Throughput over the observed window (using arrival times).
    span_ns = max(r["t3"] for r in rows) - min(r["t3"] for r in rows)
    if span_ns > 0:
        span_s = span_ns / 1e9
        print(f"\nthroughput: {len(rows) / span_s:.1f} alerts/s "
              f"over {span_s:.1f}s")

    # Completeness: (robot_id, t0) is unique per GPS sample. Under at_least_once
    # a record can be re-emitted, so rows >= unique keys.
    keys = {(r["robot_id"], r["t0"]) for r in rows}
    dups = len(rows) - len(keys)
    if dups:
        print(f"completeness: {len(keys)} unique alerts, {dups} duplicates "
              f"({len(rows) / len(keys):.2f}x) — re-run after a clean stop_fleet.sh")
    else:
        print(f"completeness: {len(keys)} unique alerts, no duplicates")


def main() -> None:
    p = argparse.ArgumentParser(description="Latency stats for a benchmark result file")
    p.add_argument("path", help="path to a run_tests.py output file")
    p.add_argument("--from", dest="frm", choices=ORDER, default="t0")
    p.add_argument("--to", dest="to", choices=ORDER, default="t3")
    p.add_argument("--warmup", type=int, default=config.WARMUP_SECONDS,
                   help=f"seconds to drop from the start (default {config.WARMUP_SECONDS})")
    args = p.parse_args()
    if ORDER.index(args.frm) >= ORDER.index(args.to):
        p.error("--from must be an earlier stage than --to")
    report(args.path, args.frm, args.to, args.warmup)


if __name__ == "__main__":
    main()
