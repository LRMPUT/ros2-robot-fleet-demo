#!/usr/bin/env python3
"""Analyze fleet latency capture artifacts.

Reads `consumer.jsonl` and `publisher/publisher_robot_*.jsonl` from a capture
output directory and reports per-stream latency percentiles, throughput, and
drop rate. Drop rate joins on the (robot_id, suffix, t0_ns) set so duplicate
deliveries (MQTT QoS 1) do not inflate the matched count.

Usage:
    python3 tools/analyze_latency.py latency_artifacts/<run>/
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import statistics
from collections import defaultdict
from typing import Optional


def _read_jsonl(path: str):
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if line:
                yield json.loads(line)


def _percentile(sorted_vals, q: float) -> float:
    """Nearest-rank percentile on an already-sorted list. q in [0, 1]."""
    if not sorted_vals:
        return 0.0
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    idx = q * (len(sorted_vals) - 1)
    lo = int(idx)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = idx - lo
    return sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac


def analyze(output_dir: str) -> dict:
    consumer_path = os.path.join(output_dir, "consumer.jsonl")
    pub_dir = os.path.join(output_dir, "publisher")

    # Consumer side: latency samples + received t0 sets, keyed by suffix.
    lat_ms_by_suffix = defaultdict(list)
    recv_t0_by_suffix = defaultdict(set)
    t1_min = None
    t1_max = None
    if os.path.exists(consumer_path):
        for rec in _read_jsonl(consumer_path):
            suffix = rec["suffix"]
            lat_ms_by_suffix[suffix].append(rec["latency_ns"] / 1e3)
            recv_t0_by_suffix[suffix].add((rec["robot_id"], rec["t0_ns"]))
            t1 = rec["t1_ns"]
            t1_min = t1 if t1_min is None else min(t1_min, t1)
            t1_max = t1 if t1_max is None else max(t1_max, t1)

    # Publisher side: published t0 sets, keyed by suffix (may be absent).
    pub_t0_by_suffix = defaultdict(set)
    have_pub = os.path.isdir(pub_dir)
    if have_pub:
        for path in sorted(glob.glob(os.path.join(pub_dir, "publisher_robot_*.jsonl"))):
            for rec in _read_jsonl(path):
                pub_t0_by_suffix[rec["suffix"]].add((rec["robot_id"], rec["t0_ns"]))

    window_s = ((t1_max - t1_min) / 1e9) if (t1_min is not None and t1_max is not None and t1_max > t1_min) else None

    by_suffix = {}
    for suffix, samples in sorted(lat_ms_by_suffix.items()):
        s = sorted(samples)
        published: Optional[int] = None
        matched: Optional[int] = None
        drop_rate: Optional[float] = None
        if have_pub and suffix in pub_t0_by_suffix:
            pub_set = pub_t0_by_suffix[suffix]
            published = len(pub_set)
            matched = len(pub_set & recv_t0_by_suffix[suffix])
            drop_rate = (published - matched) / published if published else None
        by_suffix[suffix] = {
            "count": len(s),
            "p50_ms": round(_percentile(s, 0.50), 3),
            "p95_ms": round(_percentile(s, 0.95), 3),
            "p99_ms": round(_percentile(s, 0.99), 3),
            "max_ms": round(s[-1], 3) if s else 0.0,
            "mean_ms": round(statistics.fmean(s), 3) if s else 0.0,
            "throughput_msg_s": round(len(s) / window_s, 1) if window_s else None,
            "published": published,
            "matched": matched,
            "drop_rate": drop_rate,
        }

    total = sum(v["count"] for v in by_suffix.values())
    return {
        "output_dir": output_dir,
        "window_s": round(window_s, 1) if window_s else None,
        "total_received": total,
        "have_publisher_logs": have_pub,
        "by_suffix": by_suffix,
    }


def _print_report(report: dict) -> None:
    print(f"Capture: {report['output_dir']}")
    win = report["window_s"]
    print(f"Window : {win}s   Total received: {report['total_received']:,}")
    if not report["have_publisher_logs"]:
        print("(no publisher logs found — drop rate unavailable)")
    print()
    hdr = (f"{'suffix':<8} {'count':>8} {'p50':>8} {'p95':>8} {'p99':>8} "
           f"{'max':>8} {'msg/s':>8} {'drop%':>7}")
    print(hdr)
    print("-" * len(hdr))
    for suffix, v in report["by_suffix"].items():
        drop = "   n/a" if v["drop_rate"] is None else f"{v['drop_rate'] * 100:6.2f}"
        tput = "   n/a" if v["throughput_msg_s"] is None else f"{v['throughput_msg_s']:8.1f}"
        print(f"{suffix:<8} {v['count']:>8,} {v['p50_ms']:>8.2f} "
              f"{v['p95_ms']:>8.2f} {v['p99_ms']:>8.2f} {v['max_ms']:>8.2f} "
              f"{tput} {drop:>7}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze fleet latency capture artifacts.")
    parser.add_argument("output_dir", help="Capture output directory (contains consumer.jsonl)")
    args = parser.parse_args()
    _print_report(analyze(args.output_dir))


if __name__ == "__main__":
    main()
