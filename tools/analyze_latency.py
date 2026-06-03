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
import csv
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


def _derive_broker(topic: str) -> str:
    """Infer the broker from a consumer topic string.

    Kafka topics use dots (`ros2.robot_3.gnss`); MQTT uses slashes
    (`ros2/robot_3/gnss`). The publisher topic (`/robot_3/gnss`) is the same
    for both brokers, so derivation must use the consumer topic.
    """
    if ".robot_" in topic:
        return "Kafka"
    if "/robot_" in topic:
        return "MQTT"
    return "Unknown"


def analyze(output_dir: str) -> dict:
    consumer_path = os.path.join(output_dir, "consumer.jsonl")
    pub_dir = os.path.join(output_dir, "publisher")

    # Consumer side: latency samples, stage samples, and received t0 sets,
    # keyed by suffix.
    lat_ms_by_suffix = defaultdict(list)
    ingest_ms_by_suffix = defaultdict(list)
    transport_ms_by_suffix = defaultdict(list)
    recv_t0_by_suffix = defaultdict(set)
    all_lat_ms: list[float] = []      # pooled across all suffixes (run-level stats)
    robot_ids_seen: set[int] = set()
    brokers_seen: set[str] = set()
    t2_min = None
    t2_max = None
    if os.path.exists(consumer_path):
        for rec in _read_jsonl(consumer_path):
            suffix = rec["suffix"]
            lat_ms = rec["latency_ns"] / 1e6
            lat_ms_by_suffix[suffix].append(lat_ms)
            all_lat_ms.append(lat_ms)
            recv_t0_by_suffix[suffix].add((rec["robot_id"], rec["t0_ns"]))
            robot_ids_seen.add(rec["robot_id"])
            brokers_seen.add(_derive_broker(rec.get("topic", "")))
            # Older artifacts predate the t2_ns rename; degrade gracefully.
            t2 = rec.get("t2_ns")
            if t2 is not None:
                t2_min = t2 if t2_min is None else min(t2_min, t2)
                t2_max = t2 if t2_max is None else max(t2_max, t2)
            # Sink stamp t1_ns is None for MQTT (Phase 1); skip its stages.
            t1 = rec.get("t1_ns")
            if t1 is not None and t2 is not None:
                ingest_ms_by_suffix[suffix].append((t1 - rec["t0_ns"]) / 1e6)
                transport_ms_by_suffix[suffix].append((t2 - t1) / 1e6)

    # Publisher side: published t0 sets, keyed by suffix (may be absent).
    pub_t0_by_suffix = defaultdict(set)
    have_pub = os.path.isdir(pub_dir)
    pub_files = 0
    if have_pub:
        for path in sorted(glob.glob(os.path.join(pub_dir, "publisher_robot_*.jsonl"))):
            pub_files += 1
            for rec in _read_jsonl(path):
                pub_t0_by_suffix[rec["suffix"]].add((rec["robot_id"], rec["t0_ns"]))
                robot_ids_seen.add(rec["robot_id"])

    window_s = ((t2_max - t2_min) / 1e9) if (t2_min is not None and t2_max is not None and t2_max > t2_min) else None

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
        ingest = sorted(ingest_ms_by_suffix.get(suffix, []))
        transport = sorted(transport_ms_by_suffix.get(suffix, []))
        by_suffix[suffix] = {
            "count": len(s),
            "p50_ms": round(_percentile(s, 0.50), 3),
            "p95_ms": round(_percentile(s, 0.95), 3),
            "p99_ms": round(_percentile(s, 0.99), 3),
            "max_ms": round(s[-1], 3) if s else 0.0,
            "mean_ms": round(statistics.fmean(s), 3) if s else 0.0,
            "throughput_msg_s": round(len(s) / window_s, 1) if window_s else None,
            "staged_count": len(ingest),
            "ingest_p50_ms": round(_percentile(ingest, 0.50), 3) if ingest else None,
            "transport_p50_ms": round(_percentile(transport, 0.50), 3) if transport else None,
            "published": published,
            "matched": matched,
            "drop_rate": drop_rate,
        }

    total = sum(v["count"] for v in by_suffix.values())

    # ── run-level aggregates (one row per capture for the paper table) ─────────
    # Expected = unique published (robot_id, t0_ns) pairs across all suffixes.
    # Received = total consumer records (QoS-1 duplicates included, matching the
    #   paper-table convention). Delivery is clamped at 100% because duplicate
    #   deliveries can push received above expected.
    expected: Optional[int] = None
    if have_pub:
        expected = sum(len(s) for s in pub_t0_by_suffix.values())
    matched_total: Optional[int] = None
    if have_pub:
        matched_total = sum(
            v["matched"] for v in by_suffix.values() if v["matched"] is not None
        )
    delivery_pct: Optional[float] = None
    if expected:
        delivery_pct = round(min(100.0, total / expected * 100.0), 1)

    pooled = sorted(all_lat_ms)
    broker = brokers_seen.pop() if len(brokers_seen) == 1 else (
        "Mixed" if len(brokers_seen) > 1 else "Unknown"
    )
    n_robots = pub_files if have_pub else len(robot_ids_seen)

    return {
        "output_dir": output_dir,
        "run_name": os.path.basename(os.path.normpath(output_dir)),
        "broker": broker,
        "n_robots": n_robots,
        "window_s": round(window_s, 1) if window_s else None,
        "total_received": total,
        "received": total,
        "expected": expected,
        "matched_total": matched_total,
        "delivery_pct": delivery_pct,
        "avg_ms": round(statistics.fmean(pooled), 3) if pooled else 0.0,
        "p50_ms": round(_percentile(pooled, 0.50), 3) if pooled else 0.0,
        "p95_ms": round(_percentile(pooled, 0.95), 3) if pooled else 0.0,
        "p99_ms": round(_percentile(pooled, 0.99), 3) if pooled else 0.0,
        "have_publisher_logs": have_pub,
        "by_suffix": by_suffix,
    }


# ── CSV export ────────────────────────────────────────────────────────────────

CSV_COLUMNS = [
    "run_name", "broker", "n_robots", "level", "suffix",
    "received", "expected", "matched", "delivery_pct",
    "avg_ms", "p50_ms", "p95_ms", "p99_ms",
    "count", "max_ms", "throughput_msg_s", "drop_rate",
]


def write_csv(report: dict, csv_path: str, append: bool = False) -> None:
    """Write per-suffix rows plus one run-level aggregate row to CSV.

    The run-level row (level="run", empty suffix) is what the paper-table
    generator consumes; per-suffix rows (level="suffix") preserve detail.
    With append=True the header is written only when the file is new or empty.
    """
    write_header = True
    mode = "w"
    if append:
        mode = "a"
        write_header = not (os.path.exists(csv_path) and os.path.getsize(csv_path) > 0)

    with open(csv_path, mode, newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        if write_header:
            w.writeheader()

        # run-level aggregate
        w.writerow({
            "run_name": report["run_name"],
            "broker": report["broker"],
            "n_robots": report["n_robots"],
            "level": "run",
            "suffix": "",
            "received": report["received"],
            "expected": report["expected"],
            "matched": report["matched_total"],
            "delivery_pct": report["delivery_pct"],
            "avg_ms": report["avg_ms"],
            "p50_ms": report["p50_ms"],
            "p95_ms": report["p95_ms"],
            "p99_ms": report["p99_ms"],
            "count": report["total_received"],
            "max_ms": round(max(
                (v["max_ms"] for v in report["by_suffix"].values()), default=0.0), 3),
            "throughput_msg_s": "",
            "drop_rate": "",
        })

        # per-suffix detail
        for suffix, v in report["by_suffix"].items():
            w.writerow({
                "run_name": report["run_name"],
                "broker": report["broker"],
                "n_robots": report["n_robots"],
                "level": "suffix",
                "suffix": suffix,
                "received": v["count"],
                "expected": v["published"],
                "matched": v["matched"],
                "delivery_pct": (
                    round(min(100.0, v["count"] / v["published"] * 100.0), 1)
                    if v["published"] else ""
                ),
                "avg_ms": v["mean_ms"],
                "p50_ms": v["p50_ms"],
                "p95_ms": v["p95_ms"],
                "p99_ms": v["p99_ms"],
                "count": v["count"],
                "max_ms": v["max_ms"],
                "throughput_msg_s": (
                    v["throughput_msg_s"] if v["throughput_msg_s"] is not None else ""
                ),
                "drop_rate": v["drop_rate"] if v["drop_rate"] is not None else "",
            })


def _print_report(report: dict) -> None:
    print(f"Capture: {report['output_dir']}")
    win = report["window_s"]
    exp = report["expected"]
    deliv = report["delivery_pct"]
    deliv_s = "n/a" if deliv is None else f"{deliv:.1f}%"
    exp_s = "n/a" if exp is None else f"{exp:,}"
    print(f"Broker : {report['broker']}   Robots: {report['n_robots']}")
    print(f"Window : {win}s   Received: {report['received']:,}   "
          f"Expected: {exp_s}   Delivery: {deliv_s}")
    print(f"Pooled : avg={report['avg_ms']:.2f}  p50={report['p50_ms']:.2f}  "
          f"p95={report['p95_ms']:.2f}  p99={report['p99_ms']:.2f} ms")
    if not report["have_publisher_logs"]:
        print("(no publisher logs found — drop rate unavailable)")
    print()
    hdr = (f"{'suffix':<8} {'count':>8} {'p50':>8} {'p95':>8} {'p99':>8} "
           f"{'max':>8} {'msg/s':>8} {'drop%':>7} {'ingest':>8} {'transp':>8}")
    print(hdr)
    print("-" * len(hdr))
    for suffix, v in report["by_suffix"].items():
        drop = "n/a" if v["drop_rate"] is None else f"{v['drop_rate'] * 100:.2f}"
        tput = "n/a" if v["throughput_msg_s"] is None else f"{v['throughput_msg_s']:.1f}"
        ing = "n/a" if v["ingest_p50_ms"] is None else f"{v['ingest_p50_ms']:.2f}"
        tra = "n/a" if v["transport_p50_ms"] is None else f"{v['transport_p50_ms']:.2f}"
        print(f"{suffix:<8} {v['count']:>8,} {v['p50_ms']:>8.2f} "
              f"{v['p95_ms']:>8.2f} {v['p99_ms']:>8.2f} {v['max_ms']:>8.2f} "
              f"{tput:>8} {drop:>7} {ing:>8} {tra:>8}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze fleet latency capture artifacts.")
    parser.add_argument("output_dir", help="Capture output directory (contains consumer.jsonl)")
    parser.add_argument("--csv", metavar="PATH", help="Write metrics to CSV at PATH")
    parser.add_argument("--append", action="store_true",
                        help="Append to --csv (header written only if file is new/empty)")
    parser.add_argument("--quiet", action="store_true",
                        help="Suppress the human-readable table")
    args = parser.parse_args()

    report = analyze(args.output_dir)
    if args.csv:
        write_csv(report, args.csv, append=args.append)
    if not args.quiet:
        _print_report(report)


if __name__ == "__main__":
    main()
