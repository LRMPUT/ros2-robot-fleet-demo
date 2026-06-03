#!/usr/bin/env python3
"""One-command orchestrator: setup -> settle -> collect -> stats for a single N.

Start the fleet (launch_fleet.sh) first, then run this. See README-benchmarks.md.

The "settle" step lets the new geofence queries reach steady state before the
collector takes its `latest` offset, so only live alerts are measured.

Usage:
    python run_benchmark.py --robots 10 --seconds 60
    python run_benchmark.py --robots 50 --seconds 120 --settle 15 --from t0 --to t3
"""
from __future__ import annotations

import argparse
import os
import time

import calculate_latency
import config
import run_tests
import setup_geoflink


def main() -> None:
    p = argparse.ArgumentParser(description="setup + settle + collect + stats for one run")
    p.add_argument("--robots", type=int, default=config.NUM_ROBOTS)
    p.add_argument("--seconds", type=int, default=config.COLLECT_SECONDS)
    p.add_argument("--api-url", default=config.API_URL)
    p.add_argument("--bootstrap", default=config.KAFKA_BOOTSTRAP)
    p.add_argument("--settle", type=int, default=None,
                   help="seconds to let queries reach steady state before "
                        "collecting (default: scales with --robots, like the fleet)")
    p.add_argument("--from", dest="frm", default="t0")
    p.add_argument("--to", dest="to", default="t3")
    p.add_argument("--warmup", type=int, default=config.WARMUP_SECONDS)
    p.add_argument("--out", default=None,
                   help="output path (default results/run_<robots>.txt)")
    p.add_argument("--skip-setup", action="store_true",
                   help="reuse an already-configured stack")
    args = p.parse_args()

    settle = args.settle if args.settle is not None else config.settle_seconds(args.robots)

    if not args.skip_setup:
        setup_geoflink.setup(args.api_url, args.robots)
        if settle > 0:
            print(f"[settle] waiting {settle}s for queries to reach steady state...")
            time.sleep(settle)

    out_path = args.out or os.path.join(config.OUTPUT_DIR, f"run_{args.robots}.txt")
    run_tests.collect(args.bootstrap, config.ALERT_TOPIC,
                      args.seconds, out_path, args.robots)

    calculate_latency.report(out_path, args.frm, args.to, args.warmup)


if __name__ == "__main__":
    main()
