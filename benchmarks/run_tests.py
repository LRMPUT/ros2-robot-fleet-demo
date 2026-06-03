#!/usr/bin/env python3
"""Collect geofence alerts and record the t0..t3 timestamp chain to a file.

Subscribes to robot_geofence_alerts and, for every alert, writes one TSV row:

    robot_id  ts_0  ts_1  ts_2  ts_3

  t0 event   -- dispatcher publish time, ns (_ts.t0_ns)     (alert field ts_0)
  t1 ingest  -- broker ingest of the GNSS record, ns (_ts.t1_ns) (alert field ts_1)
  t2 geoflink  -- wall clock when geoflink emitted the alert, ns (alert field ts_2)
  t3 arrival -- wall clock when this consumer received it, ns (stamped here)

The dispatcher stamps t0/t1 in nanoseconds in the `_ts` envelope on
ros2.fleet.gnss; the geofence query forwards them. t2 is ms-resolution
(geoflink UNIX_TIMESTAMP) scaled to ns. The (robot_id, ts_0) pair is unique
per input GPS sample, so rows join/dedup across engines.

Uses confluent-kafka (librdkafka) -- the same client as the parent repo's
consumer/consume.py, and one that ships wheels for modern Python.

Usage:
    python run_tests.py --robots 10 --seconds 60
    python run_tests.py --robots 50 --seconds 120 --out results/run_50.txt
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import time

from confluent_kafka import Consumer, KafkaError

import config

_stop = False


def _handle_sigint(*_):
    global _stop
    _stop = True


signal.signal(signal.SIGINT, _handle_sigint)
signal.signal(signal.SIGTERM, _handle_sigint)

HEADER = ["robot_id", "ts_0", "ts_1",
          "ts_2", "ts_3"]

# Fixed-width columns so header and ns values (19 digits) align in any editor.
# Still whitespace-separated, so calculate_latency.py parses with str.split().
_W_ID = 18
_W_TS = 22


def _fmt_row(fields: list[str]) -> str:
    cells = [fields[0].ljust(_W_ID)] + [c.ljust(_W_TS) for c in fields[1:]]
    return "".join(cells).rstrip()


def collect(bootstrap: str, topic: str, seconds: int, out_path: str, n: int) -> None:
    os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"bench-collector-{int(time.time())}",
        "auto.offset.reset": "latest",   # only alerts produced during this run
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])

    print(f"[collect] topic={topic}  robots={n}  duration={seconds}s")
    print(f"[collect] writing -> {out_path}")
    print("[collect] waiting for alerts (start the fleet/rosbag now if idle)...")

    count = 0
    deadline = time.time() + seconds
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(_fmt_row(HEADER) + "\n")
        while not _stop and time.time() < deadline:
            msg = consumer.poll(timeout=0.5)
            if msg is None:
                continue
            if msg.error():
                # UNKNOWN_TOPIC_OR_PART is expected noise until the topic exists.
                if msg.error().code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                    print(f"[collect] kafka error: {msg.error()}")
                continue

            t3_ns = time.time_ns()
            try:
                rec = json.loads(msg.value().decode("utf-8"))
            except Exception:
                continue
            # Status rows from a disabled stream carry msg == "OFF".
            if rec.get("msg") == "OFF":
                continue

            row = [
                str(rec.get("robot") or rec.get("ROBOT_ID") or ""),
                str(rec.get("ts_0", "")),
                str(rec.get("ts_1", "")),
                str(rec.get("ts_2", "")),
                str(t3_ns),
            ]
            f.write(_fmt_row(row) + "\n")
            count += 1
            if count % 200 == 0:
                f.flush()
                print(f"[collect]   {count} alerts...")

    consumer.close()
    print(f"[collect] done. {count} alerts written to {out_path}")


def main() -> None:
    p = argparse.ArgumentParser(description="Collect geofence alerts with t0..t3 timestamps")
    p.add_argument("--robots", type=int, default=config.NUM_ROBOTS,
                   help="number of robots in this run (for the output filename/metadata)")
    p.add_argument("--seconds", type=int, default=config.COLLECT_SECONDS,
                   help=f"collection duration (default {config.COLLECT_SECONDS})")
    p.add_argument("--bootstrap", default=config.KAFKA_BOOTSTRAP,
                   help=f"Kafka bootstrap servers (default {config.KAFKA_BOOTSTRAP})")
    p.add_argument("--topic", default=config.ALERT_TOPIC)
    p.add_argument("--out", default=None,
                   help="output path (default results/run_<robots>.txt)")
    args = p.parse_args()

    out_path = args.out or os.path.join(config.OUTPUT_DIR, f"run_{args.robots}.txt")
    collect(args.bootstrap, args.topic, args.seconds, out_path, args.robots)


if __name__ == "__main__":
    main()
