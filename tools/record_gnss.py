#!/usr/bin/env python3
"""Record GNSS trajectories from N robots to per-robot text files.

Subscribes to ros2.robot_N.gnss (Kafka) or ros2/robot_N/gnss (MQTT) and
writes one tab-separated file per robot:
    robot_1_gnss.txt:  timestamp_ns\tlatitude\tlongitude\taltitude

Supports JSON and CDR payloads (auto-detected).

Usage:
    # Kafka, robots 1–10, write to ./trajectories/
    python3 tools/record_gnss.py --broker kafka --robots 1-10 --out trajectories/

    # MQTT
    python3 tools/record_gnss.py --broker mqtt --robots 1-10 --out trajectories/

    # Explicit robot list
    python3 tools/record_gnss.py --broker kafka --robots 1,3,5 --out .

    # Duration limit
    python3 tools/record_gnss.py --broker kafka --robots 1-10 --out traj/ --duration 60
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import threading
import time
from collections import defaultdict
from typing import Optional

_stop = threading.Event()
signal.signal(signal.SIGINT,  lambda *_: _stop.set())
signal.signal(signal.SIGTERM, lambda *_: _stop.set())


# ── payload decoding ──────────────────────────────────────────────────────────

def _from_json(payload: bytes) -> Optional[tuple[float, float, float]]:
    try:
        d = json.loads(payload)
        return float(d["latitude"]), float(d["longitude"]), float(d.get("altitude", 0.0))
    except Exception:
        return None


def _from_cdr(payload: bytes) -> Optional[tuple[float, float, float]]:
    try:
        from rclpy.serialization import deserialize_message
        from sensor_msgs.msg import NavSatFix
        msg = deserialize_message(payload, NavSatFix)
        return msg.latitude, msg.longitude, msg.altitude
    except Exception:
        return None


def _decode(payload: bytes, fmt: str, headers=None) -> Optional[tuple[float, float, float]]:
    if fmt == "auto":
        if headers:
            for k, v in headers:
                key = k.decode() if isinstance(k, bytes) else k
                if key == "payload_format":
                    fmt = v.decode() if isinstance(v, bytes) else v
                    break
        if fmt == "auto":
            r = _from_json(payload)
            return r if r is not None else _from_cdr(payload)
    return _from_json(payload) if fmt == "json" else _from_cdr(payload)


# ── file writer ───────────────────────────────────────────────────────────────

class RobotRecorder:
    def __init__(self, out_dir: str, robot_ids: list[int]):
        os.makedirs(out_dir, exist_ok=True)
        self._files: dict[int, object] = {}
        self._counts: dict[int, int] = defaultdict(int)
        self._lock = threading.Lock()
        for rid in robot_ids:
            path = os.path.join(out_dir, f"robot_{rid}_gnss.txt")
            f = open(path, "w", buffering=1)
            f.write("timestamp_ns\tlatitude\tlongitude\taltitude\n")
            self._files[rid] = f
            print(f"  → {path}")

    def record(self, robot_id: int, ts_ns: int, lat: float, lon: float, alt: float) -> None:
        f = self._files.get(robot_id)
        if f is None:
            return
        f.write(f"{ts_ns}\t{lat:.9f}\t{lon:.9f}\t{alt:.4f}\n")
        with self._lock:
            self._counts[robot_id] += 1

    def counts(self) -> dict[int, int]:
        with self._lock:
            return dict(self._counts)

    def close(self) -> None:
        for f in self._files.values():
            f.close()


# ── broker threads ────────────────────────────────────────────────────────────

def _kafka_thread(bootstrap: str, robot_ids: list[int], fmt: str,
                  recorder: RobotRecorder) -> None:
    from confluent_kafka import Consumer, KafkaError
    import re

    topics = [f"ros2.robot_{r}.gnss" for r in robot_ids]
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"gnss-recorder-{int(time.time())}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(topics)
    print(f"[kafka] subscribed to {len(topics)} topics")

    while not _stop.is_set():
        msg = consumer.poll(timeout=0.3)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                print(f"[kafka] {msg.error()}", file=sys.stderr)
            continue
        m = re.match(r"^ros2\.robot_(\d+)\.gnss$", msg.topic())
        if not m:
            continue
        pos = _decode(msg.value(), fmt, msg.headers())
        if pos:
            ts_ns = int(time.time_ns())
            recorder.record(int(m.group(1)), ts_ns, *pos)

    consumer.close()


def _mqtt_thread(host: str, port: int, robot_ids: list[int], fmt: str,
                 recorder: RobotRecorder) -> None:
    import paho.mqtt.client as mqtt
    import re

    def on_message(_c, _u, msg):
        m = re.match(r"^ros2/robot_(\d+)/gnss$", msg.topic)
        if not m:
            return
        pos = _decode(msg.payload, fmt)
        if pos:
            ts_ns = int(time.time_ns())
            recorder.record(int(m.group(1)), ts_ns, *pos)

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, f"gnss-recorder-{int(time.time())}")
    client.on_message = on_message
    client.connect(host, port, keepalive=60)
    client.subscribe("ros2/+/gnss")
    print(f"[mqtt] subscribed to ros2/+/gnss on {host}:{port}")
    client.loop_start()
    _stop.wait()
    client.loop_stop()
    client.disconnect()


# ── stats printer ─────────────────────────────────────────────────────────────

def _stats_loop(recorder: RobotRecorder, robot_ids: list[int]) -> None:
    prev = defaultdict(int)
    while not _stop.is_set():
        time.sleep(5.0)
        counts = recorder.counts()
        total = sum(counts.values())
        active = sum(1 for r in robot_ids if counts.get(r, 0) > 0)
        rates = {r: counts.get(r, 0) - prev[r] for r in robot_ids}
        prev.update(counts)
        rate_str = "  ".join(
            f"r{r}:{rates[r]//5}/s" for r in robot_ids if counts.get(r, 0) > 0
        )
        print(f"[rec] total={total:,}  robots={active}/{len(robot_ids)}  {rate_str}")


# ── argument parsing ──────────────────────────────────────────────────────────

def _parse_robots(spec: str) -> list[int]:
    ids: list[int] = []
    for part in spec.split(","):
        part = part.strip()
        if "-" in part:
            a, b = part.split("-", 1)
            ids.extend(range(int(a), int(b) + 1))
        else:
            ids.append(int(part))
    return sorted(set(ids))


def main() -> None:
    parser = argparse.ArgumentParser(description="Record robot GNSS trajectories to files")
    parser.add_argument("--broker",    choices=["kafka", "mqtt"], default="kafka")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--mqtt-host", default="localhost")
    parser.add_argument("--mqtt-port", type=int, default=1883)
    parser.add_argument("--robots",    default="1-10",
                        help="Robot IDs: '1-10', '1,3,5', or '1-5,8' (default: 1-10)")
    parser.add_argument("--out",       default="trajectories",
                        help="Output directory (default: trajectories/)")
    parser.add_argument("--format",    choices=["json", "cdr", "auto"], default="auto")
    parser.add_argument("--duration",  type=int, default=0,
                        help="Stop after N seconds (0 = run until Ctrl+C)")
    args = parser.parse_args()

    robot_ids = _parse_robots(args.robots)
    print(f"Recording {len(robot_ids)} robots → {args.out}/")
    recorder = RobotRecorder(args.out, robot_ids)

    if args.broker == "kafka":
        t = threading.Thread(
            target=_kafka_thread,
            args=(args.bootstrap, robot_ids, args.format, recorder),
            daemon=True,
        )
    else:
        t = threading.Thread(
            target=_mqtt_thread,
            args=(args.mqtt_host, args.mqtt_port, robot_ids, args.format, recorder),
            daemon=True,
        )
    t.start()

    stats = threading.Thread(target=_stats_loop, args=(recorder, robot_ids), daemon=True)
    stats.start()

    if args.duration > 0:
        print(f"Recording for {args.duration}s (Ctrl+C to stop early)...")
        _stop.wait(timeout=args.duration)
        _stop.set()
    else:
        print("Recording (Ctrl+C to stop)...")
        _stop.wait()

    recorder.close()
    counts = recorder.counts()
    total = sum(counts.values())
    print(f"\nDone. {total:,} points written across {len(robot_ids)} files.")
    for rid in robot_ids:
        print(f"  robot_{rid}: {counts.get(rid, 0):,} points")


if __name__ == "__main__":
    main()
