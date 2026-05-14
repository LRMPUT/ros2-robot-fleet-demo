#!/usr/bin/env python3
"""Live consumer — prints decoded messages from the robot fleet to the terminal.

Subscribes to all robot topics, deserializes CDR payloads, and prints a
human-readable summary line per message.

Usage:
    python consume.py --broker kafka
    python consume.py --broker mqtt
    python consume.py --broker kafka --robots 1,3,5   # specific robots only
    python consume.py --broker kafka --stats-only      # aggregate stats every second
"""
from __future__ import annotations

import argparse
import re
import signal
import sys
import threading
import time
from collections import defaultdict
from typing import Optional

from rclpy.serialization import deserialize_message
from nav_msgs.msg import Odometry
from sensor_msgs.msg import LaserScan, NavSatFix, PointCloud2

SUFFIX_TO_CLASS = {
    "gnss":   NavSatFix,
    "odom":   Odometry,
    "scan":   LaserScan,
    "points": PointCloud2,
}

_stop = threading.Event()
signal.signal(signal.SIGTERM, lambda *_: _stop.set())
signal.signal(signal.SIGINT,  lambda *_: _stop.set())

# Thread-safe counters
_lock = threading.Lock()
_counts: dict[str, int] = defaultdict(int)   # topic → msg count this second
_bytes:  dict[str, int] = defaultdict(int)
_total_msgs = 0
_total_bytes = 0


def _record(topic: str, n_bytes: int) -> None:
    global _total_msgs, _total_bytes
    with _lock:
        _counts[topic] += 1
        _bytes[topic] += n_bytes
        _total_msgs += 1
        _total_bytes += n_bytes


def _extract_t0_ns(msg) -> int:
    return msg.header.stamp.sec * 1_000_000_000 + msg.header.stamp.nanosec


def _parse_kafka_topic(topic: str) -> Optional[tuple[int, str]]:
    m = re.match(r"^ros2\.robot_(\d+)\.(gnss|odom|scan|points)$", topic)
    return (int(m.group(1)), m.group(2)) if m else None


def _parse_mqtt_topic(topic: str) -> Optional[tuple[int, str]]:
    m = re.match(r"^ros2/robot_(\d+)/(gnss|odom|scan|points)$", topic)
    return (int(m.group(1)), m.group(2)) if m else None


def _print_line(robot_id: int, topic: str, suffix: str, lat_ms: float, n_bytes: int,
                stats_only: bool) -> None:
    if stats_only:
        return
    kb = n_bytes / 1024
    print(f"[robot_{robot_id:>3}]  {suffix:<7}  {lat_ms:6.1f} ms  {kb:6.1f} KB  ← {topic}",
          flush=True)


def _stats_loop(interval: float = 1.0) -> None:
    t_prev = time.monotonic()
    while not _stop.is_set():
        time.sleep(interval)
        now = time.monotonic()
        dt = now - t_prev
        t_prev = now
        with _lock:
            snap_counts = dict(_counts)
            snap_bytes  = dict(_bytes)
            total_m = _total_msgs
            total_b = _total_bytes
            _counts.clear()
            _bytes.clear()
        total_rate = sum(snap_counts.values()) / dt
        total_kb   = sum(snap_bytes.values()) / dt / 1024
        robots_seen = {_parse_kafka_topic(t) or _parse_mqtt_topic(t)
                       for t in snap_counts} - {None}
        robot_ids = sorted({r for r, _ in robots_seen if r is not None})
        print(
            f"\r[stats]  {total_rate:6.0f} msg/s  {total_kb:7.1f} KB/s"
            f"  robots={len(robot_ids)}  total={total_m:,}",
            end="", flush=True,
        )


def consume_kafka(bootstrap: str, robot_filter: Optional[set[int]],
                  stats_only: bool) -> None:
    from confluent_kafka import Consumer

    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"fleet-consumer-{int(time.time())}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
        "topic.metadata.refresh.interval.ms": 500,
    })
    consumer.subscribe([r"^ros2\.robot_[0-9]+\.(gnss|odom|scan|points)$"])

    while not _stop.is_set():
        msg = consumer.poll(timeout=0.3)
        if msg is None or msg.error():
            continue
        parsed = _parse_kafka_topic(msg.topic())
        if parsed is None:
            continue
        robot_id, suffix = parsed
        if robot_filter and robot_id not in robot_filter:
            continue
        t1_ns = time.time_ns()
        try:
            decoded = deserialize_message(msg.value(), SUFFIX_TO_CLASS[suffix])
        except Exception:
            continue
        t0_ns = _extract_t0_ns(decoded)
        lat_ms = (t1_ns - t0_ns) / 1e6
        _record(msg.topic(), len(msg.value()))
        _print_line(robot_id, msg.topic(), suffix, lat_ms, len(msg.value()), stats_only)
    consumer.close()


def consume_mqtt(host: str, port: int, robot_filter: Optional[set[int]],
                 stats_only: bool) -> None:
    import paho.mqtt.client as mqtt

    def on_message(_client, _userdata, msg):
        parsed = _parse_mqtt_topic(msg.topic)
        if parsed is None:
            return
        robot_id, suffix = parsed
        if robot_filter and robot_id not in robot_filter:
            return
        t1_ns = time.time_ns()
        try:
            decoded = deserialize_message(msg.payload, SUFFIX_TO_CLASS[suffix])
        except Exception:
            return
        t0_ns = _extract_t0_ns(decoded)
        lat_ms = (t1_ns - t0_ns) / 1e6
        _record(msg.topic, len(msg.payload))
        _print_line(robot_id, msg.topic, suffix, lat_ms, len(msg.payload), stats_only)

    client = mqtt.Client()
    client.on_message = on_message
    client.connect(host, port, keepalive=60)
    client.subscribe("ros2/#")
    client.loop_start()
    _stop.wait()
    client.loop_stop()
    client.disconnect()


def main() -> None:
    parser = argparse.ArgumentParser(description="Live fleet consumer")
    parser.add_argument("--broker", choices=["kafka", "mqtt"], default="kafka")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--mqtt-host", default="localhost")
    parser.add_argument("--mqtt-port", type=int, default=1883)
    parser.add_argument("--robots", default=None,
                        help="Comma-separated robot IDs to show (default: all)")
    parser.add_argument("--stats-only", action="store_true",
                        help="Print only aggregate stats line, not per-message")
    args = parser.parse_args()

    robot_filter: Optional[set[int]] = None
    if args.robots:
        robot_filter = {int(r.strip()) for r in args.robots.split(",")}

    stats_thread = threading.Thread(target=_stats_loop, daemon=True)
    stats_thread.start()

    print(f"Listening on {args.broker.upper()}... (Ctrl+C to stop)\n", flush=True)
    try:
        if args.broker == "kafka":
            consume_kafka(args.bootstrap, robot_filter, args.stats_only)
        else:
            consume_mqtt(args.mqtt_host, args.mqtt_port, robot_filter, args.stats_only)
    except KeyboardInterrupt:
        _stop.set()

    print(f"\n\nTotal: {_total_msgs:,} messages, {_total_bytes/1024/1024:.1f} MB")
    sys.exit(0)


if __name__ == "__main__":
    main()
