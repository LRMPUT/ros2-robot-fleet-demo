#!/usr/bin/env python3
"""Live consumer — prints decoded messages from the robot fleet to the terminal.

Subscribes to all robot topics, decodes payloads, and prints a human-readable
summary line per message. Supports both JSON (default) and CDR payload formats.

Usage:
    python consume.py --broker kafka
    python consume.py --broker mqtt
    python consume.py --broker kafka --robots 1,3,5   # specific robots only
    python consume.py --broker kafka --stats-only      # aggregate stats every second
    python consume.py --broker kafka --format cdr      # CDR binary payloads
"""
from __future__ import annotations

import argparse
import json as _json
import re
import signal
import sys
import threading
import time
from collections import defaultdict
from typing import Optional

_stop = threading.Event()
signal.signal(signal.SIGTERM, lambda *_: _stop.set())
signal.signal(signal.SIGINT,  lambda *_: _stop.set())

# Thread-safe counters
_lock = threading.Lock()
_counts: dict[str, int] = defaultdict(int)
_bytes:  dict[str, int] = defaultdict(int)
_total_msgs = 0
_total_bytes = 0
_last_seen: dict[int, float] = {}
_warned_silent: set[int] = set()
_start_time: float = time.monotonic()
_warned_no_data: bool = False  # only written from stats thread; no lock needed

# JSONL latency log. _log_fh is None unless --log-file is given.
_log_fh = None
_log_lock = threading.Lock()


def _log_latency(robot_id: int, suffix: str, topic: str,
                 t0_ns: int, t1_ns: int, latency_ns: int,
                 payload_bytes: int) -> None:
    """Append one JSONL latency record. No-op when logging is disabled."""
    if _log_fh is None:
        return
    rec = _json.dumps({
        "robot_id": robot_id,
        "suffix": suffix,
        "topic": topic,
        "t0_ns": t0_ns,
        "t1_ns": t1_ns,
        "latency_ns": latency_ns,
        "payload_bytes": payload_bytes,
    }, separators=(",", ":"))
    with _log_lock:
        _log_fh.write(rec + "\n")


SUFFIX_TO_TYPE = {
    "gnss":   "sensor_msgs/msg/NavSatFix",
    "odom":   "nav_msgs/msg/Odometry",
    "scan":   "sensor_msgs/msg/LaserScan",
    "points": "sensor_msgs/msg/PointCloud2",
}


def _record(topic: str, n_bytes: int, robot_id: Optional[int] = None) -> None:
    global _total_msgs, _total_bytes
    with _lock:
        _counts[topic] += 1
        _bytes[topic] += n_bytes
        _total_msgs += 1
        _total_bytes += n_bytes
        if robot_id is not None:
            _last_seen[robot_id] = time.monotonic()


def _check_health(silence_threshold: float = 10.0,
                  total_m: Optional[int] = None) -> None:
    global _warned_no_data
    now = time.monotonic()

    with _lock:
        if total_m is None:
            total_m = _total_msgs
        snap_last_seen = dict(_last_seen)

    if not _warned_no_data and total_m == 0 and (now - _start_time) > 15.0:
        print("\n[WARNING] No messages received — is the fleet running and broker reachable?",
              flush=True)
        _warned_no_data = True

    for robot_id, last_t in snap_last_seen.items():
        elapsed = now - last_t
        if elapsed > silence_threshold:
            if robot_id not in _warned_silent:
                print(f"\n[WARNING] robot_{robot_id} silent for {elapsed:.0f}s", flush=True)
                _warned_silent.add(robot_id)
        else:
            _warned_silent.discard(robot_id)


def _t0_from_json(payload: bytes) -> Optional[int]:
    try:
        data = _json.loads(payload)
        stamp = data.get("header", {}).get("stamp", {})
        sec = int(stamp.get("sec", 0))
        nanosec = int(stamp.get("nanosec", 0))
        if sec == 0 and nanosec == 0:
            return None
        return sec * 1_000_000_000 + nanosec
    except Exception:
        return None


def _t0_from_cdr(payload: bytes, suffix: str) -> Optional[int]:
    try:
        from rclpy.serialization import deserialize_message
        from nav_msgs.msg import Odometry
        from sensor_msgs.msg import LaserScan, NavSatFix, PointCloud2
        cls = {"gnss": NavSatFix, "odom": Odometry,
               "scan": LaserScan, "points": PointCloud2}[suffix]
        msg = deserialize_message(payload, cls)
        return msg.header.stamp.sec * 1_000_000_000 + msg.header.stamp.nanosec
    except Exception:
        return None


def _payload_format_from_headers(headers) -> Optional[str]:
    """Extract payload_format from Kafka message headers."""
    if not headers:
        return None
    for k, v in headers:
        key = k.decode() if isinstance(k, bytes) else k
        if key == "payload_format":
            return v.decode() if isinstance(v, bytes) else v
    return None


def _decode(payload: bytes, suffix: str, fmt: str,
            kafka_headers=None) -> Optional[int]:
    """Return t0_ns or None on failure. fmt: 'json', 'cdr', or 'auto'."""
    if fmt == "auto":
        detected = _payload_format_from_headers(kafka_headers)
        if detected:
            fmt = detected
        else:
            # Try JSON; fall back to CDR
            t0 = _t0_from_json(payload)
            if t0 is not None:
                return t0
            return _t0_from_cdr(payload, suffix)
    if fmt == "json":
        return _t0_from_json(payload)
    return _t0_from_cdr(payload, suffix)


def _parse_kafka_topic(topic: str) -> Optional[tuple[int, str]]:
    m = re.match(r"^ros2\.robot_(\d+)\.(gnss|odom|scan|points)$", topic)
    return (int(m.group(1)), m.group(2)) if m else None


def _parse_mqtt_topic(topic: str) -> Optional[tuple[int, str]]:
    m = re.match(r"^ros2/robot_(\d+)/(gnss|odom|scan|points)$", topic)
    return (int(m.group(1)), m.group(2)) if m else None


def _print_line(robot_id: int, topic: str, suffix: str, lat_ms: float,
                n_bytes: int, stats_only: bool) -> None:
    if stats_only:
        return
    kb = n_bytes / 1024
    print(f"[robot_{robot_id:>3}]  {suffix:<7}  {lat_ms:6.1f} ms  {kb:6.1f} KB  ← {topic}",
          flush=True)


def _stats_loop(interval: float = 1.0, stats_only: bool = False,
                silence_threshold: float = 10.0) -> None:
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
            _counts.clear()
            _bytes.clear()
        _check_health(silence_threshold, total_m=total_m)
        total_rate = sum(snap_counts.values()) / dt
        total_kb   = sum(snap_bytes.values()) / dt / 1024
        robots_seen = {_parse_kafka_topic(t) or _parse_mqtt_topic(t)
                       for t in snap_counts} - {None}
        robot_ids = sorted({r for r, _ in robots_seen if r is not None})
        line = (f"[stats]  {total_rate:6.0f} msg/s  {total_kb:7.1f} KB/s"
                f"  robots={len(robot_ids)}  total={total_m:,}")
        if stats_only:
            print(f"\r{line}", end="", flush=True)
        else:
            print(line, flush=True)


def consume_kafka(bootstrap: str, robot_filter: Optional[set[int]],
                  stats_only: bool, fmt: str) -> None:
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
        if msg is None:
            continue
        if msg.error():
            from confluent_kafka import KafkaError
            # UNKNOWN_TOPIC_OR_PART is expected noise for regex subscriptions
            # when no matching topics exist yet; suppress it.
            if msg.error().code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                print(f"[kafka error] {msg.error()}", file=sys.stderr, flush=True)
            continue
        parsed = _parse_kafka_topic(msg.topic())
        if parsed is None:
            continue
        robot_id, suffix = parsed
        if robot_filter and robot_id not in robot_filter:
            continue
        t1_ns = time.time_ns()
        t0_ns = _decode(msg.value(), suffix, fmt, msg.headers())
        if t0_ns is None:
            continue
        lat_ms = (t1_ns - t0_ns) / 1e6
        _record(msg.topic(), len(msg.value()), robot_id=robot_id)
        _log_latency(robot_id, suffix, msg.topic(),
                     t0_ns, t1_ns, t1_ns - t0_ns, len(msg.value()))
        _print_line(robot_id, msg.topic(), suffix, lat_ms, len(msg.value()), stats_only)
    consumer.close()


def consume_mqtt(host: str, port: int, robot_filter: Optional[set[int]],
                 stats_only: bool, fmt: str) -> None:
    import paho.mqtt.client as mqtt

    def on_message(_client, _userdata, msg):
        parsed = _parse_mqtt_topic(msg.topic)
        if parsed is None:
            return
        robot_id, suffix = parsed
        if robot_filter and robot_id not in robot_filter:
            return
        t1_ns = time.time_ns()
        t0_ns = _decode(msg.payload, suffix, fmt)
        if t0_ns is None:
            return
        lat_ms = (t1_ns - t0_ns) / 1e6
        _record(msg.topic, len(msg.payload), robot_id=robot_id)
        _log_latency(robot_id, suffix, msg.topic,
                     t0_ns, t1_ns, t1_ns - t0_ns, len(msg.payload))
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
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--mqtt-host", default="localhost")
    parser.add_argument("--mqtt-port", type=int, default=1883)
    parser.add_argument("--robots", default=None,
                        help="Comma-separated robot IDs to show (default: all)")
    parser.add_argument("--stats-only", action="store_true")
    parser.add_argument("--format", choices=["json", "cdr", "auto"], default="auto",
                        help="Payload format: json, cdr, or auto-detect (default: auto)")
    parser.add_argument(
        "--silence-threshold", type=float, default=10.0, metavar="SECONDS",
        help="seconds of robot silence before a warning is printed (default: 10)",
    )
    parser.add_argument("--log-file", default=None,
                        help="Append per-message latency JSONL to this path")
    args = parser.parse_args()

    global _log_fh
    if args.log_file:
        _log_fh = open(args.log_file, "a", buffering=1)

    robot_filter: Optional[set[int]] = None
    if args.robots:
        robot_filter = {int(r.strip()) for r in args.robots.split(",")}

    stats_thread = threading.Thread(
        target=_stats_loop,
        kwargs={"stats_only": args.stats_only, "silence_threshold": args.silence_threshold},
        daemon=True,
    )
    stats_thread.start()

    print(f"Listening on {args.broker.upper()}  format={args.format} ... (Ctrl+C to stop)\n",
          flush=True)
    try:
        if args.broker == "kafka":
            consume_kafka(args.bootstrap, robot_filter, args.stats_only, args.format)
        else:
            consume_mqtt(args.mqtt_host, args.mqtt_port, robot_filter, args.stats_only,
                         args.format)
    except KeyboardInterrupt:
        _stop.set()

    print(f"\n\nTotal: {_total_msgs:,} messages, {_total_bytes/1024/1024:.1f} MB")
    sys.exit(0)


if __name__ == "__main__":
    main()
