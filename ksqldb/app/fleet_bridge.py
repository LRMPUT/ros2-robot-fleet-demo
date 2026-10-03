#!/usr/bin/env python3
"""Flatten per-robot Kafka JSON topics into fleet-wide GIS streams.

Subscribes to ``ros2.robot_<id>.{gnss,odom}`` (JSON payloads from the fleet
with ``PAYLOAD_FORMAT=json``) and republishes:

* ``ros2.fleet.gnss`` → fields expected by ``ROS_GPS_FIX_STREAM``
* ``ros2.fleet.odom`` → fields expected by ``ROS_FILTERED_ODOM_STREAM``
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
import time

logging.basicConfig(
    level=logging.INFO,
    format="[fleet-bridge] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("fleet_bridge")

_TOPIC_RE = re.compile(r"^ros2\.robot_(\d+)\.(gnss|odom)$")


def _t_ms_from_payload(data: dict) -> int:
    ts = data.get("_ts")
    if isinstance(ts, dict) and ts.get("t0_ns"):
        return int(ts["t0_ns"]) // 1_000_000
    stamp = (data.get("header") or {}).get("stamp") or {}
    sec = int(stamp.get("sec", 0))
    nanosec = int(stamp.get("nanosec", 0))
    if sec or nanosec:
        return sec * 1000 + nanosec // 1_000_000
    return int(time.time() * 1000)


def _flatten_gnss(robot_id: str, data: dict) -> dict | None:
    try:
        return {
            "robot_id": robot_id,
            "timestamp": _t_ms_from_payload(data),
            "latitude": float(data["latitude"]),
            "longitude": float(data["longitude"]),
        }
    except (KeyError, TypeError, ValueError):
        return None


def _flatten_odom(robot_id: str, data: dict) -> dict | None:
    try:
        pos = data["pose"]["pose"]["position"]
        return {
            "robot_id": robot_id,
            "timestamp": _t_ms_from_payload(data),
            "position_x": float(pos["x"]),
            "position_y": float(pos["y"]),
            "position_z": float(pos.get("z", 0.0)),
        }
    except (KeyError, TypeError, ValueError):
        return None


async def run() -> None:
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

    bootstrap = os.environ.get(
        "KAFKA_BOOTSTRAP_SERVERS",
        os.environ.get("KAFKA_BROKER", "broker:29092"),
    )

    consumer = AIOKafkaConsumer(
        bootstrap_servers=bootstrap,
        group_id="fleet-gis-bridge",
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )
    producer = AIOKafkaProducer(bootstrap_servers=bootstrap)
    await consumer.start()
    await producer.start()
    # Pattern subscribe: match topics as they appear.
    consumer.subscribe(pattern=r"^ros2\.robot_[0-9]+\.(gnss|odom)$")
    log.info("listening on %s → ros2.fleet.{gnss,odom}", bootstrap)

    try:
        async for msg in consumer:
            parsed = _TOPIC_RE.match(msg.topic)
            if not parsed:
                continue
            robot_id = f"robot_{parsed.group(1)}"
            suffix = parsed.group(2)
            try:
                data = json.loads(msg.value)
            except Exception:
                continue
            if suffix == "gnss":
                out = _flatten_gnss(robot_id, data)
                dest = "ros2.fleet.gnss"
            else:
                out = _flatten_odom(robot_id, data)
                dest = "ros2.fleet.odom"
            if out is None:
                continue
            await producer.send_and_wait(
                dest,
                key=robot_id.encode(),
                value=json.dumps(out, separators=(",", ":")).encode(),
            )
    finally:
        await consumer.stop()
        await producer.stop()
        log.info("stopped")


def main() -> None:
    asyncio.run(run())


if __name__ == "__main__":
    main()
