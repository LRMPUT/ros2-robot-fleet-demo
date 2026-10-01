#!/usr/bin/env python3
"""Echo GNSS (NavSatFix) lat/lon from Kafka or MQTT for selected robots.

Works with both payload formats:
  json — parsed directly, no ROS needed
  cdr  — deserialized via rclpy (requires ROS 2 environment)

Usage:
  # Kafka, robots 1 and 2 (JSON default)
  python examples/echo_gnss.py --broker kafka --robots 1,2

  # MQTT
  python examples/echo_gnss.py --broker mqtt --robots 1,2

  # CDR payloads
  python examples/echo_gnss.py --broker kafka --robots 1,2 --format cdr

  # Docker (no local ROS needed for JSON)
  docker run --rm --network host ros2-fleet-consumer \
      python3 /app/echo_gnss.py --broker kafka --robots 1,2
"""
from __future__ import annotations

import argparse
import json
import signal
import threading
import time

_stop = threading.Event()
signal.signal(signal.SIGINT,  lambda *_: _stop.set())
signal.signal(signal.SIGTERM, lambda *_: _stop.set())


def _decode_json(payload: bytes) -> tuple[float, float] | None:
    try:
        msg = json.loads(payload)
        return float(msg["latitude"]), float(msg["longitude"])
    except Exception:
        return None


def _decode_cdr(payload: bytes) -> tuple[float, float] | None:
    try:
        from rclpy.serialization import deserialize_message
        from sensor_msgs.msg import NavSatFix
        msg = deserialize_message(payload, NavSatFix)
        return msg.latitude, msg.longitude
    except Exception:
        return None


def _decode_auto(payload: bytes) -> tuple[float, float] | None:
    result = _decode_json(payload)
    return result if result is not None else _decode_cdr(payload)


def _print(robot_id: int, topic: str, lat: float, lon: float) -> None:
    ts = time.strftime("%H:%M:%S")
    print(f"{ts}  robot_{robot_id:<3}  lat={lat:>12.7f}  lon={lon:>12.7f}  ({topic})", flush=True)


def echo_kafka(bootstrap: str, robot_ids: list[int], decode) -> None:
    from confluent_kafka import Consumer
    topics = [f"ros2.robot_{r}.gnss" for r in robot_ids]
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": f"echo-gnss-{int(time.time())}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(topics)
    print(f"Subscribed to: {', '.join(topics)}\n")
    while not _stop.is_set():
        msg = consumer.poll(timeout=0.3)
        if msg is None:
            continue
        if msg.error():
            from confluent_kafka import KafkaError
            if msg.error().code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                print(f"[kafka] {msg.error()}", flush=True)
            continue
        # For auto mode, check payload_format header first
        dec = decode
        if decode is _decode_auto and msg.headers():
            for k, v in (msg.headers() or []):
                if (k.decode() if isinstance(k, bytes) else k) == "payload_format":
                    fmt = v.decode() if isinstance(v, bytes) else v
                    dec = _decode_json if fmt == "json" else _decode_cdr
                    break
        result = dec(msg.value())
        if result:
            rid = int(msg.topic().split("_")[1].split(".")[0])
            _print(rid, msg.topic(), *result)
    consumer.close()


def echo_mqtt(host: str, port: int, robot_ids: list[int], decode) -> None:
    import paho.mqtt.client as mqtt
    topics = [f"ros2/robot_{r}/gnss" for r in robot_ids]

    def on_message(_client, _userdata, msg):
        result = decode(msg.payload)
        if result:
            rid = int(msg.topic.split("/")[1].split("_")[1])
            _print(rid, msg.topic, *result)

    client = mqtt.Client()
    client.on_message = on_message
    client.connect(host, port, keepalive=60)
    for t in topics:
        client.subscribe(t)
    print(f"Subscribed to: {', '.join(topics)}\n")
    client.loop_start()
    _stop.wait()
    client.loop_stop()
    client.disconnect()


def main() -> None:
    parser = argparse.ArgumentParser(description="Echo GNSS lat/lon from fleet topics")
    parser.add_argument("--broker",    choices=["kafka", "mqtt"], default="kafka")
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--mqtt-host", default="localhost")
    parser.add_argument("--mqtt-port", type=int, default=1883)
    parser.add_argument("--robots",    default="1,2",
                        help="Comma-separated robot IDs (default: 1,2)")
    parser.add_argument("--format",    choices=["json", "cdr", "auto"], default="auto",
                        help="Payload format (default: auto)")
    args = parser.parse_args()

    robot_ids = [int(r.strip()) for r in args.robots.split(",")]
    decode = {"json": _decode_json, "cdr": _decode_cdr, "auto": _decode_auto}[args.format]

    print(f"broker={args.broker}  robots={robot_ids}  format={args.format}")
    print("-" * 60)

    if args.broker == "kafka":
        echo_kafka(args.bootstrap, robot_ids, decode)
    else:
        echo_mqtt(args.mqtt_host, args.mqtt_port, robot_ids, decode)


if __name__ == "__main__":
    main()
