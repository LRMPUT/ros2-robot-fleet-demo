#!/usr/bin/env python3
"""Replay NavSatFix from an MCAP bag to MQTT at real-time speed.

Publishes JSON payloads to ros2/robot_1/gnss, matching the fleet topic schema
so consume.py --broker mqtt echoes them without modification.

Usage:
    python3 tools/publish_mcap_to_mqtt.py --bag bags/rosbag2_2026_04_10-11_01_18/
    python3 tools/publish_mcap_to_mqtt.py --bag bags/... --loop --speed 2.0
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import sys
import time

import paho.mqtt.client as mqtt
from mcap.reader import make_reader
from mcap_ros2.decoder import DecoderFactory

MQTT_TOPIC = "ros2/robot_1/gnss"


def find_mcap(bag_path: str) -> str:
    if bag_path.endswith(".mcap"):
        return bag_path
    matches = glob.glob(os.path.join(bag_path, "*.mcap"))
    if not matches:
        sys.exit(f"ERROR: no .mcap file in {bag_path}")
    return matches[0]


def load_pts(mcap_path: str, topic: str) -> list:
    pts = []
    with open(mcap_path, "rb") as f:
        reader = make_reader(f, decoder_factories=[DecoderFactory()])
        for _, _, _, msg in reader.iter_decoded_messages(topics=[topic]):
            pts.append(msg)
    return pts


def replay(pts: list, client: mqtt.Client, speed: float) -> None:
    bag_t0 = pts[0].header.stamp.sec + pts[0].header.stamp.nanosec * 1e-9
    wall_t0 = time.monotonic()

    for i, msg in enumerate(pts):
        bag_t = msg.header.stamp.sec + msg.header.stamp.nanosec * 1e-9
        target = wall_t0 + (bag_t - bag_t0) / speed
        wait = target - time.monotonic()
        if wait > 0:
            time.sleep(wait)

        now_ns = time.time_ns()
        payload = json.dumps({
            "header": {
                "stamp": {
                    "sec":     now_ns // 1_000_000_000,
                    "nanosec": now_ns  % 1_000_000_000,
                },
                "frame_id": "gnss",
            },
            "status":    {"status": msg.status.status, "service": msg.status.service},
            "latitude":  msg.latitude,
            "longitude": msg.longitude,
            "altitude":  msg.altitude,
        })
        client.publish(MQTT_TOPIC, payload)

        if (i + 1) % 100 == 0 or i == len(pts) - 1:
            print(f"  {i+1}/{len(pts)}  lat={msg.latitude:.6f}  lon={msg.longitude:.6f}",
                  flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay MCAP NavSatFix → MQTT")
    parser.add_argument("--bag",      required=True)
    parser.add_argument("--topic",    default="/sensing/ins/imu/nav_sat_fix")
    parser.add_argument("--host",     default="localhost")
    parser.add_argument("--port",     type=int, default=1883)
    parser.add_argument("--speed",    type=float, default=1.0,
                        help="Replay speed multiplier (default: 1.0)")
    parser.add_argument("--loop",     action="store_true",
                        help="Loop bag indefinitely")
    args = parser.parse_args()

    mcap_path = find_mcap(args.bag)
    print(f"MCAP:  {mcap_path}")
    print(f"Topic: {args.topic}  →  MQTT {args.host}:{args.port}/{MQTT_TOPIC}")
    print(f"Speed: {args.speed}x")

    pts = load_pts(mcap_path, args.topic)
    if not pts:
        sys.exit("ERROR: no NavSatFix messages found.")
    dur = (pts[-1].header.stamp.sec - pts[0].header.stamp.sec)
    print(f"Loaded {len(pts)} points  ({dur} s bag duration)\n")

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
    client.connect(args.host, args.port)
    client.loop_start()

    run = 0
    try:
        while True:
            run += 1
            print(f"--- run {run} ---")
            replay(pts, client, args.speed)
            if not args.loop:
                break
            print("Looping...")
    except KeyboardInterrupt:
        pass
    finally:
        client.loop_stop()
        client.disconnect()
        print("\nPublisher stopped.")


if __name__ == "__main__":
    main()
