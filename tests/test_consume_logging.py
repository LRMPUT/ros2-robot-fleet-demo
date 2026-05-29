"""Tests for JSONL latency logging in consume.py."""
import io
import json
import sys
from unittest.mock import MagicMock

# Stub broker libraries so consume.py imports without them installed.
sys.modules.setdefault("confluent_kafka", MagicMock())
sys.modules.setdefault("paho", MagicMock())
sys.modules.setdefault("paho.mqtt", MagicMock())
sys.modules.setdefault("paho.mqtt.client", MagicMock())

import consumer.consume as consume


def _reset_log():
    consume._log_fh = None


def test_log_latency_noop_when_disabled():
    _reset_log()
    # Must not raise when logging is off.
    consume._log_latency(1, "gnss", "ros2.robot_1.gnss", 100, 150, 200, 100, 42)


def test_log_latency_writes_one_jsonl_record():
    _reset_log()
    buf = io.StringIO()
    consume._log_fh = buf
    try:
        consume._log_latency(3, "gnss", "ros2.robot_3.gnss",
                             1000, 1200, 1500, 500, 142)
    finally:
        consume._log_fh = None

    lines = buf.getvalue().strip().splitlines()
    assert len(lines) == 1
    rec = json.loads(lines[0])
    assert rec == {
        "robot_id": 3,
        "suffix": "gnss",
        "topic": "ros2.robot_3.gnss",
        "t0_ns": 1000,
        "t1_ns": 1200,
        "t2_ns": 1500,
        "latency_ns": 500,
        "payload_bytes": 142,
    }


def test_log_latency_null_sink_stamp():
    _reset_log()
    buf = io.StringIO()
    consume._log_fh = buf
    try:
        # MQTT (Phase 1) has no sink stamp: t1_ns is None -> JSON null.
        consume._log_latency(2, "odom", "ros2/robot_2/odom",
                             1000, None, 1500, 500, 80)
    finally:
        consume._log_fh = None

    rec = json.loads(buf.getvalue().strip())
    assert rec["t1_ns"] is None
    assert rec["t0_ns"] == 1000
    assert rec["t2_ns"] == 1500
    assert rec["latency_ns"] == 500
