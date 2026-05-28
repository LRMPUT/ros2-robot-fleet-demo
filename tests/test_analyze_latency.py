"""Tests for the latency analyzer's join + stats math."""
import json
import pathlib
import sys

REPO = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "tools"))

import analyze_latency as al  # noqa: E402


def _write_jsonl(path, records):
    with open(path, "w") as fh:
        for r in records:
            fh.write(json.dumps(r) + "\n")


def test_analyze_computes_drop_rate_and_percentiles(tmp_path):
    pub_dir = tmp_path / "publisher"
    pub_dir.mkdir()
    # robot 1 gnss published 3 messages (t0 = 100, 200, 300)
    _write_jsonl(pub_dir / "publisher_robot_1.jsonl", [
        {"robot_id": 1, "suffix": "gnss", "topic": "/robot_1/gnss", "t0_ns": 100},
        {"robot_id": 1, "suffix": "gnss", "topic": "/robot_1/gnss", "t0_ns": 200},
        {"robot_id": 1, "suffix": "gnss", "topic": "/robot_1/gnss", "t0_ns": 300},
    ])
    # consumer received 2 of them (t0 = 100, 300); 200 was dropped.
    # A duplicate delivery of 100 must NOT inflate the matched count.
    _write_jsonl(tmp_path / "consumer.jsonl", [
        {"robot_id": 1, "suffix": "gnss", "topic": "ros2.robot_1.gnss",
         "t0_ns": 100, "t1_ns": 1100, "latency_ns": 1000000, "payload_bytes": 10},
        {"robot_id": 1, "suffix": "gnss", "topic": "ros2.robot_1.gnss",
         "t0_ns": 100, "t1_ns": 1200, "latency_ns": 1100000, "payload_bytes": 10},
        {"robot_id": 1, "suffix": "gnss", "topic": "ros2.robot_1.gnss",
         "t0_ns": 300, "t1_ns": 3000, "latency_ns": 2700000, "payload_bytes": 10},
    ])

    report = al.analyze(str(tmp_path))
    gnss = report["by_suffix"]["gnss"]
    assert gnss["published"] == 3
    assert gnss["matched"] == 2          # dedup by t0: {100, 300}
    assert gnss["drop_rate"] == 1 / 3    # 1 of 3 dropped
    # Latency percentiles use ALL consumer records (ns → ms).
    assert gnss["count"] == 3
    assert gnss["p50_ms"] == 1.1         # median of [1.0, 1.1, 2.7] ms


def test_analyze_without_publisher_dir_degrades(tmp_path):
    _write_jsonl(tmp_path / "consumer.jsonl", [
        {"robot_id": 2, "suffix": "odom", "topic": "ros2.robot_2.odom",
         "t0_ns": 5, "t1_ns": 1005, "latency_ns": 1000, "payload_bytes": 8},
    ])
    report = al.analyze(str(tmp_path))
    odom = report["by_suffix"]["odom"]
    assert odom["count"] == 1
    assert odom["published"] is None      # no publisher data
    assert odom["drop_rate"] is None
