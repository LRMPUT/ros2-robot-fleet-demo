"""Tests for the latency analyzer's join + stage/percentile math."""
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


def test_analyze_computes_stages_drop_rate_and_percentiles(tmp_path):
    pub_dir = tmp_path / "publisher"
    pub_dir.mkdir()
    # robot 1 gnss published 3 messages (t0 = 100, 200, 300)
    _write_jsonl(pub_dir / "publisher_robot_1.jsonl", [
        {"robot_id": 1, "suffix": "gnss", "topic": "/robot_1/gnss", "t0_ns": 100},
        {"robot_id": 1, "suffix": "gnss", "topic": "/robot_1/gnss", "t0_ns": 200},
        {"robot_id": 1, "suffix": "gnss", "topic": "/robot_1/gnss", "t0_ns": 300},
    ])
    # Consumer received 2 distinct (t0 = 100, 300); 200 dropped; 100 delivered
    # twice (duplicate must not inflate matched). Each record carries the sink
    # stamp t1_ns so ingest = t1-t0 and transport = t2-t1 are computable.
    #   rec1: t0=100  t1=500100   t2=1000100  -> ingest 0.5ms transport 0.5ms e2e 1.0ms
    #   rec2: t0=100  t1=600100   t2=1100100  -> ingest 0.6ms transport 0.5ms e2e 1.1ms
    #   rec3: t0=300  t1=1350300  t2=2700300  -> ingest 1.35ms transport 1.35ms e2e 2.7ms
    _write_jsonl(tmp_path / "consumer.jsonl", [
        {"robot_id": 1, "suffix": "gnss", "topic": "ros2.robot_1.gnss",
         "t0_ns": 100, "t1_ns": 500100, "t2_ns": 1000100,
         "latency_ns": 1000000, "payload_bytes": 10},
        {"robot_id": 1, "suffix": "gnss", "topic": "ros2.robot_1.gnss",
         "t0_ns": 100, "t1_ns": 600100, "t2_ns": 1100100,
         "latency_ns": 1100000, "payload_bytes": 10},
        {"robot_id": 1, "suffix": "gnss", "topic": "ros2.robot_1.gnss",
         "t0_ns": 300, "t1_ns": 1350300, "t2_ns": 2700300,
         "latency_ns": 2700000, "payload_bytes": 10},
    ])

    report = al.analyze(str(tmp_path))
    gnss = report["by_suffix"]["gnss"]
    # Drop accounting (join on (robot_id, t0_ns) set, dedup duplicate t0=100).
    assert gnss["published"] == 3
    assert gnss["matched"] == 2
    assert gnss["drop_rate"] == 1 / 3
    # End-to-end percentiles from latency_ns -> [1.0, 1.1, 2.7] ms.
    assert gnss["count"] == 3
    assert gnss["p50_ms"] == 1.1
    # Stage breakdown.
    assert gnss["staged_count"] == 3
    assert gnss["ingest_p50_ms"] == 0.6       # median of [0.5, 0.6, 1.35]
    assert gnss["transport_p50_ms"] == 0.5    # median of [0.5, 0.5, 1.35]


def test_analyze_null_sink_reports_stages_na(tmp_path):
    # MQTT Phase 1: t1_ns is null -> stages n/a but e2e still computed.
    _write_jsonl(tmp_path / "consumer.jsonl", [
        {"robot_id": 2, "suffix": "odom", "topic": "ros2/robot_2/odom",
         "t0_ns": 5, "t1_ns": None, "t2_ns": 1000005,
         "latency_ns": 1000000, "payload_bytes": 8},
    ])
    report = al.analyze(str(tmp_path))
    odom = report["by_suffix"]["odom"]
    assert odom["count"] == 1
    assert odom["staged_count"] == 0
    assert odom["ingest_p50_ms"] is None
    assert odom["transport_p50_ms"] is None
    # No publisher dir -> drop accounting degrades.
    assert odom["published"] is None
    assert odom["drop_rate"] is None


def test_analyze_mixed_broker_stages_count_only_kafka(tmp_path):
    # Same suffix, two records: one Kafka (integer sink t1) and one MQTT
    # (null t1). staged_count and the stage percentiles must reflect ONLY the
    # Kafka record; e2e count includes both.
    _write_jsonl(tmp_path / "consumer.jsonl", [
        {"robot_id": 1, "suffix": "gnss", "topic": "ros2.robot_1.gnss",
         "t0_ns": 100, "t1_ns": 600100, "t2_ns": 1100100,
         "latency_ns": 1100000, "payload_bytes": 10},
        {"robot_id": 2, "suffix": "gnss", "topic": "ros2/robot_2/gnss",
         "t0_ns": 200, "t1_ns": None, "t2_ns": 1200200,
         "latency_ns": 1000000, "payload_bytes": 10},
    ])
    report = al.analyze(str(tmp_path))
    gnss = report["by_suffix"]["gnss"]
    assert gnss["count"] == 2                 # both records counted for e2e
    assert gnss["staged_count"] == 1          # only the Kafka record has a sink stamp
    assert gnss["ingest_p50_ms"] == 0.6       # (600100 - 100) / 1e6
    assert gnss["transport_p50_ms"] == 0.5    # (1100100 - 600100) / 1e6
