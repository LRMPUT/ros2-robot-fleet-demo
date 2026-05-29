"""Tests for the latency analyzer's join + stage/percentile math."""
import csv
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


def test_analyze_mqtt_with_ts_envelope_reports_stages(tmp_path):
    # With the `_ts` envelope the sink now stamps t1 for MQTT too, so the
    # analyzer must report ingest/transport for MQTT topics (broker-agnostic).
    _write_jsonl(tmp_path / "consumer.jsonl", [
        {"robot_id": 1, "suffix": "gnss", "topic": "ros2/robot_1/gnss",
         "t0_ns": 100, "t1_ns": 600100, "t2_ns": 1100100,
         "latency_ns": 1_100_000, "payload_bytes": 10},
    ])
    report = al.analyze(str(tmp_path))
    gnss = report["by_suffix"]["gnss"]
    assert gnss["staged_count"] == 1
    assert gnss["ingest_p50_ms"] == 0.6        # (600100 - 100) / 1e6
    assert gnss["transport_p50_ms"] == 0.5     # (1100100 - 600100) / 1e6


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


# ── run-level aggregates + CSV export ────────────────────────────────────────

def test_analyze_run_level_aggregates_kafka(tmp_path):
    pub_dir = tmp_path / "publisher"
    pub_dir.mkdir()
    # 2 robots, gnss only, 3 published each (6 expected total).
    _write_jsonl(pub_dir / "publisher_robot_1.jsonl", [
        {"robot_id": 1, "suffix": "gnss", "topic": "/robot_1/gnss", "t0_ns": 100},
        {"robot_id": 1, "suffix": "gnss", "topic": "/robot_1/gnss", "t0_ns": 200},
        {"robot_id": 1, "suffix": "gnss", "topic": "/robot_1/gnss", "t0_ns": 300},
    ])
    _write_jsonl(pub_dir / "publisher_robot_2.jsonl", [
        {"robot_id": 2, "suffix": "gnss", "topic": "/robot_2/gnss", "t0_ns": 400},
        {"robot_id": 2, "suffix": "gnss", "topic": "/robot_2/gnss", "t0_ns": 500},
        {"robot_id": 2, "suffix": "gnss", "topic": "/robot_2/gnss", "t0_ns": 600},
    ])
    # Received 4 records (latency 1,2,3,4 ms) — all distinct, ≤ expected.
    _write_jsonl(tmp_path / "consumer.jsonl", [
        {"robot_id": 1, "suffix": "gnss", "topic": "ros2.robot_1.gnss",
         "t0_ns": 100, "t1_ns": None, "t2_ns": 1000100,
         "latency_ns": 1_000_000, "payload_bytes": 10},
        {"robot_id": 1, "suffix": "gnss", "topic": "ros2.robot_1.gnss",
         "t0_ns": 200, "t1_ns": None, "t2_ns": 2000200,
         "latency_ns": 2_000_000, "payload_bytes": 10},
        {"robot_id": 2, "suffix": "gnss", "topic": "ros2.robot_2.gnss",
         "t0_ns": 400, "t1_ns": None, "t2_ns": 3000400,
         "latency_ns": 3_000_000, "payload_bytes": 10},
        {"robot_id": 2, "suffix": "gnss", "topic": "ros2.robot_2.gnss",
         "t0_ns": 500, "t1_ns": None, "t2_ns": 4000500,
         "latency_ns": 4_000_000, "payload_bytes": 10},
    ])

    report = al.analyze(str(tmp_path))
    assert report["broker"] == "Kafka"
    assert report["n_robots"] == 2            # two publisher files
    assert report["expected"] == 6
    assert report["received"] == 4
    assert report["delivery_pct"] == round(4 / 6 * 100, 1)   # 66.7
    # pooled latency [1,2,3,4] ms -> avg 2.5, p50 2.5
    assert report["avg_ms"] == 2.5
    assert report["p50_ms"] == 2.5


def test_analyze_broker_mqtt_and_delivery_clamped(tmp_path):
    pub_dir = tmp_path / "publisher"
    pub_dir.mkdir()
    _write_jsonl(pub_dir / "publisher_robot_1.jsonl", [
        {"robot_id": 1, "suffix": "odom", "topic": "/robot_1/odom", "t0_ns": 10},
        {"robot_id": 1, "suffix": "odom", "topic": "/robot_1/odom", "t0_ns": 20},
    ])
    # 3 received (one duplicate t0=10) > 2 expected -> delivery clamped to 100.0
    _write_jsonl(tmp_path / "consumer.jsonl", [
        {"robot_id": 1, "suffix": "odom", "topic": "ros2/robot_1/odom",
         "t0_ns": 10, "t1_ns": None, "t2_ns": 1000010,
         "latency_ns": 1_000_000, "payload_bytes": 8},
        {"robot_id": 1, "suffix": "odom", "topic": "ros2/robot_1/odom",
         "t0_ns": 10, "t1_ns": None, "t2_ns": 1000011,
         "latency_ns": 1_000_001, "payload_bytes": 8},
        {"robot_id": 1, "suffix": "odom", "topic": "ros2/robot_1/odom",
         "t0_ns": 20, "t1_ns": None, "t2_ns": 1000020,
         "latency_ns": 1_000_000, "payload_bytes": 8},
    ])
    report = al.analyze(str(tmp_path))
    assert report["broker"] == "MQTT"
    assert report["expected"] == 2
    assert report["received"] == 3
    assert report["delivery_pct"] == 100.0    # clamped despite 150% raw


def test_analyze_handles_missing_t2(tmp_path):
    # Older artifacts have no t2_ns key — must not raise.
    _write_jsonl(tmp_path / "consumer.jsonl", [
        {"robot_id": 1, "suffix": "scan", "topic": "ros2.robot_1.scan",
         "t0_ns": 100, "t1_ns": 500, "latency_ns": 1_000_000, "payload_bytes": 4},
    ])
    report = al.analyze(str(tmp_path))
    assert report["window_s"] is None         # no t2 -> no window
    assert report["received"] == 1
    assert report["by_suffix"]["scan"]["count"] == 1


def test_write_csv_run_and_suffix_rows(tmp_path):
    pub_dir = tmp_path / "publisher"
    pub_dir.mkdir()
    _write_jsonl(pub_dir / "publisher_robot_1.jsonl", [
        {"robot_id": 1, "suffix": "gnss", "topic": "/robot_1/gnss", "t0_ns": 1},
    ])
    _write_jsonl(tmp_path / "consumer.jsonl", [
        {"robot_id": 1, "suffix": "gnss", "topic": "ros2.robot_1.gnss",
         "t0_ns": 1, "t1_ns": None, "t2_ns": 1000001,
         "latency_ns": 1_000_000, "payload_bytes": 10},
    ])
    report = al.analyze(str(tmp_path))
    csv_path = tmp_path / "out.csv"
    al.write_csv(report, str(csv_path))

    with open(csv_path, newline="") as fh:
        rows = list(csv.DictReader(fh))
    levels = [r["level"] for r in rows]
    assert levels.count("run") == 1
    assert levels.count("suffix") == 1
    run = next(r for r in rows if r["level"] == "run")
    assert run["broker"] == "Kafka"
    assert run["received"] == "1"
    assert run["expected"] == "1"


def test_write_csv_append_single_header(tmp_path):
    _write_jsonl(tmp_path / "consumer.jsonl", [
        {"robot_id": 1, "suffix": "gnss", "topic": "ros2/robot_1/gnss",
         "t0_ns": 1, "t1_ns": None, "t2_ns": 1000001,
         "latency_ns": 1_000_000, "payload_bytes": 10},
    ])
    report = al.analyze(str(tmp_path))
    csv_path = tmp_path / "combined.csv"
    al.write_csv(report, str(csv_path), append=True)
    al.write_csv(report, str(csv_path), append=True)

    with open(csv_path) as fh:
        header_lines = [ln for ln in fh if ln.startswith("run_name,")]
    assert len(header_lines) == 1
