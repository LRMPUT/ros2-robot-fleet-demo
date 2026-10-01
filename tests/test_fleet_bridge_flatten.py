"""Unit tests for fleet_bridge payload flattening (no Kafka needed)."""
import pathlib
import sys

REPO = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "ksqldb"))

from app.fleet_bridge import _flatten_gnss, _flatten_odom  # noqa: E402


def test_flatten_gnss_nested_header():
    out = _flatten_gnss("robot_3", {
        "latitude": 46.1,
        "longitude": 3.2,
        "header": {"stamp": {"sec": 10, "nanosec": 500_000_000}},
    })
    assert out == {
        "robot_id": "robot_3",
        "timestamp": 10_500,
        "latitude": 46.1,
        "longitude": 3.2,
    }


def test_flatten_gnss_prefers_ts_envelope():
    out = _flatten_gnss("robot_1", {
        "latitude": 1.0,
        "longitude": 2.0,
        "_ts": {"t0_ns": 2_000_000_000},
        "header": {"stamp": {"sec": 0, "nanosec": 0}},
    })
    assert out["timestamp"] == 2000


def test_flatten_odom():
    out = _flatten_odom("robot_2", {
        "pose": {"pose": {"position": {"x": 1.5, "y": -2.0, "z": 0.25}}},
        "header": {"stamp": {"sec": 1, "nanosec": 0}},
    })
    assert out == {
        "robot_id": "robot_2",
        "timestamp": 1000,
        "position_x": 1.5,
        "position_y": -2.0,
        "position_z": 0.25,
    }


def test_flatten_rejects_bad_payload():
    assert _flatten_gnss("robot_1", {"latitude": 1.0}) is None
    assert _flatten_odom("robot_1", {}) is None
