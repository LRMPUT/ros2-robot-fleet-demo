"""Tests for PublisherLatencyLogger in robot_replay.py."""
import json
import sys
from unittest.mock import MagicMock

# Stub ROS imports so robot_replay imports without a ROS install.
for mod in [
    "rosbag2_py", "rclpy", "rclpy.node", "rclpy.serialization",
    "rclpy.executors", "rosidl_runtime_py", "rosidl_runtime_py.utilities",
    "nav_msgs", "nav_msgs.msg", "sensor_msgs", "sensor_msgs.msg",
]:
    sys.modules.setdefault(mod, MagicMock())

from robot_replay import PublisherLatencyLogger  # noqa: E402


def test_logger_writes_one_record_per_call(tmp_path):
    logger = PublisherLatencyLogger(str(tmp_path), robot_id=7)
    logger.record("gnss", "/robot_7/gnss", 123)
    logger.record("odom", "/robot_7/odom", 456)
    logger.close()

    log_path = tmp_path / "publisher_robot_7.jsonl"
    assert log_path.exists()
    lines = log_path.read_text().strip().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0]) == {
        "robot_id": 7, "suffix": "gnss", "topic": "/robot_7/gnss", "t0_ns": 123,
    }
    assert json.loads(lines[1])["suffix"] == "odom"


def test_logger_filename_uses_robot_id(tmp_path):
    logger = PublisherLatencyLogger(str(tmp_path), robot_id=2)
    logger.close()
    assert (tmp_path / "publisher_robot_2.jsonl").exists()
