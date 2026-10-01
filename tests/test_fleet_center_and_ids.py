"""Unit tests for GPS fleet centering and ksql ID allowlists."""
import pathlib
import sys
from unittest.mock import MagicMock

import pytest

REPO = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "ksqldb"))

# Stub ROS imports so robot_replay imports without a ROS install.
for mod in [
    "rosbag2_py", "rclpy", "rclpy.node", "rclpy.serialization",
    "rclpy.executors", "rosidl_runtime_py", "rosidl_runtime_py.utilities",
    "nav_msgs", "nav_msgs.msg", "sensor_msgs", "sensor_msgs.msg",
]:
    sys.modules.setdefault(mod, MagicMock())

from robot_replay import fleet_center_id  # noqa: E402
from app.adapters.ksqldb.ids import (  # noqa: E402
    UnsafeIdError,
    require_safe_geo_hex,
    require_safe_id,
)


def test_fleet_center_midpoint():
    assert fleet_center_id(10) == 5.5
    assert fleet_center_id(1) == 1.0
    assert fleet_center_id(50) == 25.5


def test_require_safe_id_accepts():
    assert require_safe_id("robot_1", "robot_id") == "robot_1"
    assert require_safe_id("spe1", "config_name") == "spe1"


def test_require_safe_id_rejects():
    with pytest.raises(UnsafeIdError):
        require_safe_id("robot'; DROP TABLE x", "robot_id")


def test_require_safe_geo_hex():
    assert require_safe_geo_hex("AABB|ccdd", "geo") == "AABB|ccdd"
    with pytest.raises(UnsafeIdError):
        require_safe_geo_hex("not hex!", "geo")
