"""Unit tests for GPS fleet centering."""
import pathlib
import sys
from unittest.mock import MagicMock

REPO = pathlib.Path(__file__).resolve().parents[1]

# Stub ROS imports so robot_replay imports without a ROS install.
for mod in [
    "rosbag2_py", "rclpy", "rclpy.node", "rclpy.serialization",
    "rclpy.executors", "rosidl_runtime_py", "rosidl_runtime_py.utilities",
    "nav_msgs", "nav_msgs.msg", "sensor_msgs", "sensor_msgs.msg",
]:
    sys.modules.setdefault(mod, MagicMock())

sys.path.insert(0, str(REPO))
from robot_replay import fleet_center_id  # noqa: E402


def test_fleet_center_midpoint():
    assert fleet_center_id(10) == 5.5
    assert fleet_center_id(1) == 1.0
    assert fleet_center_id(50) == 25.5
