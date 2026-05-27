import sys
from unittest.mock import MagicMock, patch, PropertyMock

# Stub all ROS2 imports so robot_replay can be imported without a ROS install
for mod in [
    "rosbag2_py", "rclpy", "rclpy.node", "rclpy.serialization",
    "rclpy.executors", "rosidl_runtime_py", "rosidl_runtime_py.utilities",
    "nav_msgs", "nav_msgs.msg", "sensor_msgs", "sensor_msgs.msg",
]:
    sys.modules.setdefault(mod, MagicMock())

import pytest
import robot_replay  # noqa: E402  (import after stubs)
from robot_replay import BagLooper


def _make_reader(has_messages: bool):
    """Return a mock SequentialReader that looks full or empty."""
    reader = MagicMock()
    reader.get_all_topics_and_types.return_value = [
        MagicMock(name="/robot_1/gnss", type="sensor_msgs/msg/NavSatFix")
    ]
    reader.has_next.return_value = has_messages
    if has_messages:
        reader.read_next.return_value = ("/robot_1/gnss", b"\x00" * 8, 0)
    return reader


@patch("robot_replay.rosbag2_py")
@patch("robot_replay.get_message", return_value=MagicMock())
@patch("robot_replay.deserialize_message", return_value=MagicMock())
def test_empty_bag_raises_clear_error(mock_deser, mock_get_msg, mock_ros2):
    mock_ros2.SequentialReader.return_value = _make_reader(has_messages=False)
    mock_ros2.StorageOptions = MagicMock()
    mock_ros2.ConverterOptions = MagicMock()
    mock_ros2.StorageFilter = MagicMock()

    looper = BagLooper("/fake/bag", "sensor_msgs/msg/NavSatFix")
    with pytest.raises(RuntimeError, match="contains no messages"):
        next(looper)


@patch("robot_replay.rosbag2_py")
@patch("robot_replay.get_message", return_value=MagicMock())
@patch("robot_replay.deserialize_message", return_value=MagicMock())
def test_non_empty_bag_returns_message(mock_deser, mock_get_msg, mock_ros2):
    mock_ros2.SequentialReader.return_value = _make_reader(has_messages=True)
    mock_ros2.StorageOptions = MagicMock()
    mock_ros2.ConverterOptions = MagicMock()
    mock_ros2.StorageFilter = MagicMock()

    looper = BagLooper("/fake/bag", "sensor_msgs/msg/NavSatFix")
    msg = next(looper)
    assert msg is not None
