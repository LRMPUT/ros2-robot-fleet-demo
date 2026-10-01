"""Tests for health monitoring logic in consume.py."""
import sys
from unittest.mock import MagicMock

# Stub broker libraries so consume.py can be imported standalone
sys.modules.setdefault("confluent_kafka", MagicMock())
sys.modules.setdefault("paho", MagicMock())
sys.modules.setdefault("paho.mqtt", MagicMock())
sys.modules.setdefault("paho.mqtt.client", MagicMock())

import time
import pytest
import consumer.consume as consume


def _reset():
    """Reset all module-level health state between tests."""
    consume._last_seen.clear()
    consume._warned_silent.clear()
    consume._total_msgs = 0
    consume._total_bytes = 0
    consume._warned_no_data = False
    consume._start_time = time.monotonic()
    consume._counts.clear()
    consume._bytes.clear()


def test_record_updates_last_seen():
    _reset()
    consume._record("ros2/robot_3/gnss", 100, robot_id=3)
    assert 3 in consume._last_seen


def test_record_without_robot_id_does_not_update_last_seen():
    _reset()
    consume._record("ros2/robot_3/gnss", 100)
    assert consume._last_seen == {}


def test_silence_warning_added_when_robot_goes_quiet(capsys):
    _reset()
    consume._last_seen[5] = time.monotonic() - 15.0
    consume._check_health(silence_threshold=10.0)
    captured = capsys.readouterr()
    assert "[WARNING]" in captured.out
    assert "robot_5" in captured.out
    assert 5 in consume._warned_silent


def test_silence_warning_not_repeated_for_same_robot(capsys):
    _reset()
    consume._last_seen[5] = time.monotonic() - 15.0
    consume._check_health(silence_threshold=10.0)
    consume._check_health(silence_threshold=10.0)
    captured = capsys.readouterr()
    assert captured.out.count("[WARNING]") == 1  # fires exactly once, not twice


def test_silence_warning_clears_when_robot_recovers(capsys):
    _reset()
    consume._last_seen[5] = time.monotonic() - 15.0
    consume._check_health(silence_threshold=10.0)
    consume._last_seen[5] = time.monotonic()
    consume._check_health(silence_threshold=10.0)
    assert 5 not in consume._warned_silent


def test_no_data_alert_fires_after_15s(capsys):
    _reset()
    consume._start_time = time.monotonic() - 20.0
    consume._total_msgs = 0
    consume._check_health(silence_threshold=10.0)
    captured = capsys.readouterr()
    assert "No messages received" in captured.out
    assert consume._warned_no_data is True


def test_no_data_alert_does_not_fire_before_15s(capsys):
    _reset()
    consume._start_time = time.monotonic() - 5.0
    consume._total_msgs = 0
    consume._check_health(silence_threshold=10.0)
    captured = capsys.readouterr()
    assert "No messages received" not in captured.out


def test_no_data_alert_does_not_fire_if_messages_received(capsys):
    _reset()
    consume._start_time = time.monotonic() - 20.0
    consume._total_msgs = 42
    consume._check_health(silence_threshold=10.0)
    captured = capsys.readouterr()
    assert "No messages received" not in captured.out


def test_no_data_alert_fires_only_once(capsys):
    _reset()
    consume._start_time = time.monotonic() - 20.0
    consume._total_msgs = 0
    consume._check_health(silence_threshold=10.0)
    consume._check_health(silence_threshold=10.0)
    captured = capsys.readouterr()
    assert captured.out.count("No messages received") == 1
