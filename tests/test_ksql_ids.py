"""Unit tests for ksql ID allowlists."""
import pathlib
import sys

import pytest

REPO = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "ksqldb"))

from app.adapters.ksqldb.ids import (  # noqa: E402
    UnsafeIdError,
    require_safe_geo_hex,
    require_safe_id,
)


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
