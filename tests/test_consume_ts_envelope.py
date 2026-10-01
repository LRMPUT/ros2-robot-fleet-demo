"""Tests for the `_ts` staged-timestamp envelope in consume.py decode."""
import json
import sys
from unittest.mock import MagicMock

# Stub broker libraries so consume.py imports without them installed.
sys.modules.setdefault("confluent_kafka", MagicMock())
sys.modules.setdefault("paho", MagicMock())
sys.modules.setdefault("paho.mqtt", MagicMock())
sys.modules.setdefault("paho.mqtt.client", MagicMock())

import consumer.consume as consume  # noqa: E402


def _json_payload(obj) -> bytes:
    return json.dumps(obj).encode()


def test_t0_prefers_ts_envelope():
    payload = _json_payload({
        "header": {"stamp": {"sec": 1, "nanosec": 0}},
        "_ts": {"t0_ns": 1779985112917221721, "t1_ns": 1779985112918000000},
    })
    assert consume._t0_from_json(payload) == 1779985112917221721


def test_t0_falls_back_to_header_stamp():
    payload = _json_payload({"header": {"stamp": {"sec": 2, "nanosec": 5}}})
    assert consume._t0_from_json(payload) == 2_000_000_005


def test_t1_from_ts_envelope():
    payload = _json_payload({
        "header": {"stamp": {"sec": 1, "nanosec": 0}},
        "_ts": {"t0_ns": 1000, "t1_ns": 2000},
    })
    assert consume._t1_from_json(payload) == 2000


def test_t1_none_without_envelope():
    payload = _json_payload({"header": {"stamp": {"sec": 1, "nanosec": 0}}})
    assert consume._t1_from_json(payload) is None


def test_decode_json_returns_t0_and_t1():
    payload = _json_payload({
        "header": {"stamp": {"sec": 1, "nanosec": 0}},
        "_ts": {"t0_ns": 1000, "t1_ns": 2000},
    })
    t0, t1 = consume._decode(payload, "gnss", "json")
    assert (t0, t1) == (1000, 2000)


def test_decode_json_no_envelope_t1_none():
    payload = _json_payload({"header": {"stamp": {"sec": 1, "nanosec": 7}}})
    t0, t1 = consume._decode(payload, "gnss", "json")
    assert t0 == 1_000_000_007
    assert t1 is None


def test_decode_cdr_t1_always_none():
    # CDR cannot carry the envelope; t1 is None and t0 comes from CDR (here the
    # bytes are not valid CDR, so t0 is also None — the point is t1 is None).
    t0, t1 = consume._decode(b"\x00\x01not-cdr", "gnss", "cdr")
    assert t1 is None
