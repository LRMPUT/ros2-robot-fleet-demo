"""Strict ID allowlists for values interpolated into ksqlDB DDL."""
from __future__ import annotations

import re

# Safe for ksql identifiers / string literals we embed in DDL.
_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")
# PostGIS EWKB hex fragments, optionally joined with '|'.
_HEX_GEO_RE = re.compile(r"^[0-9A-Fa-f|]+$")


class UnsafeIdError(ValueError):
    """Raised when a value fails the allowlist (routers map this to HTTP 400)."""


def require_safe_id(value: str, field: str = "id") -> str:
    """Return value if it matches the allowlist; else raise UnsafeIdError."""
    if not value or not _ID_RE.match(value):
        raise UnsafeIdError(f"Invalid {field}: must match {_ID_RE.pattern}")
    return value


def require_safe_geo_hex(value: str, field: str = "geo") -> str:
    if not value or not _HEX_GEO_RE.match(value):
        raise UnsafeIdError(
            f"Invalid {field}: expected hex (optional '|' separators)"
        )
    return value


def http_safe_id(value: str, field: str = "id") -> str:
    """Like require_safe_id but raises FastAPI HTTPException(400)."""
    try:
        return require_safe_id(value, field)
    except UnsafeIdError as e:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail=str(e)) from e


def http_safe_geo_hex(value: str, field: str = "geo") -> str:
    try:
        return require_safe_geo_hex(value, field)
    except UnsafeIdError as e:
        from fastapi import HTTPException
        raise HTTPException(status_code=400, detail=str(e)) from e
