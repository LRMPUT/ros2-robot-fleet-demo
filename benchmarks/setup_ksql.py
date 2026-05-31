#!/usr/bin/env python3
"""Configure the ksqlDB stack for a geofence benchmark run via the REST API.

Reproduces, for N robots, exactly what you did by hand in the Swagger UI:
    1. POST /ksqldb/robots      {"id": "robot_<i>_s<salt>"}      (select robot)
    2. POST /ksqldb/zones       {"id": "3MONT", "geo": "<hex>"}  (create zone)
    3. POST /ksqldb/geofence    {robot_id, zone_id, config_name} (assign rule)

Each geofence assignment makes the API CREATE OR REPLACE a per-robot ksqlDB
stream that writes OUTSIDE alerts (with t0/t1/t2 timestamps) to
robot_geofence_alerts.

Usage:
    python setup_ksql.py --robots 10
    python setup_ksql.py --robots 50 --api-url http://localhost:8000
"""
from __future__ import annotations

import argparse
import sys
import time

import requests

import config


def _post(session: requests.Session, url: str, payload: dict) -> requests.Response:
    resp = session.post(
        url,
        json=payload,
        headers={"accept": "application/json", "Content-Type": "application/json"},
        timeout=30,
    )
    return resp


def setup(api_url: str, n: int) -> None:
    robots = config.robot_ids(n)
    session = requests.Session()

    print(f"[setup] API={api_url}  robots={n}  zone={config.ZONE_ID}")

    # 1. Select every robot.
    for rid in robots:
        r = _post(session, f"{api_url}/ksqldb/robots", {"id": rid})
        if r.status_code != 200:
            print(f"[setup] ERROR selecting {rid}: {r.status_code} {r.text}")
            sys.exit(1)
    print(f"[setup] selected {len(robots)} robots")

    # 2. Create the zone (idempotent: INSERT OR REPLACE on the server).
    r = _post(
        session,
        f"{api_url}/ksqldb/zones",
        {"id": config.ZONE_ID, "geo": config.ZONE_HEX},
    )
    if r.status_code != 200:
        print(f"[setup] ERROR creating zone: {r.status_code} {r.text}")
        sys.exit(1)
    print(f"[setup] zone '{config.ZONE_ID}' created")

    # 3. Assign the geofence rule to every robot. This is the heavy step: each
    #    call issues a CREATE OR REPLACE STREAM to ksqlDB, so pace it lightly.
    for i, rid in enumerate(robots, 1):
        payload = {
            "robot_id": rid,
            "zone_id": config.ZONE_ID,
            "config_name": config.CONFIG_NAME,
        }
        r = _post(session, f"{api_url}/ksqldb/geofence", payload)
        if r.status_code != 200:
            print(f"[setup] ERROR assigning geofence to {rid}: "
                  f"{r.status_code} {r.text}")
            sys.exit(1)
        print(f"[setup]   [{i}/{len(robots)}] geofence assigned -> {rid}")
        time.sleep(0.2)  # let ksqlDB register each persistent query

    print(f"[setup] done. {len(robots)} geofence monitor streams active.")
    print("[setup] robots under test:")
    for rid in robots:
        print(f"          {rid}")


def main() -> None:
    p = argparse.ArgumentParser(description="Configure ksqlDB for a geofence benchmark run")
    p.add_argument("--robots", type=int, default=config.NUM_ROBOTS,
                   help=f"number of robots, 1..{len(config.SALTS)} (default {config.NUM_ROBOTS})")
    p.add_argument("--api-url", default=config.API_URL,
                   help=f"FastAPI base URL (default {config.API_URL})")
    args = p.parse_args()
    setup(args.api_url, args.robots)


if __name__ == "__main__":
    main()
