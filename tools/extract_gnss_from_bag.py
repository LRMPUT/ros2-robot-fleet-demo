#!/usr/bin/env python3
"""Extract GNSS trajectories directly from a rosbag2 SQLite3 file.

Reads NavSatFix messages from a single source topic (default:
/follower/gps/fix), applies a per-robot lat/lon offset matching
robot_replay.py, and writes one tab-separated file per robot:

    robot_1_gnss.txt:  timestamp_ns\tlatitude\tlongitude\taltitude

No ROS installation required — decodes CDR binary directly.

Usage:
    python3 tools/extract_gnss_from_bag.py --bag bags/my_bag/ --robots 10
    python3 tools/extract_gnss_from_bag.py --bag bags/my_bag/ --topic /leader/gps/fix --out traj/
    python3 tools/extract_gnss_from_bag.py --bag bags/my_bag/ --robots 1-5,8
"""
from __future__ import annotations

import argparse
import glob
import os
import sqlite3
import struct
import sys

# Must match robot_replay.py — perpendicular to field path, fleet centred at id=5.5
LAT_OFFSET_DEG_PER_ID = 0.00001777
LON_OFFSET_DEG_PER_ID = 0.00007371
FLEET_CENTER_ID = 5.5


# ── CDR NavSatFix decoder ────────────────────────────────────────────────────
# CDR alignment is measured from the start of the payload (after the 4-byte
# encapsulation header), not from absolute byte 0.

def _align(abs_off: int, n: int, payload_start: int = 4) -> int:
    rel = abs_off - payload_start
    rel = (rel + n - 1) & ~(n - 1)
    return rel + payload_start


def decode_navsatfix_cdr(data: bytes) -> tuple[int, float, float, float] | None:
    """Return (timestamp_ns, lat, lon, alt) or None if decode fails."""
    try:
        # Encapsulation header (4 bytes), little-endian marker at byte 1
        if data[1] not in (0x01, 0x00):
            return None

        sec     = struct.unpack_from('<I', data, 4)[0]
        nanosec = struct.unpack_from('<I', data, 8)[0]
        flen    = struct.unpack_from('<I', data, 12)[0]   # frame_id length (incl. null)
        off     = 16 + flen                               # after string bytes

        # NavSatStatus.status (int8)
        off += 1
        # NavSatStatus.service (uint16, 2-byte aligned from payload start)
        off  = _align(off, 2)
        off += 2

        # latitude/longitude/altitude (float64, 8-byte aligned from payload start)
        off = _align(off, 8)
        lat, lon, alt = struct.unpack_from('<ddd', data, off)

        ts_ns = sec * 1_000_000_000 + nanosec
        return ts_ns, lat, lon, alt
    except Exception:
        return None


# ── bag reading ───────────────────────────────────────────────────────────────

def find_db3(bag_path: str) -> str:
    if bag_path.endswith('.db3'):
        return bag_path
    matches = glob.glob(os.path.join(bag_path, '*.db3'))
    if not matches:
        print(f"ERROR: no .db3 file found in {bag_path}", file=sys.stderr)
        sys.exit(1)
    return matches[0]


def get_topic_id(conn: sqlite3.Connection, topic_name: str) -> int:
    row = conn.execute(
        'SELECT id FROM topics WHERE name=?', (topic_name,)
    ).fetchone()
    if row is None:
        available = [r[0] for r in conn.execute(
            "SELECT name FROM topics WHERE type='sensor_msgs/msg/NavSatFix'"
        )]
        print(f"ERROR: topic '{topic_name}' not found.", file=sys.stderr)
        print(f"  Available NavSatFix topics: {available}", file=sys.stderr)
        sys.exit(1)
    return row[0]


def read_navsatfix(db3_path: str, topic_name: str) -> list[tuple[int, float, float, float]]:
    conn = sqlite3.connect(db3_path)
    topic_id = get_topic_id(conn, topic_name)
    count = conn.execute(
        'SELECT COUNT(*) FROM messages WHERE topic_id=?', (topic_id,)
    ).fetchone()[0]
    print(f"  {topic_name}: {count:,} messages in bag")

    pts = []
    failed = 0
    for (data,) in conn.execute(
        'SELECT data FROM messages WHERE topic_id=? ORDER BY timestamp', (topic_id,)
    ):
        result = decode_navsatfix_cdr(bytes(data))
        if result:
            pts.append(result)
        else:
            failed += 1

    conn.close()
    if failed:
        print(f"  WARNING: {failed} messages failed to decode", file=sys.stderr)
    return pts


# ── robot offset ──────────────────────────────────────────────────────────────

def apply_robot_offset(
    pts: list[tuple[int, float, float, float]],
    robot_id: int,
) -> list[tuple[int, float, float, float]]:
    dlat = (robot_id - FLEET_CENTER_ID) * LAT_OFFSET_DEG_PER_ID
    dlon = (robot_id - FLEET_CENTER_ID) * LON_OFFSET_DEG_PER_ID
    return [(ts, lat + dlat, lon + dlon, alt) for ts, lat, lon, alt in pts]


# ── writer ────────────────────────────────────────────────────────────────────

def write_robot_file(
    out_dir: str,
    robot_id: int,
    pts: list[tuple[int, float, float, float]],
) -> str:
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"robot_{robot_id}_gnss.txt")
    with open(path, 'w') as f:
        f.write("timestamp_ns\tlatitude\tlongitude\taltitude\n")
        for ts, lat, lon, alt in pts:
            f.write(f"{ts}\t{lat:.9f}\t{lon:.9f}\t{alt:.4f}\n")
    return path


# ── argument parsing ──────────────────────────────────────────────────────────

def parse_robots(spec: str) -> list[int]:
    ids: list[int] = []
    for part in spec.split(','):
        part = part.strip()
        if '-' in part:
            a, b = part.split('-', 1)
            ids.extend(range(int(a), int(b) + 1))
        else:
            ids.append(int(part))
    return sorted(set(ids))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract GNSS trajectories from a rosbag2 SQLite3 bag"
    )
    parser.add_argument('--bag',    required=True,
                        help='Path to bag directory or .db3 file')
    parser.add_argument('--topic',  default='/follower/gps/fix',
                        help='Source NavSatFix topic (default: /follower/gps/fix)')
    parser.add_argument('--robots', default='1-10',
                        help="Robot IDs: '1-10', '1,3,5', etc. (default: 1-10)")
    parser.add_argument('--out',    default='trajectories',
                        help='Output directory (default: trajectories/)')
    args = parser.parse_args()

    robot_ids = parse_robots(args.robots)
    db3 = find_db3(args.bag)

    print(f"Bag:    {db3}")
    print(f"Topic:  {args.topic}")
    print(f"Robots: {robot_ids}")
    print(f"Out:    {args.out}/")
    print()

    base_pts = read_navsatfix(db3, args.topic)
    print(f"  decoded: {len(base_pts):,} valid points")
    print()

    for rid in robot_ids:
        pts = apply_robot_offset(base_pts, rid)
        path = write_robot_file(args.out, rid, pts)
        print(f"  robot_{rid}: {len(pts):,} pts → {path}")

    print(f"\nDone. {len(robot_ids) * len(base_pts):,} total points written.")


if __name__ == '__main__':
    main()
