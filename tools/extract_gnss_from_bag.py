#!/usr/bin/env python3
"""Extract GNSS trajectories directly from a rosbag2 SQLite3 file.

Reads NavSatFix messages from a single source topic (default:
/follower/gps/fix), applies a pre-computed per-robot lat/lon offset that
tiles robots in a seamless boustrophedon grid (matching the field geometry
of the INRAE parcelle bag), and writes one tab-separated file per robot:

    robot_1_gnss.txt:  timestamp_ns\tlatitude\tlongitude\taltitude

No ROS installation required — decodes CDR binary directly.

Supported fleet sizes with hardcoded geometry-aware offsets: 1, 5, 10, 25, 50.
The offsets were computed by gen_fleet_offsets.py from the INRAE parcelle bag
(strip_sp=5.60 m, cell_cross=22.39 m, cell_along=37.72 m).

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

# ── per-robot offsets (dlat, dlon) ────────────────────────────────────────────
# Computed by gen_fleet_offsets.py from bags/rorbots_follower_leader_parcelle_1MONT_ros2/
# strip_sp=5.60 m  cell_cross=22.39 m  cell_along=37.72 m

# N=1 (1 rows × 1 cols)
ROBOT_GPS_OFFSETS_1 = {
     1: (+0.00000000, +0.00000000),  # row=0 col=0
}

# N=5 (1 rows × 5 cols)
ROBOT_GPS_OFFSETS_5 = {
     1: (+0.00000000, +0.00000000),  # row=0 col=0
     2: (+0.00011511, +0.00023886),  # row=0 col=1
     3: (+0.00023022, +0.00047772),  # row=0 col=2
     4: (+0.00034533, +0.00071658),  # row=0 col=3
     5: (+0.00046044, +0.00095544),  # row=0 col=4
}

# N=10 (2 rows × 5 cols)
ROBOT_GPS_OFFSETS_10 = {
     1: (+0.00000000, +0.00000000),  # row=0 col=0
     2: (+0.00011511, +0.00023886),  # row=0 col=1
     3: (+0.00023022, +0.00047772),  # row=0 col=2
     4: (+0.00034533, +0.00071658),  # row=0 col=3
     5: (+0.00046044, +0.00095544),  # row=0 col=4
     6: (-0.00026413, +0.00030751),  # row=1 col=0
     7: (-0.00014902, +0.00054637),  # row=1 col=1
     8: (-0.00003391, +0.00078523),  # row=1 col=2
     9: (+0.00008120, +0.00102409),  # row=1 col=3
    10: (+0.00019631, +0.00126295),  # row=1 col=4
}

# N=25 (5 rows × 5 cols)
ROBOT_GPS_OFFSETS_25 = {
     1: (+0.00000000, +0.00000000),  # row=0 col=0
     2: (+0.00011511, +0.00023886),  # row=0 col=1
     3: (+0.00023022, +0.00047772),  # row=0 col=2
     4: (+0.00034533, +0.00071658),  # row=0 col=3
     5: (+0.00046044, +0.00095544),  # row=0 col=4
     6: (-0.00026413, +0.00030751),  # row=1 col=0
     7: (-0.00014902, +0.00054637),  # row=1 col=1
     8: (-0.00003391, +0.00078523),  # row=1 col=2
     9: (+0.00008120, +0.00102409),  # row=1 col=3
    10: (+0.00019631, +0.00126295),  # row=1 col=4
    11: (-0.00052825, +0.00061503),  # row=2 col=0
    12: (-0.00041314, +0.00085389),  # row=2 col=1
    13: (-0.00029803, +0.00109274),  # row=2 col=2
    14: (-0.00018292, +0.00133160),  # row=2 col=3
    15: (-0.00006781, +0.00157046),  # row=2 col=4
    16: (-0.00079238, +0.00092254),  # row=3 col=0
    17: (-0.00067727, +0.00116140),  # row=3 col=1
    18: (-0.00056216, +0.00140026),  # row=3 col=2
    19: (-0.00044705, +0.00163912),  # row=3 col=3
    20: (-0.00033194, +0.00187798),  # row=3 col=4
    21: (-0.00105651, +0.00123005),  # row=4 col=0
    22: (-0.00094140, +0.00146891),  # row=4 col=1
    23: (-0.00082629, +0.00170777),  # row=4 col=2
    24: (-0.00071118, +0.00194663),  # row=4 col=3
    25: (-0.00059607, +0.00218549),  # row=4 col=4
}

# N=50 (5 rows × 10 cols)
ROBOT_GPS_OFFSETS_50 = {
     1: (+0.00000000, +0.00000000),  # row=0 col=0
     2: (+0.00011511, +0.00023886),  # row=0 col=1
     3: (+0.00023022, +0.00047772),  # row=0 col=2
     4: (+0.00034533, +0.00071658),  # row=0 col=3
     5: (+0.00046044, +0.00095544),  # row=0 col=4
     6: (+0.00057555, +0.00119430),  # row=0 col=5
     7: (+0.00069066, +0.00143316),  # row=0 col=6
     8: (+0.00080577, +0.00167201),  # row=0 col=7
     9: (+0.00092088, +0.00191087),  # row=0 col=8
    10: (+0.00103599, +0.00214973),  # row=0 col=9
    11: (-0.00026413, +0.00030751),  # row=1 col=0
    12: (-0.00014902, +0.00054637),  # row=1 col=1
    13: (-0.00003391, +0.00078523),  # row=1 col=2
    14: (+0.00008120, +0.00102409),  # row=1 col=3
    15: (+0.00019631, +0.00126295),  # row=1 col=4
    16: (+0.00031142, +0.00150181),  # row=1 col=5
    17: (+0.00042653, +0.00174067),  # row=1 col=6
    18: (+0.00054164, +0.00197953),  # row=1 col=7
    19: (+0.00065675, +0.00221839),  # row=1 col=8
    20: (+0.00077186, +0.00245725),  # row=1 col=9
    21: (-0.00052825, +0.00061503),  # row=2 col=0
    22: (-0.00041314, +0.00085389),  # row=2 col=1
    23: (-0.00029803, +0.00109274),  # row=2 col=2
    24: (-0.00018292, +0.00133160),  # row=2 col=3
    25: (-0.00006781, +0.00157046),  # row=2 col=4
    26: (+0.00004730, +0.00180932),  # row=2 col=5
    27: (+0.00016241, +0.00204818),  # row=2 col=6
    28: (+0.00027752, +0.00228704),  # row=2 col=7
    29: (+0.00039263, +0.00252590),  # row=2 col=8
    30: (+0.00050774, +0.00276476),  # row=2 col=9
    31: (-0.00079238, +0.00092254),  # row=3 col=0
    32: (-0.00067727, +0.00116140),  # row=3 col=1
    33: (-0.00056216, +0.00140026),  # row=3 col=2
    34: (-0.00044705, +0.00163912),  # row=3 col=3
    35: (-0.00033194, +0.00187798),  # row=3 col=4
    36: (-0.00021683, +0.00211684),  # row=3 col=5
    37: (-0.00010172, +0.00235569),  # row=3 col=6
    38: (+0.00001339, +0.00259455),  # row=3 col=7
    39: (+0.00012850, +0.00283341),  # row=3 col=8
    40: (+0.00024361, +0.00307227),  # row=3 col=9
    41: (-0.00105651, +0.00123005),  # row=4 col=0
    42: (-0.00094140, +0.00146891),  # row=4 col=1
    43: (-0.00082629, +0.00170777),  # row=4 col=2
    44: (-0.00071118, +0.00194663),  # row=4 col=3
    45: (-0.00059607, +0.00218549),  # row=4 col=4
    46: (-0.00048096, +0.00242435),  # row=4 col=5
    47: (-0.00036585, +0.00266321),  # row=4 col=6
    48: (-0.00025074, +0.00290207),  # row=4 col=7
    49: (-0.00013563, +0.00314093),  # row=4 col=8
    50: (-0.00002052, +0.00337979),  # row=4 col=9
}

OFFSET_TABLES = {
     1: ROBOT_GPS_OFFSETS_1,
     5: ROBOT_GPS_OFFSETS_5,
    10: ROBOT_GPS_OFFSETS_10,
    25: ROBOT_GPS_OFFSETS_25,
    50: ROBOT_GPS_OFFSETS_50,
}


# ── CDR NavSatFix decoder ────────────────────────────────────────────────────

def _align(abs_off: int, n: int, payload_start: int = 4) -> int:
    rel = abs_off - payload_start
    rel = (rel + n - 1) & ~(n - 1)
    return rel + payload_start


def decode_navsatfix_cdr(data: bytes) -> tuple[int, float, float, float] | None:
    """Return (timestamp_ns, lat, lon, alt) or None if decode fails."""
    try:
        if data[1] not in (0x01, 0x00):
            return None

        sec     = struct.unpack_from('<I', data, 4)[0]
        nanosec = struct.unpack_from('<I', data, 8)[0]
        flen    = struct.unpack_from('<I', data, 12)[0]
        off     = 16 + flen

        off += 1                    # NavSatStatus.status (int8)
        off  = _align(off, 2) + 2  # NavSatStatus.service (uint16)
        off  = _align(off, 8)      # float64 alignment
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
    n = max(robot_ids)

    if n not in OFFSET_TABLES:
        supported = sorted(OFFSET_TABLES)
        print(f"ERROR: no offset table for N={n}. Supported fleet sizes: {supported}", file=sys.stderr)
        print(f"  Run gen_fleet_offsets.py --n {n} to compute and add a new table.", file=sys.stderr)
        sys.exit(1)

    offsets = OFFSET_TABLES[n]

    db3 = find_db3(args.bag)
    print(f"Bag:    {db3}")
    print(f"Topic:  {args.topic}")
    print(f"Robots: {robot_ids}")
    print(f"Grid:   {n}-robot table ({n} robots)")
    print(f"Out:    {args.out}/")
    print()

    base_pts = read_navsatfix(db3, args.topic)
    print(f"  decoded: {len(base_pts):,} valid points")
    print()

    for rid in robot_ids:
        dlat, dlon = offsets[rid]
        pts = [(ts, lat + dlat, lon + dlon, alt) for ts, lat, lon, alt in base_pts]
        path = write_robot_file(args.out, rid, pts)
        print(f"  robot_{rid}: {len(pts):,} pts → {path}")

    print(f"\nDone. {len(robot_ids) * len(base_pts):,} total points written.")


if __name__ == '__main__':
    main()
