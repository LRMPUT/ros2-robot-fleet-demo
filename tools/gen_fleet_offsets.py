#!/usr/bin/env python3
"""Compute per-robot GPS offsets for seamless boustrophedon fleet coverage.

Algorithm
---------
1. Load the base NavSatFix trajectory from the bag (single robot, CDR decoded).

2. Smooth the N coordinate heavily with a Gaussian filter (sigma≈50 samples)
   to suppress GPS noise while preserving the large-scale boustrophedon shape.

3. Find local maxima / minima in the smoothed N coordinate.
   These are the U-turn tips — the "vertices of the turns" the user can see
   at each end of the strips:
     • maxima → NW tips  (robot reaches top-left of each strip and reverses)
     • minima → SE tips  (robot reaches bottom-right and reverses)

4. Derive the field geometry from the tips:
     cross_dir  = mean direction between consecutive tips of the same side
                  (perpendicular to the strips — this is the tiling direction)
     strip_dir  = mean direction from NW-tip to SE-tip of the same strip
                  (along the strip — the direction the robot travels)
     strip_len  = mean NW-to-SE tip distance  (strip length ≈ 38 m)
     strip_sp   = mean spacing between consecutive tips on one side (≈ 5.6 m)

5. Cell size for one robot:
     cell_cross = n_strips × strip_sp   (n_strips = number of strips detected)
     cell_along = strip_len

   With this cell size the last strip of robot k and the first strip of
   robot k+1 are exactly strip_sp apart — the same gap as within a single
   robot's trajectory.  All U-turn tips of all robots therefore lie on the
   same pair of boundary lines.

6. For a (n_rows × n_cols) grid, robot at position (row, col) gets:
     offset_m = col × cell_cross × cross_dir
              + row × cell_along × strip_dir
     dlat = offset_m[N] / DEG2M
     dlon = offset_m[E] / (DEG2M × cos_lat)

Usage
-----
    python3 tools/gen_fleet_offsets.py --bag bags/my_bag_ros2/ --n 10
    python3 tools/gen_fleet_offsets.py --bag bags/my_bag_ros2/ --n 25 --rows 5
    python3 tools/gen_fleet_offsets.py --bag bags/my_bag_ros2/ --n 50 --rows 5
    python3 tools/gen_fleet_offsets.py --bag bags/my_bag_ros2/ --all
"""
from __future__ import annotations

import argparse
import glob
import os
import sqlite3
import struct
import sys

import numpy as np
from scipy.ndimage import gaussian_filter1d
from scipy.signal import argrelextrema


# ── CDR NavSatFix decoder (no ROS needed) ────────────────────────────────────

def _align(off: int, n: int, ps: int = 4) -> int:
    rel = off - ps
    rel = (rel + n - 1) & ~(n - 1)
    return rel + ps


def _decode_navsatfix(data: bytes):
    try:
        if data[1] not in (0x01, 0x00):
            return None
        flen = struct.unpack_from('<I', data, 12)[0]
        off = 16 + flen + 1
        off = _align(off, 2) + 2
        off = _align(off, 8)
        lat, lon, _ = struct.unpack_from('<ddd', data, off)
        return lat, lon
    except Exception:
        return None


def load_base_trajectory(bag_path: str, topic: str = '/follower/gps/fix'):
    """Return list of (lat, lon) from the first NavSatFix topic in the bag."""
    matches = glob.glob(os.path.join(bag_path, '*.db3'))
    if not matches:
        if bag_path.endswith('.db3'):
            matches = [bag_path]
        else:
            print(f"ERROR: no .db3 in {bag_path}", file=sys.stderr)
            sys.exit(1)
    db3 = matches[0]
    conn = sqlite3.connect(db3)
    row = conn.execute("SELECT id FROM topics WHERE name=?", (topic,)).fetchone()
    if row is None:
        available = [r[0] for r in conn.execute(
            "SELECT name FROM topics WHERE type='sensor_msgs/msg/NavSatFix'")]
        print(f"ERROR: topic '{topic}' not found. Available: {available}", file=sys.stderr)
        sys.exit(1)
    tid = row[0]
    pts = [r for (d,) in conn.execute(
        "SELECT data FROM messages WHERE topic_id=? ORDER BY timestamp", (tid,))
        if (r := _decode_navsatfix(bytes(d))) is not None]
    conn.close()
    print(f"  Loaded {len(pts):,} NavSatFix points from {db3}")
    return pts


# ── geometry analysis ─────────────────────────────────────────────────────────

def analyse_trajectory(pts: list[tuple[float, float]],
                       smooth_sigma: float = 50.0,
                       order: int = 150):
    """
    Detect U-turn tips, derive strip geometry.

    Returns dict with keys:
      cross_dir, strip_dir  – unit vectors (E, N)
      strip_len, strip_sp   – metres
      n_strips              – number of strips in base trajectory
      cell_cross, cell_along – metres, seamless cell dimensions
      lat0, cos_lat, DEG2M  – for degree conversion
      nw_tips, se_tips      – (E,N) tip arrays in metric space
    """
    lats = np.array([p[0] for p in pts])
    lons = np.array([p[1] for p in pts])
    lat0    = lats.mean()
    cos_lat = np.cos(np.radians(lat0))
    DEG2M   = 111320.0

    x = (lons - lons.mean()) * DEG2M * cos_lat   # East [m]
    y = (lats - lats.mean()) * DEG2M              # North [m]

    # Smooth N coordinate and find U-turn extremes
    y_sm = gaussian_filter1d(y, sigma=smooth_sigma)
    nw_idx = argrelextrema(y_sm, np.greater, order=order)[0]
    se_idx = argrelextrema(y_sm, np.less,    order=order)[0]

    # Filter out near-duplicate tips (bag start/end artefacts)
    def dedup(idx, min_dist=5.0):
        if len(idx) == 0:
            return idx
        coords = np.column_stack([x[idx], y[idx]])
        keep = [0]  # indices into coords/idx, not into x/y
        for i in range(1, len(idx)):
            if np.linalg.norm(coords[i] - coords[keep[-1]]) > min_dist:
                keep.append(i)
        return idx[keep]

    nw_idx = dedup(nw_idx)
    se_idx = dedup(se_idx)

    n_tips = min(len(nw_idx), len(se_idx))
    if n_tips == 0:
        raise RuntimeError("No U-turn tips detected — try reducing smooth_sigma or order.")

    nw_tips = np.column_stack([x[nw_idx[:n_tips]], y[nw_idx[:n_tips]]])
    se_tips = np.column_stack([x[se_idx[:n_tips]], y[se_idx[:n_tips]]])

    print(f"  NW tips: {n_tips}  SE tips: {n_tips}")

    # Cross-strip direction from consecutive NW tips
    diffs = np.diff(nw_tips, axis=0)
    cross_dir = diffs.mean(axis=0)
    cross_dir /= np.linalg.norm(cross_dir)
    if cross_dir[0] < 0:
        cross_dir = -cross_dir

    # Strip direction from NW→SE
    strip_vecs = se_tips - nw_tips
    strip_dir  = strip_vecs.mean(axis=0)
    strip_dir /= np.linalg.norm(strip_dir)

    strip_len = float(np.linalg.norm(strip_vecs, axis=1).mean())
    strip_sp  = float(np.linalg.norm(diffs, axis=1).mean())

    cell_cross = n_tips * strip_sp
    cell_along = strip_len

    return dict(
        cross_dir  = cross_dir,
        strip_dir  = strip_dir,
        strip_len  = strip_len,
        strip_sp   = strip_sp,
        n_strips   = n_tips,
        cell_cross = cell_cross,
        cell_along = cell_along,
        lat0       = lat0,
        cos_lat    = cos_lat,
        DEG2M      = DEG2M,
        nw_tips    = nw_tips,
        se_tips    = se_tips,
        x_base     = x,
        y_base     = y,
    )


# ── offset table generation ───────────────────────────────────────────────────

def make_offsets(geo: dict, n_robots: int, n_rows: int):
    """
    Return dict robot_id → (dlat, dlon) for a (n_rows × n_cols) grid.
    n_cols = n_robots // n_rows  (must divide evenly).
    """
    n_cols = n_robots // n_rows
    assert n_cols * n_rows == n_robots, \
        f"{n_robots} robots must be divisible by {n_rows} rows"

    cross_dir  = geo['cross_dir']
    strip_dir  = geo['strip_dir']
    cell_cross = geo['cell_cross']
    cell_along = geo['cell_along']
    DEG2M      = geo['DEG2M']
    cos_lat    = geo['cos_lat']

    offsets = {}
    for robot_id in range(1, n_robots + 1):
        col = (robot_id - 1) % n_cols
        row = (robot_id - 1) // n_cols
        off_m = col * cell_cross * cross_dir + row * cell_along * strip_dir
        dlon = off_m[0] / (DEG2M * cos_lat)
        dlat = off_m[1] / DEG2M
        offsets[robot_id] = (dlat, dlon)
    return offsets


def print_table(offsets: dict, geo: dict, n_robots: int, n_rows: int):
    n_cols = n_robots // n_rows
    print(f"\n# N={n_robots} ({n_rows} rows × {n_cols} cols)")
    print(f"# strip_sp={geo['strip_sp']:.2f} m  cell_cross={geo['cell_cross']:.2f} m"
          f"  cell_along={geo['cell_along']:.2f} m")
    print(f"ROBOT_GPS_OFFSETS_{n_robots} = {{")
    for rid, (dlat, dlon) in offsets.items():
        col = (rid - 1) % n_cols
        row = (rid - 1) // n_cols
        print(f"    {rid:2d}: ({dlat:+.8f}, {dlon:+.8f}),  # row={row} col={col}")
    print("}")


# ── satellite map image ───────────────────────────────────────────────────────

COLORS_16 = [
    "#e41a1c", "#377eb8", "#4daf4a", "#984ea3", "#ff7f00",
    "#a65628", "#f781bf", "#66c2a5", "#fc8d62", "#8da0cb",
    "#e78ac3", "#a6d854", "#ffd92f", "#e5c494", "#b3b3b3",
    "#1b9e77",
]


def robot_color(robot_id: int) -> str:
    return COLORS_16[(robot_id - 1) % len(COLORS_16)]


def plot_fleet(pts: list, offsets: dict, geo: dict,
               out_path: str, zoom: int = 17,
               figsize: tuple = (18, 12)):
    try:
        import geopandas as gpd
        import matplotlib.pyplot as plt
        import contextily as ctx
        from shapely.geometry import LineString
    except ImportError as e:
        print(f"Skipping satellite plot (missing dep: {e})")
        return

    fig, ax = plt.subplots(figsize=figsize)
    for robot_id, (dlat, dlon) in offsets.items():
        robot_pts = [(lon + dlon, lat + dlat) for lat, lon in pts]
        gdf = gpd.GeoDataFrame(
            geometry=[LineString(robot_pts)], crs="EPSG:4326"
        ).to_crs(epsg=3857)
        gdf.plot(ax=ax, color=robot_color(robot_id),
                 linewidth=1.6, label=f"robot_{robot_id}", zorder=3)

    xmin, xmax = ax.get_xlim(); ymin, ymax = ax.get_ylim()
    pad_x = (xmax - xmin) * 0.05; pad_y = (ymax - ymin) * 0.07
    ax.set_xlim(xmin - pad_x, xmax + pad_x)
    ax.set_ylim(ymin - pad_y, ymax + pad_y)
    ctx.add_basemap(ax, source="Esri.WorldImagery", zoom=zoom, attribution=False)

    n = len(offsets)
    ncol = 3 if n > 15 else (2 if n > 8 else 1)
    ax.legend(loc="upper right", frameon=True, fontsize=8, ncol=ncol,
              handlelength=2, labelspacing=0.3)
    ax.set_axis_off()
    plt.savefig(out_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  Saved → {out_path}")


# ── CLI ───────────────────────────────────────────────────────────────────────

# Default grid shapes for standard fleet sizes
DEFAULT_GRIDS = {
     1: (1,  1),
     5: (1,  5),
    10: (2,  5),
    25: (5,  5),
    50: (5, 10),
}


def main():
    parser = argparse.ArgumentParser(
        description="Compute seamless boustrophedon fleet offsets from a bag."
    )
    parser.add_argument('--bag',   required=True, help='Path to rosbag2 directory')
    parser.add_argument('--topic', default='/follower/gps/fix',
                        help='NavSatFix topic (default: /follower/gps/fix)')
    parser.add_argument('--n',     type=int, default=10,
                        help='Number of robots (default: 10)')
    parser.add_argument('--rows',  type=int, default=None,
                        help='Number of rows in grid (default: auto from --n)')
    parser.add_argument('--all',   action='store_true',
                        help='Generate tables for N=1,5,10,25,50')
    parser.add_argument('--plot',  action='store_true',
                        help='Also generate satellite map PNG')
    parser.add_argument('--out',   default='.',
                        help='Output directory for PNG files')
    args = parser.parse_args()

    print("Loading trajectory...")
    pts = load_base_trajectory(args.bag, args.topic)

    print("Analysing strip geometry...")
    geo = analyse_trajectory(pts)
    print(f"  strip_dir  (E,N): ({geo['strip_dir'][0]:.4f}, {geo['strip_dir'][1]:.4f})")
    print(f"  cross_dir  (E,N): ({geo['cross_dir'][0]:.4f}, {geo['cross_dir'][1]:.4f})")
    print(f"  strip_len : {geo['strip_len']:.2f} m")
    print(f"  strip_sp  : {geo['strip_sp']:.2f} m")
    print(f"  n_strips  : {geo['n_strips']}")
    print(f"  cell_cross: {geo['cell_cross']:.2f} m")
    print(f"  cell_along: {geo['cell_along']:.2f} m")

    targets = sorted(DEFAULT_GRIDS) if args.all else [args.n]

    for n in targets:
        rows, cols = DEFAULT_GRIDS.get(n, (None, None))
        if args.rows:
            rows = args.rows
        if rows is None or n % rows != 0:
            rows = 1
        offsets = make_offsets(geo, n, rows)
        print_table(offsets, geo, n, rows)
        if args.plot:
            out_png = os.path.join(args.out, f"fleet_{n:02d}robots.png")
            zoom = 16 if n >= 25 else 17
            plot_fleet(pts, offsets, geo, out_png, zoom=zoom)


if __name__ == '__main__':
    main()
