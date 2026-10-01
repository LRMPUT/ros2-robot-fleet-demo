#!/usr/bin/env python3
"""Plot robot GNSS trajectories from recorded txt files.

Reads robot_N_gnss.txt files (written by record_gnss.py) and produces:
  - trajectories.html   — interactive Folium/Leaflet map
  - trajectories.png    — static satellite map (Esri WorldImagery, 500 dpi)
  - trajectories.pdf    — same as PNG, vector format

Each robot gets a distinct colour. The static map matches the style of the
existing chist-era-aricle-tools plots.

Usage:
    python3 tools/plot_trajectories.py trajectories/
    python3 tools/plot_trajectories.py trajectories/ --out results/
    python3 tools/plot_trajectories.py trajectories/ --min-points 50
"""
from __future__ import annotations

import argparse
import glob
import os
import re
import sys

import pandas as pd

COLORS = [
    "#e41a1c", "#377eb8", "#4daf4a", "#984ea3", "#ff7f00",
    "#a65628", "#f781bf", "#66c2a5", "#fc8d62", "#8da0cb",
    "#e78ac3", "#a6d854", "#ffd92f", "#e5c494", "#b3b3b3",
]

# Folium uses CSS colour names / hex — same list works.
FOLIUM_COLORS_HEX = COLORS


def _color(robot_id: int) -> str:
    return COLORS[(robot_id - 1) % len(COLORS)]


def load_trajectories(in_dir: str, min_points: int) -> dict[int, list[tuple[float, float]]]:
    pattern = os.path.join(in_dir, "robot_*_gnss.txt")
    files = sorted(glob.glob(pattern))
    if not files:
        print(f"ERROR: no robot_*_gnss.txt files found in {in_dir}", file=sys.stderr)
        sys.exit(1)

    trajs: dict[int, list[tuple[float, float]]] = {}
    for path in files:
        m = re.search(r"robot_(\d+)_gnss\.txt$", path)
        if not m:
            continue
        rid = int(m.group(1))
        try:
            df = pd.read_csv(path, sep="\t")
            df = df.dropna(subset=["latitude", "longitude"])
            if len(df) < min_points:
                print(f"  robot_{rid}: only {len(df)} points — skipping (< {min_points})")
                continue
            pts = list(zip(df["latitude"], df["longitude"]))
            trajs[rid] = pts
            print(f"  robot_{rid}: {len(pts):,} points")
        except Exception as e:
            print(f"  robot_{rid}: failed to load ({e})", file=sys.stderr)

    return trajs


def make_folium(trajs: dict[int, list[tuple[float, float]]], out_path: str) -> None:
    import folium

    all_pts = [pt for pts in trajs.values() for pt in pts]
    center_lat = sum(p[0] for p in all_pts) / len(all_pts)
    center_lon = sum(p[1] for p in all_pts) / len(all_pts)

    m = folium.Map(location=[center_lat, center_lon], zoom_start=17)

    for rid, pts in sorted(trajs.items()):
        color = _color(rid)
        folium.PolyLine(
            locations=pts,
            color=color,
            weight=2.5,
            tooltip=f"robot_{rid}",
            popup=f"robot_{rid}  ({len(pts):,} pts)",
        ).add_to(m)
        # Start marker
        folium.CircleMarker(
            location=pts[0],
            radius=5,
            color=color,
            fill=True,
            fill_color=color,
            tooltip=f"robot_{rid} start",
        ).add_to(m)

    lats = [p[0] for p in all_pts]
    lons = [p[1] for p in all_pts]
    m.fit_bounds([[min(lats), min(lons)], [max(lats), max(lons)]])

    m.save(out_path)
    print(f"Saved → {out_path}")


def make_static(trajs: dict[int, list[tuple[float, float]]],
                out_png: str, out_pdf: str, zoom: int) -> None:
    import geopandas as gpd
    import matplotlib.pyplot as plt
    import contextily as ctx
    from shapely.geometry import LineString

    fig, ax = plt.subplots(figsize=(12, 12))

    for rid, pts in sorted(trajs.items()):
        color = _color(rid)
        line = LineString([(lon, lat) for lat, lon in pts])
        gdf = gpd.GeoDataFrame(geometry=[line], crs="EPSG:4326").to_crs(epsg=3857)
        gdf.plot(ax=ax, color=color, linewidth=2.0, label=f"robot_{rid}", zorder=3)

    # Expand view slightly beyond trajectories
    xmin, xmax = ax.get_xlim()
    ymin, ymax = ax.get_ylim()
    pad_x = (xmax - xmin) * 0.08
    pad_y = (ymax - ymin) * 0.08
    ax.set_xlim(xmin - pad_x, xmax + pad_x)
    ax.set_ylim(ymin - pad_y, ymax + pad_y)

    ctx.add_basemap(ax, source="Esri.WorldImagery", zoom=zoom, attribution=False)

    ax.legend(
        loc="upper right",
        frameon=True,
        fontsize=11,
        handlelength=2.5,
        handletextpad=0.7,
        labelspacing=0.4,
        ncol=2 if len(trajs) > 8 else 1,
    )
    ax.set_axis_off()

    plt.savefig(out_png, dpi=500, bbox_inches="tight")
    print(f"Saved → {out_png}")
    plt.savefig(out_pdf, bbox_inches="tight")
    print(f"Saved → {out_pdf}")
    plt.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot robot GNSS trajectories")
    parser.add_argument("input_dir",
                        help="Directory containing robot_N_gnss.txt files")
    parser.add_argument("--out",        default=None,
                        help="Output directory (default: same as input_dir)")
    parser.add_argument("--min-points", type=int, default=10,
                        help="Skip robots with fewer than N points (default: 10)")
    parser.add_argument("--zoom",       type=int, default=18,
                        help="Tile zoom level for static map (default: 18)")
    parser.add_argument("--no-static",  action="store_true",
                        help="Skip static PNG/PDF (faster, no geopandas needed)")
    args = parser.parse_args()

    out_dir = args.out or args.input_dir
    os.makedirs(out_dir, exist_ok=True)

    print(f"Loading trajectories from {args.input_dir}/")
    trajs = load_trajectories(args.input_dir, args.min_points)

    if not trajs:
        print("No valid trajectories found.", file=sys.stderr)
        sys.exit(1)

    print(f"\nPlotting {len(trajs)} robots...")
    make_folium(trajs, os.path.join(out_dir, "trajectories.html"))

    if not args.no_static:
        try:
            make_static(
                trajs,
                os.path.join(out_dir, "trajectories.png"),
                os.path.join(out_dir, "trajectories.pdf"),
                args.zoom,
            )
        except ImportError as e:
            print(f"Static map skipped (missing dependency: {e})")
            print("Install with: pip install geopandas contextily matplotlib")


if __name__ == "__main__":
    main()
