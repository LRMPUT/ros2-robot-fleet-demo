#!/usr/bin/env python3
"""Extract NavSatFix from an MCAP rosbag2 and visualise the trajectory.

Usage:
    python3 tools/extract_gnss_from_mcap.py --bag bags/rosbag2_2026_04_10-11_01_18/
    python3 tools/extract_gnss_from_mcap.py --bag ... --topic /sensing/ins/imu/nav_sat_fix --out traj.html
"""
from __future__ import annotations

import argparse
import glob
import os
import sys

from mcap.reader import make_reader
from mcap_ros2.decoder import DecoderFactory


def find_mcap(bag_path: str) -> str:
    if bag_path.endswith(".mcap"):
        return bag_path
    matches = glob.glob(os.path.join(bag_path, "*.mcap"))
    if not matches:
        print(f"ERROR: no .mcap file found in {bag_path}", file=sys.stderr)
        sys.exit(1)
    return matches[0]


def read_navsatfix(mcap_path: str, topic: str) -> list[tuple[int, float, float, float]]:
    pts: list[tuple[int, float, float, float]] = []
    with open(mcap_path, "rb") as f:
        reader = make_reader(f, decoder_factories=[DecoderFactory()])
        for schema, channel, message, decoded in reader.iter_decoded_messages(topics=[topic]):
            h = decoded.header
            ts_ns = h.stamp.sec * 1_000_000_000 + h.stamp.nanosec
            pts.append((ts_ns, decoded.latitude, decoded.longitude, decoded.altitude))
    return pts


def write_tsv(pts: list[tuple[int, float, float, float]], path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w") as f:
        f.write("timestamp_ns\tlatitude\tlongitude\taltitude\n")
        for ts, lat, lon, alt in pts:
            f.write(f"{ts}\t{lat:.9f}\t{lon:.9f}\t{alt:.4f}\n")


def make_html_map(pts: list[tuple[int, float, float, float]], out_html: str) -> None:
    import folium

    lats = [p[1] for p in pts]
    lons = [p[2] for p in pts]
    center = (sum(lats) / len(lats), sum(lons) / len(lons))

    m = folium.Map(location=center, zoom_start=17, tiles="OpenStreetMap")

    coords = [(p[1], p[2]) for p in pts]
    folium.PolyLine(coords, color="royalblue", weight=3, opacity=0.9,
                    tooltip="GNSS trajectory").add_to(m)

    folium.CircleMarker(coords[0],  radius=6, color="green",  fill=True, tooltip="Start").add_to(m)
    folium.CircleMarker(coords[-1], radius=6, color="red",    fill=True, tooltip="End").add_to(m)

    duration_s = (pts[-1][0] - pts[0][0]) / 1e9
    info = (
        f"Points: {len(pts)}<br>"
        f"Duration: {duration_s:.1f} s<br>"
        f"Lat range: {min(lats):.6f} – {max(lats):.6f}<br>"
        f"Lon range: {min(lons):.6f} – {max(lons):.6f}"
    )
    folium.Marker(
        coords[0],
        icon=folium.DivIcon(html=f'<div style="font-size:11px;background:white;padding:3px;border:1px solid #aaa">{info}</div>'),
    ).add_to(m)

    m.save(out_html)


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract & visualise GNSS from MCAP bag")
    parser.add_argument("--bag",   required=True, help="Bag directory or .mcap file")
    parser.add_argument("--topic", default="/sensing/ins/imu/nav_sat_fix")
    parser.add_argument("--out",   default="trajectories_mcap/gnss_trajectory.html")
    parser.add_argument("--tsv",   default="trajectories_mcap/gnss.tsv",
                        help="Also write raw TSV (empty string to skip)")
    args = parser.parse_args()

    mcap_path = find_mcap(args.bag)
    print(f"MCAP:  {mcap_path}")
    print(f"Topic: {args.topic}")

    pts = read_navsatfix(mcap_path, args.topic)
    if not pts:
        print("ERROR: no NavSatFix messages decoded.", file=sys.stderr)
        sys.exit(1)

    print(f"Points: {len(pts):,}  duration: {(pts[-1][0]-pts[0][0])/1e9:.1f} s")

    if args.tsv:
        write_tsv(pts, args.tsv)
        print(f"TSV:   {args.tsv}")

    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    make_html_map(pts, args.out)
    print(f"Map:   {args.out}")


if __name__ == "__main__":
    main()
