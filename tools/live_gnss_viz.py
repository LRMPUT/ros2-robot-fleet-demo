#!/usr/bin/env python3
"""Live GNSS trajectory visualizer with satellite + OSM background.

Layer order (bottom to top):
  1. Local satellite GeoTIFF (year_2024, Kakolewo, EPSG:2180 → reprojected to 3857)
  2. OSM road/label overlay (semi-transparent, via contextily)
  3. GNSS trail (plasma colormap, fades old→new)
  4. Live position marker (lime dot)

The satellite image is reprojected once at startup and cached as a NumPy array.
The extent is hardcoded for rosbag2_2026_04_10-11_01_18 (Poznań / Kakolewo area).

Usage:
    python3 tools/live_gnss_viz.py
    python3 tools/live_gnss_viz.py --trail 800 --no-sat
"""
from __future__ import annotations

import argparse
import collections
import json
import os
import queue

import contextily as ctx
import matplotlib.animation as animation
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import paho.mqtt.client as mqtt
import pyproj

MQTT_TOPIC = "ros2/robot_1/gnss"

# ── Satellite tile (local GeoTIFF) ────────────────────────────────────────────
SAT_PATH = "/home/maciej/Github/sat_data/rendered_kakolewo/year_2024.tiff"

# ── Hardcoded map extent for rosbag2_2026_04_10 (Kakolewo / Poznań, Poland) ───
_LAT_MIN, _LAT_MAX = 52.225, 52.248
_LON_MIN, _LON_MAX = 16.227, 16.263

_WGS_TO_MERC = pyproj.Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
_MERC_TO_WGS = pyproj.Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)

_X0, _Y0 = _WGS_TO_MERC.transform(_LON_MIN, _LAT_MIN)
_X1, _Y1 = _WGS_TO_MERC.transform(_LON_MAX, _LAT_MAX)


def latlon_to_merc(lats, lons):
    xs, ys = _WGS_TO_MERC.transform(list(lons), list(lats))
    return np.array(xs), np.array(ys)


def _sat_cache_path(out_px: int) -> str:
    return SAT_PATH + f".merc_{out_px}px.npy"


def load_sat_tile(out_px: int = 2750) -> np.ndarray | None:
    """Reproject satellite GeoTIFF to EPSG:3857, crop to display extent.

    Results are cached to <source>.merc_<px>px.npy alongside the GeoTIFF so
    subsequent launches are instant. Cache is invalidated when the source
    file's mtime changes.

    Returns an (H, W, 3) uint8 RGB array, or None on failure.
    """
    if not os.path.exists(SAT_PATH):
        print(f"WARNING: satellite tile not found at {SAT_PATH}")
        return None
    try:
        import json as _json
        import rasterio
        from rasterio.transform import from_bounds
        from rasterio.warp import reproject, Resampling

        cache_npy  = _sat_cache_path(out_px)
        cache_meta = cache_npy + ".meta.json"
        src_mtime  = os.path.getmtime(SAT_PATH)

        # ── try cache hit ─────────────────────────────────────────────────────
        if os.path.exists(cache_npy) and os.path.exists(cache_meta):
            meta = _json.loads(open(cache_meta).read())
            if meta.get("src_mtime") == src_mtime and meta.get("out_px") == out_px:
                print(f"Loading cached satellite tile ({cache_npy})…", flush=True)
                img = np.load(cache_npy)
                print(f"Cache hit — {img.shape[1]}×{img.shape[0]} px  "
                      f"({(_X1-_X0)/img.shape[1]:.2f} m/px)", flush=True)
                return img

        # ── reproject ─────────────────────────────────────────────────────────
        w_m = _X1 - _X0
        h_m = _Y1 - _Y0
        if w_m >= h_m:
            out_w, out_h = out_px, max(1, int(out_px * h_m / w_m))
        else:
            out_h, out_w = out_px, max(1, int(out_px * w_m / h_m))

        print(f"Reprojecting satellite tile to {out_w}×{out_h} px "
              f"({w_m/out_w:.2f} m/px)…", flush=True)

        dst_transform = from_bounds(_X0, _Y0, _X1, _Y1, out_w, out_h)
        rgb = np.zeros((3, out_h, out_w), dtype=np.uint8)

        with rasterio.open(SAT_PATH) as src:
            for band in range(1, 4):
                reproject(
                    source=rasterio.band(src, band),
                    destination=rgb[band - 1],
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=dst_transform,
                    dst_crs="EPSG:3857",
                    resampling=Resampling.bilinear,
                )

        img = np.moveaxis(rgb, 0, -1)  # → (H, W, 3)

        # ── write cache ───────────────────────────────────────────────────────
        try:
            np.save(cache_npy, img)
            open(cache_meta, "w").write(
                _json.dumps({"src_mtime": src_mtime, "out_px": out_px})
            )
            print(f"Satellite tile cached → {cache_npy}", flush=True)
        except OSError as e:
            print(f"WARNING: could not write cache: {e}")

        return img
    except Exception as exc:
        print(f"WARNING: could not load satellite tile: {exc}")
        return None


def make_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Live GNSS viz — satellite + OSM background")
    p.add_argument("--host",   default="localhost")
    p.add_argument("--port",   type=int, default=1883)
    p.add_argument("--trail",  type=int, default=800,
                   help="Max trail points shown (default: 800)")
    p.add_argument("--zoom",   type=int, default=17,
                   help="OSM tile zoom level (default: 17)")
    p.add_argument("--osm-alpha", type=float, default=0.35,
                   help="OSM overlay opacity 0-1 (default: 0.35)")
    p.add_argument("--no-sat", action="store_true",
                   help="Skip satellite layer (OSM only)")
    return p


def main() -> None:
    args = make_parser().parse_args()

    q: queue.Queue = queue.Queue()

    def on_message(_client, _userdata, msg):
        try:
            data = json.loads(msg.payload)
            q.put((float(data["latitude"]), float(data["longitude"])))
        except Exception:
            pass

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
    client.on_message = on_message
    client.connect(args.host, args.port)
    client.subscribe(MQTT_TOPIC)
    client.loop_start()

    lats: collections.deque = collections.deque(maxlen=args.trail)
    lons: collections.deque = collections.deque(maxlen=args.trail)
    total = [0]

    # ── Figure ────────────────────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(11, 9))
    fig.patch.set_facecolor("#111")
    ax.set_xlim(_X0, _X1)
    ax.set_ylim(_Y0, _Y1)

    # ── Layer 1: satellite GeoTIFF ────────────────────────────────────────────
    if not args.no_sat:
        print("Reprojecting satellite tile…", flush=True)
        sat_img = load_sat_tile(out_px=1024)
        if sat_img is not None:
            ax.imshow(
                sat_img,
                extent=[_X0, _X1, _Y0, _Y1],
                origin="upper",
                aspect="auto",
                zorder=0,
            )
            print("Satellite tile loaded.", flush=True)
        else:
            ax.set_facecolor("#1a1a2e")
    else:
        ax.set_facecolor("#1a1a2e")

    # ── Layer 2: OSM roads + labels (semi-transparent) ────────────────────────
    print("Fetching OSM tiles…", flush=True)
    try:
        ctx.add_basemap(
            ax,
            crs="EPSG:3857",
            source=ctx.providers.OpenStreetMap.Mapnik,
            zoom=args.zoom,
            reset_extent=False,
            attribution_size=7,
            alpha=args.osm_alpha,
            zorder=1,
        )
        ax.set_xlim(_X0, _X1)
        ax.set_ylim(_Y0, _Y1)
        print("OSM tiles loaded.", flush=True)
    except Exception as exc:
        print(f"WARNING: could not fetch OSM tiles ({exc}).")

    # ── Axis labels in lat/lon ────────────────────────────────────────────────
    ax.xaxis.set_major_formatter(
        ticker.FuncFormatter(
            lambda x, _: f"{_MERC_TO_WGS.transform(x, _Y0)[0]:.4f}°E"
        )
    )
    ax.yaxis.set_major_formatter(
        ticker.FuncFormatter(
            lambda y, _: f"{_MERC_TO_WGS.transform(_X0, y)[1]:.5f}°N"
        )
    )
    ax.tick_params(axis="both", colors="white", labelsize=8)
    for spine in ax.spines.values():
        spine.set_edgecolor("#555")
    ax.set_xlabel("Longitude", color="white", fontsize=10)
    ax.set_ylabel("Latitude",  color="white", fontsize=10)
    ax.set_title(
        "Live GNSS  —  Kakolewo 2024 satellite + OSM",
        color="white", fontsize=12,
    )

    # ── Layer 3 & 4: GNSS trail + live cursor ────────────────────────────────
    trail_sc = ax.scatter([], [], s=8, c=[], cmap="plasma",
                          vmin=0, vmax=1, alpha=0.95, linewidths=0, zorder=4)
    head_sc  = ax.scatter([], [], s=140, c="lime", zorder=6,
                          marker="o", edgecolors="white", linewidths=1.4)
    status_text = ax.text(
        0.015, 0.975, "Waiting for data…",
        transform=ax.transAxes, color="white", fontsize=9,
        verticalalignment="top", fontfamily="monospace",
        bbox=dict(boxstyle="round,pad=0.35", facecolor="#000000bb", edgecolor="none"),
        zorder=7,
    )

    # ── Animation ─────────────────────────────────────────────────────────────
    def update(_frame):
        new = False
        while not q.empty():
            try:
                lat, lon = q.get_nowait()
                lats.append(lat)
                lons.append(lon)
                total[0] += 1
                new = True
            except queue.Empty:
                break

        if not lats:
            return trail_sc, head_sc, status_text

        if new:
            xs, ys = latlon_to_merc(lats, lons)
            n = len(xs)
            trail_sc.set_offsets(np.column_stack([xs, ys]))
            trail_sc.set_array(np.linspace(0, 1, n))
            head_sc.set_offsets([[xs[-1], ys[-1]]])
            status_text.set_text(
                f"  pts {total[0]}   {lats[-1]:.6f}°N  {lons[-1]:.6f}°E"
            )

        return trail_sc, head_sc, status_text

    ani = animation.FuncAnimation(
        fig, update, interval=200, blit=False, cache_frame_data=False
    )

    print(f"Listening on MQTT {args.host}:{args.port} → {MQTT_TOPIC}")
    print("Close the window or Ctrl+C to stop.")
    try:
        plt.tight_layout()
        plt.show()
    except KeyboardInterrupt:
        pass
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
