#!/usr/bin/env python3
"""Plot cumulative state coverage over time from coverage_timeseries.csv."""

from __future__ import annotations

import argparse
import csv
import struct
import zlib
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple


def _load_rows(path: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            out.append(
                {
                    "db": row.get("db", ""),
                    "workload": row.get("workload", ""),
                    "strategy": row.get("strategy", ""),
                    "repetition": int(row.get("repetition", "0") or 0),
                    "time_ms": int(float(row.get("time_ms", "0") or 0)),
                    "cumulative_unique_states": int(float(row.get("cumulative_unique_states", "0") or 0)),
                }
            )
    return out


def _value_at_or_before(points: List[Tuple[int, int]], target_t: int) -> int:
    last = 0
    for t, v in points:
        if t > target_t:
            break
        last = v
    return last


def _set_px(buf: bytearray, width: int, height: int, x: int, y: int, rgb: Tuple[int, int, int]) -> None:
    if x < 0 or y < 0 or x >= width or y >= height:
        return
    idx = (y * width + x) * 3
    buf[idx] = rgb[0]
    buf[idx + 1] = rgb[1]
    buf[idx + 2] = rgb[2]


def _draw_line(buf: bytearray, width: int, height: int, x0: int, y0: int, x1: int, y1: int, rgb: Tuple[int, int, int]) -> None:
    dx = abs(x1 - x0)
    sx = 1 if x0 < x1 else -1
    dy = -abs(y1 - y0)
    sy = 1 if y0 < y1 else -1
    err = dx + dy
    x = x0
    y = y0
    while True:
        _set_px(buf, width, height, x, y, rgb)
        if x == x1 and y == y1:
            break
        e2 = 2 * err
        if e2 >= dy:
            err += dy
            x += sx
        if e2 <= dx:
            err += dx
            y += sy


def _png_chunk(tag: bytes, data: bytes) -> bytes:
    crc = zlib.crc32(tag + data) & 0xFFFFFFFF
    return struct.pack(">I", len(data)) + tag + data + struct.pack(">I", crc)


def _write_png(path: Path, width: int, height: int, rgb: bytearray) -> None:
    raw = bytearray()
    row_bytes = width * 3
    for y in range(height):
        raw.append(0)  # filter type 0
        start = y * row_bytes
        raw.extend(rgb[start : start + row_bytes])
    compressed = zlib.compress(bytes(raw), level=9)
    png = bytearray()
    png.extend(b"\x89PNG\r\n\x1a\n")
    ihdr = struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0)  # RGB
    png.extend(_png_chunk(b"IHDR", ihdr))
    png.extend(_png_chunk(b"IDAT", compressed))
    png.extend(_png_chunk(b"IEND", b""))
    path.write_bytes(bytes(png))


def _fallback_plot_png(
    out_path: Path,
    series: Dict[str, List[Tuple[float, float]]],
) -> None:
    width, height = 900, 520
    left, top, right, bottom = 70, 30, 30, 60
    plot_w = width - left - right
    plot_h = height - top - bottom
    buf = bytearray([255] * (width * height * 3))
    black = (30, 30, 30)
    grid = (220, 220, 220)

    # Axes and grid.
    for i in range(6):
        gy = top + int(i * plot_h / 5)
        _draw_line(buf, width, height, left, gy, left + plot_w, gy, grid)
    _draw_line(buf, width, height, left, top, left, top + plot_h, black)
    _draw_line(buf, width, height, left, top + plot_h, left + plot_w, top + plot_h, black)

    all_x = [x for pts in series.values() for x, _ in pts]
    all_y = [y for pts in series.values() for _, y in pts]
    x_max = max(all_x) if all_x else 1.0
    y_max = max(all_y) if all_y else 1.0
    x_max = max(1.0, x_max)
    y_max = max(1.0, y_max)

    palette = [
        (33, 113, 181),
        (49, 163, 84),
        (217, 95, 2),
        (117, 107, 177),
        (214, 39, 40),
    ]
    for idx, (_name, pts) in enumerate(sorted(series.items())):
        color = palette[idx % len(palette)]
        if len(pts) < 2:
            continue
        mapped: List[Tuple[int, int]] = []
        for x, y in pts:
            px = left + int((x / x_max) * plot_w)
            py = top + plot_h - int((y / y_max) * plot_h)
            mapped.append((px, py))
        for i in range(1, len(mapped)):
            _draw_line(
                buf,
                width,
                height,
                mapped[i - 1][0],
                mapped[i - 1][1],
                mapped[i][0],
                mapped[i][1],
                color,
            )
    _write_png(out_path, width, height, buf)


def plot_coverage_timeseries(input_csv: Path, output_dir: Path) -> List[Path]:
    try:
        import matplotlib.pyplot as plt
        has_matplotlib = True
    except ImportError:
        has_matplotlib = False

    rows = _load_rows(input_csv)
    if not rows:
        return []

    grouped: Dict[Tuple[str, str, str, int], List[Tuple[int, int]]] = defaultdict(list)
    for row in rows:
        key = (row["db"], row["workload"], row["strategy"], row["repetition"])
        grouped[key].append((row["time_ms"], row["cumulative_unique_states"]))

    for key in list(grouped.keys()):
        grouped[key].sort(key=lambda x: x[0])

    by_dw: Dict[Tuple[str, str], Dict[str, Dict[int, List[Tuple[int, int]]]]] = defaultdict(lambda: defaultdict(dict))
    for (db, workload, strategy, rep), pts in grouped.items():
        by_dw[(db, workload)][strategy][rep] = pts

    output_dir.mkdir(parents=True, exist_ok=True)
    generated: List[Path] = []
    for (db, workload), strat_map in sorted(by_dw.items()):
        series: Dict[str, List[Tuple[float, float]]] = {}
        for strategy, rep_map in sorted(strat_map.items()):
            all_ts = sorted({t for pts in rep_map.values() for t, _ in pts})
            if not all_ts:
                continue
            y_vals: List[float] = []
            for t in all_ts:
                rep_vals = [_value_at_or_before(pts, t) for pts in rep_map.values()]
                y_vals.append(sum(rep_vals) / max(1, len(rep_vals)))
            series[strategy] = [(t / 1000.0, y) for t, y in zip(all_ts, y_vals)]

        out_path = output_dir / f"coverage_{db}_{workload}.png"
        if has_matplotlib:
            plt.figure(figsize=(8, 5))
            for strategy, pts in sorted(series.items()):
                plt.plot([x for x, _ in pts], [y for _, y in pts], label=strategy, linewidth=2.0)
            plt.title(f"{db} - {workload} coverage growth")
            plt.xlabel("Time (seconds)")
            plt.ylabel("Cumulative unique canonical states")
            plt.grid(alpha=0.25, linestyle="--")
            plt.legend()
            plt.tight_layout()
            plt.savefig(out_path, dpi=140)
            plt.close()
        else:
            _fallback_plot_png(out_path, series)
        generated.append(out_path)
    return generated


def main() -> int:
    parser = argparse.ArgumentParser(description="Plot multi-DB coverage growth curves.")
    parser.add_argument("--input", required=True, help="Path to coverage_timeseries.csv")
    parser.add_argument("--output-dir", required=True, help="Directory for generated PNGs")
    args = parser.parse_args()

    input_csv = Path(args.input)
    output_dir = Path(args.output_dir)
    generated = plot_coverage_timeseries(input_csv, output_dir)
    if not generated:
        print("No plots generated (no rows).")
        return 1
    for path in generated:
        print(f"wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
