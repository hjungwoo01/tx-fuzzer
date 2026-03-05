"""
Generate cumulative unique-state discovery charts for two fuzzing campaigns.

Default inputs:
  - Baseline: corpus_baseline_v4/fuzz/postgres/run_2026-02-28T13-31-47Z
  - Smart Fuzzer: corpus_fuzzer_v3/fuzz/postgres/run_2026-02-21T16-08-32Z

Output:
  - discovery_velocity.png (time mode by default)
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple

import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator


def parse_iteration(value) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        s = value.strip()
        if s.isdigit():
            return int(s)
    return None


def parse_timestamp(value) -> datetime | None:
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = f"{s[:-1]}+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def load_discovery_data(run_dir: Path) -> Tuple[List[int], List[datetime]]:
    iterations: List[int] = []
    timestamps: List[datetime] = []
    files = sorted(run_dir.glob("state_*.json"))
    for path in files:
        try:
            with path.open("r", encoding="utf-8") as f:
                record = json.load(f)
        except Exception:
            continue
        if not isinstance(record, dict):
            continue
        it = parse_iteration(record.get("iteration"))
        if it is not None and it >= 0:
            iterations.append(it)
        ts = parse_timestamp(record.get("timestamp"))
        if ts is not None:
            timestamps.append(ts)
    return iterations, timestamps


def build_cumulative_curve(iterations: List[int], max_iterations: int) -> Tuple[List[int], List[int]]:
    hit_count = Counter()
    for it in iterations:
        if 0 <= it <= max_iterations:
            hit_count[it] += 1

    xs = list(range(0, max_iterations + 1))
    ys: List[int] = []
    total = 0
    for x in xs:
        total += hit_count.get(x, 0)
        ys.append(total)
    return xs, ys


def to_elapsed_minutes(timestamps: List[datetime]) -> List[float]:
    if not timestamps:
        return []
    start = min(timestamps)
    values = [max(0.0, (ts - start).total_seconds() / 60.0) for ts in timestamps]
    values.sort()
    return values


def build_cumulative_time_curve(elapsed_minutes: List[float], max_minutes: float) -> Tuple[List[float], List[int]]:
    valid = [v for v in elapsed_minutes if 0.0 <= v <= max_minutes]
    valid.sort()

    if not valid:
        return [0.0, max_minutes], [0, 0]

    xs = [0.0] + valid
    ys = [0] + list(range(1, len(valid) + 1))
    if xs[-1] < max_minutes:
        xs.append(max_minutes)
        ys.append(ys[-1])
    return xs, ys


def discovery_rate(num_states: int, iterations: int) -> float:
    if iterations <= 0:
        return 0.0
    return 100.0 * num_states / float(iterations)


def derived_output(path: Path, suffix: str) -> Path:
    return path.with_name(f"{path.stem}_{suffix}{path.suffix}")


def plot_iteration_chart(
    baseline_iters: List[int],
    fuzzer_iters: List[int],
    max_iterations: int,
    output: Path,
) -> Tuple[int, int]:
    bx, by = build_cumulative_curve(baseline_iters, max_iterations)
    fx, fy = build_cumulative_curve(fuzzer_iters, max_iterations)

    baseline_final = by[-1]
    fuzzer_final = fy[-1]

    fig, ax = plt.subplots(figsize=(12, 6.5))
    ax.step(
        bx,
        by,
        where="post",
        linewidth=2.4,
        color="#1f77b4",
        label=f"Baseline (Natural) — {baseline_final} states",
    )
    ax.step(
        fx,
        fy,
        where="post",
        linewidth=2.4,
        color="#d62728",
        label=f"Smart Fuzzer (Adversarial) — {fuzzer_final} states",
    )

    ax.scatter([max_iterations], [baseline_final], color="#1f77b4", s=36, zorder=5)
    ax.scatter([max_iterations], [fuzzer_final], color="#d62728", s=36, zorder=5)

    ax.annotate(
        f"Baseline={baseline_final}",
        xy=(max_iterations, baseline_final),
        xytext=(-160, 18),
        textcoords="offset points",
        fontsize=10,
        color="#1f77b4",
        arrowprops=dict(arrowstyle="->", color="#1f77b4", lw=1.0),
    )
    ax.annotate(
        f"Fuzzer={fuzzer_final}",
        xy=(max_iterations, fuzzer_final),
        xytext=(-160, -25),
        textcoords="offset points",
        fontsize=10,
        color="#d62728",
        arrowprops=dict(arrowstyle="->", color="#d62728", lw=1.0),
    )

    ax.set_title("Cumulative Unique-State Discovery (PostgreSQL Blind Overwrite)", fontsize=13)
    ax.set_xlabel("Iteration")
    ax.set_ylabel("Cumulative Unique States Found")
    ax.set_xlim(0, max_iterations)
    ax.set_ylim(0, max(fuzzer_final, baseline_final) * 1.08 + 1)
    ax.set_xticks(range(0, max_iterations + 1, 500))
    ax.grid(True, linestyle="--", alpha=0.35)
    ax.legend(loc="upper left", frameon=True)

    fig.tight_layout()
    fig.savefig(output, dpi=160)
    plt.close(fig)
    return baseline_final, fuzzer_final


def plot_time_chart(
    baseline_timestamps: List[datetime],
    fuzzer_timestamps: List[datetime],
    max_minutes: float | None,
    output: Path,
) -> Tuple[int, int, float]:
    baseline_minutes = to_elapsed_minutes(baseline_timestamps)
    fuzzer_minutes = to_elapsed_minutes(fuzzer_timestamps)

    auto_max = 1.0
    if baseline_minutes:
        auto_max = max(auto_max, baseline_minutes[-1])
    if fuzzer_minutes:
        auto_max = max(auto_max, fuzzer_minutes[-1])

    x_max = auto_max if max_minutes is None else max_minutes
    if x_max <= 0:
        x_max = auto_max

    bx, by = build_cumulative_time_curve(baseline_minutes, x_max)
    fx, fy = build_cumulative_time_curve(fuzzer_minutes, x_max)

    baseline_final = by[-1]
    fuzzer_final = fy[-1]

    fig, ax = plt.subplots(figsize=(12, 6.5))
    ax.step(
        bx,
        by,
        where="post",
        linewidth=2.4,
        color="#1f77b4",
        label=f"Baseline (Natural) — {baseline_final} states",
    )
    ax.step(
        fx,
        fy,
        where="post",
        linewidth=2.4,
        color="#d62728",
        label=f"Smart Fuzzer (Adversarial) — {fuzzer_final} states",
    )

    ax.scatter([x_max], [baseline_final], color="#1f77b4", s=36, zorder=5)
    ax.scatter([x_max], [fuzzer_final], color="#d62728", s=36, zorder=5)

    ax.annotate(
        f"Baseline={baseline_final}",
        xy=(x_max, baseline_final),
        xytext=(-160, 18),
        textcoords="offset points",
        fontsize=10,
        color="#1f77b4",
        arrowprops=dict(arrowstyle="->", color="#1f77b4", lw=1.0),
    )
    ax.annotate(
        f"Fuzzer={fuzzer_final}",
        xy=(x_max, fuzzer_final),
        xytext=(-160, -25),
        textcoords="offset points",
        fontsize=10,
        color="#d62728",
        arrowprops=dict(arrowstyle="->", color="#d62728", lw=1.0),
    )

    ax.set_title("Cumulative Unique-State Discovery (PostgreSQL Blind Overwrite)", fontsize=13)
    ax.set_xlabel("Elapsed Time (minutes)")
    ax.set_ylabel("Cumulative Unique States Found")
    ax.set_xlim(0, x_max)
    ax.set_ylim(0, max(fuzzer_final, baseline_final) * 1.08 + 1)
    ax.xaxis.set_major_locator(MaxNLocator(nbins=12))
    ax.grid(True, linestyle="--", alpha=0.35)
    ax.legend(loc="upper left", frameon=True)

    fig.tight_layout()
    fig.savefig(output, dpi=160)
    plt.close(fig)
    return baseline_final, fuzzer_final, x_max


def main() -> None:
    parser = argparse.ArgumentParser(description="Plot cumulative unique-state discovery curves.")
    parser.add_argument(
        "--baseline-dir",
        type=Path,
        default=Path("corpus_baseline_v4/fuzz/postgres/run_2026-02-28T13-31-47Z"),
        help="Path to baseline run directory containing state_*.json",
    )
    parser.add_argument(
        "--fuzzer-dir",
        type=Path,
        default=Path("corpus_fuzzer_v3/fuzz/postgres/run_2026-02-21T16-08-32Z"),
        help="Path to smart fuzzer run directory containing state_*.json",
    )
    parser.add_argument(
        "--max-iterations",
        type=int,
        default=5000,
        help="Iteration x-axis upper bound (inclusive).",
    )
    parser.add_argument(
        "--max-minutes",
        type=float,
        default=None,
        help="Time x-axis upper bound in minutes (defaults to max elapsed time across runs).",
    )
    parser.add_argument(
        "--x-axis",
        choices=["time", "iteration", "both"],
        default="time",
        help="Which x-axis mode to plot.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("discovery_velocity.png"),
        help="Output plot path.",
    )
    args = parser.parse_args()

    baseline_iters, baseline_timestamps = load_discovery_data(args.baseline_dir)
    fuzzer_iters, fuzzer_timestamps = load_discovery_data(args.fuzzer_dir)

    if args.x_axis in {"iteration", "both"}:
        out = args.output if args.x_axis == "iteration" else derived_output(args.output, "iteration")
        baseline_final, fuzzer_final = plot_iteration_chart(
            baseline_iters=baseline_iters,
            fuzzer_iters=fuzzer_iters,
            max_iterations=args.max_iterations,
            output=out,
        )
        b_rate = discovery_rate(baseline_final, args.max_iterations)
        f_rate = discovery_rate(fuzzer_final, args.max_iterations)
        print(f"Saved: {out}")
        print(f"Baseline states: {baseline_final} ({b_rate:.2f}% of {args.max_iterations})")
        print(f"Fuzzer states:   {fuzzer_final} ({f_rate:.2f}% of {args.max_iterations})")

    if args.x_axis in {"time", "both"}:
        out = args.output if args.x_axis == "time" else derived_output(args.output, "time")
        baseline_final, fuzzer_final, x_max = plot_time_chart(
            baseline_timestamps=baseline_timestamps,
            fuzzer_timestamps=fuzzer_timestamps,
            max_minutes=args.max_minutes,
            output=out,
        )
        print(f"Saved: {out}")
        print(f"Baseline states: {baseline_final}")
        print(f"Fuzzer states:   {fuzzer_final}")
        print(f"Time window:     {x_max:.2f} minutes")


if __name__ == "__main__":
    main()
