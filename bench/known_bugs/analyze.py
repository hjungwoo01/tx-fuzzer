from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


@dataclass
class GroupStats:
    n: int
    detections: int
    detection_rate: float
    median_s: float
    p90_s: float
    mean_s: float
    std_s: float
    median_trials: float
    p90_trials: float
    mean_trials: float
    std_trials: float


@dataclass
class ComparisonStats:
    u_stat: float
    p_value: float
    cliffs_delta: float


def _to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def _percentile(values: Sequence[float], q: float) -> float:
    if not values:
        return float("nan")
    sorted_vals = sorted(values)
    if len(sorted_vals) == 1:
        return float(sorted_vals[0])
    pos = q * (len(sorted_vals) - 1)
    low = int(math.floor(pos))
    high = int(math.ceil(pos))
    if low == high:
        return float(sorted_vals[low])
    frac = pos - low
    return float(sorted_vals[low] * (1.0 - frac) + sorted_vals[high] * frac)


def _safe_mean(values: Sequence[float]) -> float:
    if not values:
        return float("nan")
    return float(statistics.fmean(values))


def _safe_std(values: Sequence[float]) -> float:
    if len(values) < 2:
        return 0.0
    return float(statistics.pstdev(values))


def _format_float(value: float) -> str:
    if math.isinf(value):
        return "inf"
    if math.isnan(value):
        return "nan"
    return f"{value:.4f}"


def _load_rows(results_csv: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with results_csv.open("r", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows.append(dict(row))
    if not rows:
        raise ValueError(f"No rows found in {results_csv}")
    return rows


def _group_rows(rows: Iterable[Dict[str, Any]], bug: str, runner: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for row in rows:
        if str(row.get("bug")) != bug:
            continue
        if str(row.get("runner")) != runner:
            continue
        out.append(row)
    return out


def _times_for_test(rows: Sequence[Dict[str, Any]]) -> Tuple[List[float], List[float], List[float], List[float]]:
    # test_times: +inf for censored runs (no detection within budget)
    # capped_times: censored at budget for finite descriptive summaries/plots
    test_times: List[float] = []
    capped_times: List[float] = []
    test_trials: List[float] = []
    capped_trials: List[float] = []

    for row in rows:
        detected = _to_bool(row.get("detected", False))
        budget = _to_float(row.get("time_budget_s", 0.0), default=0.0)
        max_trials = _to_int(row.get("max_trials", 0), default=0)
        ttf = row.get("time_to_first_s")
        trials = row.get("trials_to_first")

        if detected and ttf not in (None, "", "None"):
            tt = _to_float(ttf, default=budget)
            tr = _to_float(trials, default=max_trials)
            test_times.append(tt)
            capped_times.append(tt)
            test_trials.append(tr)
            capped_trials.append(tr)
        else:
            test_times.append(float("inf"))
            capped_times.append(budget)
            test_trials.append(float("inf"))
            capped_trials.append(float(max_trials))

    return test_times, capped_times, test_trials, capped_trials


def _compute_group_stats(rows: Sequence[Dict[str, Any]]) -> GroupStats:
    detections = sum(1 for row in rows if _to_bool(row.get("detected", False)))
    n = len(rows)
    detection_rate = (detections / n) if n else 0.0

    _, capped_times, _, capped_trials = _times_for_test(rows)
    return GroupStats(
        n=n,
        detections=detections,
        detection_rate=detection_rate,
        median_s=_percentile(capped_times, 0.5),
        p90_s=_percentile(capped_times, 0.9),
        mean_s=_safe_mean(capped_times),
        std_s=_safe_std(capped_times),
        median_trials=_percentile(capped_trials, 0.5),
        p90_trials=_percentile(capped_trials, 0.9),
        mean_trials=_safe_mean(capped_trials),
        std_trials=_safe_std(capped_trials),
    )


def _ranks_with_ties(values: Sequence[Tuple[float, int]]) -> Tuple[List[float], List[int]]:
    # values: [(value, group)], sorted by value
    ranks = [0.0] * len(values)
    tie_sizes: List[int] = []
    i = 0
    while i < len(values):
        j = i + 1
        while j < len(values) and values[j][0] == values[i][0]:
            j += 1
        avg_rank = (i + 1 + j) / 2.0
        for k in range(i, j):
            ranks[k] = avg_rank
        tie_sizes.append(j - i)
        i = j
    return ranks, tie_sizes


def mann_whitney_u_test(x: Sequence[float], y: Sequence[float]) -> ComparisonStats:
    if not x or not y:
        return ComparisonStats(u_stat=float("nan"), p_value=float("nan"), cliffs_delta=float("nan"))

    combined: List[Tuple[float, int]] = []
    combined.extend((float(v), 0) for v in x)
    combined.extend((float(v), 1) for v in y)
    combined.sort(key=lambda item: item[0])

    ranks, tie_sizes = _ranks_with_ties(combined)

    r1 = 0.0
    for idx, (_, group) in enumerate(combined):
        if group == 0:
            r1 += ranks[idx]

    n1 = len(x)
    n2 = len(y)
    u1 = r1 - (n1 * (n1 + 1) / 2.0)
    u2 = (n1 * n2) - u1
    u = min(u1, u2)

    n = n1 + n2
    tie_term = sum((t * t * t - t) for t in tie_sizes)
    # Tie-corrected variance for MWU normal approximation.
    variance = (n1 * n2 / 12.0) * ((n + 1) - (tie_term / (n * (n - 1)))) if n > 1 else 0.0

    if variance <= 0:
        p_value = 1.0
    else:
        mean_u = n1 * n2 / 2.0
        diff = abs(u - mean_u)
        z = (diff - 0.5) / math.sqrt(variance) if diff > 0 else 0.0
        p_value = math.erfc(abs(z) / math.sqrt(2.0))

    wins = 0
    losses = 0
    for xv in x:
        for yv in y:
            if xv < yv:
                wins += 1
            elif xv > yv:
                losses += 1
    cliffs_delta = (wins - losses) / float(n1 * n2)

    return ComparisonStats(u_stat=float(u), p_value=float(p_value), cliffs_delta=float(cliffs_delta))


def _ecdf(values: Sequence[float]) -> Tuple[List[float], List[float]]:
    if not values:
        return [], []
    xs = sorted(float(v) for v in values)
    ys = [(i + 1) / len(xs) for i in range(len(xs))]
    return xs, ys


def _write_plots(
    rows: Sequence[Dict[str, Any]],
    bugs: Sequence[str],
    plot_dir: Path,
) -> List[str]:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except Exception:
        return []

    plot_dir.mkdir(parents=True, exist_ok=True)
    generated: List[str] = []

    for bug in bugs:
        tool_rows = _group_rows(rows, bug, "tool")
        baseline_rows = _group_rows(rows, bug, "baseline")
        _, tool_capped, _, _ = _times_for_test(tool_rows)
        _, base_capped, _, _ = _times_for_test(baseline_rows)

        x_tool, y_tool = _ecdf(tool_capped)
        x_base, y_base = _ecdf(base_capped)
        if not x_tool and not x_base:
            continue

        fig = plt.figure(figsize=(7, 4))
        ax = fig.add_subplot(1, 1, 1)
        if x_tool:
            ax.step(x_tool, y_tool, where="post", label="tool", linewidth=2)
        if x_base:
            ax.step(x_base, y_base, where="post", label="baseline", linewidth=2)
        ax.set_title(f"ECDF Time-to-First Detection ({bug})")
        ax.set_xlabel("time to first detection (seconds, censored at budget)")
        ax.set_ylabel("ECDF")
        ax.grid(True, alpha=0.3)
        ax.legend()

        out = plot_dir / f"{bug}_ecdf.png"
        fig.tight_layout()
        fig.savefig(out, dpi=140)
        plt.close(fig)
        generated.append(str(out))

    return generated


def analyze_results(
    *,
    results_csv: Path,
    summary_md: Path,
    plots_dir: Optional[Path] = None,
) -> Dict[str, Any]:
    rows = _load_rows(results_csv)
    bugs = sorted({str(row.get("bug")) for row in rows})

    report: Dict[str, Any] = {
        "results_csv": str(results_csv),
        "bugs": {},
        "method": {
            "censoring": "For Mann–Whitney U and Cliff's delta, undetected runs are encoded as +infinity.",
            "descriptive": "For descriptive moments and ECDF plots, undetected runs are right-censored at the configured time budget.",
        },
    }

    lines: List[str] = []
    lines.append("# Known Bugs Benchmark Summary")
    lines.append("")
    lines.append("Method notes:")
    lines.append("- Mann–Whitney U and Cliff's delta use +infinity for undetected runs.")
    lines.append("- Median/p90/mean/std below are computed on times censored at the run time budget.")
    lines.append("")

    for bug in bugs:
        tool_rows = _group_rows(rows, bug, "tool")
        baseline_rows = _group_rows(rows, bug, "baseline")
        if not tool_rows or not baseline_rows:
            continue

        tool_stats = _compute_group_stats(tool_rows)
        base_stats = _compute_group_stats(baseline_rows)

        tool_test_times, _, _, _ = _times_for_test(tool_rows)
        base_test_times, _, _, _ = _times_for_test(baseline_rows)
        comparison = mann_whitney_u_test(tool_test_times, base_test_times)

        report["bugs"][bug] = {
            "tool": tool_stats.__dict__,
            "baseline": base_stats.__dict__,
            "comparison": comparison.__dict__,
        }

        lines.append(f"## {bug}")
        lines.append("")
        lines.append("| runner | n | detections | rate | median_s | p90_s | mean_s | std_s | median_trials | p90_trials |")
        lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
        lines.append(
            "| tool | {n} | {d} | {r:.3f} | {med} | {p90} | {mean} | {std} | {mtr} | {p90tr} |".format(
                n=tool_stats.n,
                d=tool_stats.detections,
                r=tool_stats.detection_rate,
                med=_format_float(tool_stats.median_s),
                p90=_format_float(tool_stats.p90_s),
                mean=_format_float(tool_stats.mean_s),
                std=_format_float(tool_stats.std_s),
                mtr=_format_float(tool_stats.median_trials),
                p90tr=_format_float(tool_stats.p90_trials),
            )
        )
        lines.append(
            "| baseline | {n} | {d} | {r:.3f} | {med} | {p90} | {mean} | {std} | {mtr} | {p90tr} |".format(
                n=base_stats.n,
                d=base_stats.detections,
                r=base_stats.detection_rate,
                med=_format_float(base_stats.median_s),
                p90=_format_float(base_stats.p90_s),
                mean=_format_float(base_stats.mean_s),
                std=_format_float(base_stats.std_s),
                mtr=_format_float(base_stats.median_trials),
                p90tr=_format_float(base_stats.p90_trials),
            )
        )
        lines.append("")
        lines.append(
            "- Mann–Whitney U (time-to-first, +inf censoring): U={u:.3f}, p={p:.6f}".format(
                u=comparison.u_stat,
                p=comparison.p_value,
            )
        )
        lines.append(
            "- Cliff's delta (positive favors tool): {d:.4f}".format(d=comparison.cliffs_delta)
        )
        lines.append("")

    if plots_dir is not None:
        generated = _write_plots(rows, bugs, plots_dir)
        report["plots"] = generated
        if generated:
            lines.append("## Plots")
            lines.append("")
            for path in generated:
                lines.append(f"- {path}")
            lines.append("")

    summary_md.parent.mkdir(parents=True, exist_ok=True)
    summary_md.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")

    summary_json = summary_md.with_suffix(".json")
    summary_json.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    return report


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Analyze known-bug benchmark results.")
    parser.add_argument("--results", type=Path, required=True, help="Path to results.csv")
    parser.add_argument("--summary", type=Path, required=True, help="Output markdown summary path")
    parser.add_argument("--plots-dir", type=Path, default=None, help="Optional plot output directory")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    analyze_results(results_csv=args.results, summary_md=args.summary, plots_dir=args.plots_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
