#!/usr/bin/env python3
"""
Statistical significance evaluator.

Consumes a coverage time-series CSV (from ``evaluate_coverage_longitudinal.py``
or ``evaluate_baselines.py``) and emits per-pair significance results so the
paper can claim:

  "heuristic discovers 2.3x more states than stress (Mann–Whitney U,
   p = 0.003, Cliff's delta = 0.78 large)."

The "primary metric" picked off each rep is the *final* per-rep value
(``cumulative_unique_states`` and ``cumulative_bugs`` at the largest
``timestamp_s`` seen for that rep).

Outputs
-------
  <output>/significance_summary.csv     mean ± 95% bootstrap CI per cell
  <output>/significance_pairs.csv       pairwise tests for each (db, wl)
  <output>/significance_report.txt      human-readable summary

Usage
-----
  python scripts/evaluate_significance.py results/longitudinal/coverage_timeseries.csv
  python scripts/evaluate_significance.py results/baselines/baseline_timeseries.csv \
      --output results/baselines/sig --metric bugs
"""

from __future__ import annotations

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from scripts.stats_utils import (
    bootstrap_ci,
    cliffs_delta,
    cohens_d,
    interpret_cliffs,
    mann_whitney_u,
    welch_t_test,
)


def _load_per_rep_finals(
    path: Path, metric_col: str
) -> Dict[Tuple[str, str, str], List[float]]:
    """Read CSV and produce {(db, workload, strategy): [final-per-rep]}."""
    by_rep: Dict[Tuple[str, str, str, int], Tuple[float, float]] = {}
    with path.open("r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = (
                row["db"],
                row["workload"],
                row["strategy"],
                int(row["rep"]),
            )
            ts = float(row["timestamp_s"])
            v = float(row[metric_col])
            prev = by_rep.get(key)
            if prev is None or ts > prev[0]:
                by_rep[key] = (ts, v)
    by_strat: Dict[Tuple[str, str, str], List[float]] = defaultdict(list)
    for (db, wl, strat, _rep), (_ts, v) in by_rep.items():
        by_strat[(db, wl, strat)].append(v)
    return by_strat


def _write_summary(
    by_strat: Dict[Tuple[str, str, str], List[float]],
    path: Path,
    metric: str,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "db", "workload", "strategy", "n", "metric",
            "mean", "ci_lo_95", "ci_hi_95", "min", "max",
        ])
        for (db, wl, strat), vals in sorted(by_strat.items()):
            mean, lo, hi = bootstrap_ci(vals, level=0.95, n_resamples=5000, seed=42)
            w.writerow([
                db, wl, strat, len(vals), metric,
                f"{mean:.4f}", f"{lo:.4f}", f"{hi:.4f}",
                f"{min(vals):.4f}" if vals else "0",
                f"{max(vals):.4f}" if vals else "0",
            ])


def _write_pairs(
    by_strat: Dict[Tuple[str, str, str], List[float]],
    path: Path,
    metric: str,
    *,
    reference_strategy: str = "stress",
) -> None:
    """For each (db, wl), test every other strategy vs the reference."""
    path.parent.mkdir(parents=True, exist_ok=True)
    cells: Dict[Tuple[str, str], Dict[str, List[float]]] = defaultdict(dict)
    for (db, wl, strat), vals in by_strat.items():
        cells[(db, wl)][strat] = vals

    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow([
            "db", "workload", "metric",
            "strategy_a", "strategy_b",
            "n_a", "n_b", "mean_a", "mean_b",
            "mwu_U", "mwu_z", "mwu_p", "mwu_r",
            "welch_t", "welch_df", "welch_p",
            "cohens_d", "cliffs_delta", "cliffs_magnitude",
            "significant_at_0.05",
        ])
        for (db, wl), strat_map in sorted(cells.items()):
            strats = sorted(strat_map.keys())
            for i, sa in enumerate(strats):
                for sb in strats[i + 1:]:
                    a = strat_map[sa]
                    b = strat_map[sb]
                    if not a or not b:
                        continue
                    mean_a = sum(a) / len(a)
                    mean_b = sum(b) / len(b)
                    U, z, p_mwu, r = mann_whitney_u(a, b)
                    t, df, p_w = welch_t_test(a, b)
                    d = cohens_d(a, b)
                    cd = cliffs_delta(a, b)
                    sig = "yes" if min(p_mwu, p_w) < 0.05 else "no"
                    w.writerow([
                        db, wl, metric, sa, sb,
                        len(a), len(b), f"{mean_a:.3f}", f"{mean_b:.3f}",
                        f"{U:.2f}", f"{z:.3f}", f"{p_mwu:.4f}", f"{r:.3f}",
                        f"{t:.3f}", f"{df:.2f}", f"{p_w:.4f}",
                        f"{d:.3f}", f"{cd:.3f}", interpret_cliffs(cd),
                        sig,
                    ])


def _write_text_report(
    by_strat: Dict[Tuple[str, str, str], List[float]],
    path: Path,
    metric: str,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cells: Dict[Tuple[str, str], Dict[str, List[float]]] = defaultdict(dict)
    for (db, wl, strat), vals in by_strat.items():
        cells[(db, wl)][strat] = vals

    lines: List[str] = []
    lines.append("=" * 78)
    lines.append(f"IsoTest Significance Report — metric={metric}")
    lines.append("=" * 78)
    for (db, wl), strat_map in sorted(cells.items()):
        lines.append("")
        lines.append(f"[{db} / {wl}]")
        for strat in sorted(strat_map.keys()):
            v = strat_map[strat]
            mean, lo, hi = bootstrap_ci(v, level=0.95, n_resamples=5000, seed=42)
            lines.append(
                f"  {strat:<22} n={len(v):<3}  "
                f"mean={mean:.2f}  95% CI=[{lo:.2f}, {hi:.2f}]"
            )
        # Highlight the strongest pair vs the weakest mean
        if len(strat_map) >= 2:
            ranked = sorted(
                strat_map.items(),
                key=lambda kv: (sum(kv[1]) / max(len(kv[1]), 1)),
            )
            weak_name, weak_v = ranked[0]
            strong_name, strong_v = ranked[-1]
            if weak_name != strong_name:
                U, z, p, r = mann_whitney_u(strong_v, weak_v)
                d = cohens_d(strong_v, weak_v)
                cd = cliffs_delta(strong_v, weak_v)
                lines.append(
                    f"  -> {strong_name} vs {weak_name}: "
                    f"MWU U={U:.1f} p={p:.4f} | "
                    f"d={d:.2f} | cliffs={cd:.2f} ({interpret_cliffs(cd)})"
                )
    lines.append("")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=str, help="Coverage CSV path")
    parser.add_argument("--metric", default="states",
                        choices=["states", "bugs", "trials"])
    parser.add_argument("--output", type=str, default=None)
    parser.add_argument("--reference", type=str, default="stress")
    args = parser.parse_args()

    metric_col = {
        "states": "cumulative_unique_states",
        "bugs": "cumulative_bugs",
        "trials": "cumulative_trials",
    }[args.metric]

    in_path = Path(args.input)
    out_dir = Path(args.output) if args.output else in_path.parent / "significance"

    by_strat = _load_per_rep_finals(in_path, metric_col)
    if not by_strat:
        print(f"No data found in {in_path}")
        sys.exit(1)

    _write_summary(by_strat, out_dir / "significance_summary.csv", args.metric)
    _write_pairs(by_strat, out_dir / "significance_pairs.csv", args.metric,
                 reference_strategy=args.reference)
    _write_text_report(by_strat, out_dir / "significance_report.txt", args.metric)

    print(f"Wrote: {out_dir}/significance_summary.csv")
    print(f"Wrote: {out_dir}/significance_pairs.csv")
    print(f"Wrote: {out_dir}/significance_report.txt")
    # Print preview
    print()
    print((out_dir / "significance_report.txt").read_text())


if __name__ == "__main__":
    main()
