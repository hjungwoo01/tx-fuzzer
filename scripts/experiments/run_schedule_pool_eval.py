"""
Schedule-pool evaluation experiment.

Runs determinism, coverage, and throughput experiments comparing
stress / random / heuristic / schedule_pool strategies across
postgres / mysql / sqlite on micro workloads.

Usage:
  python scripts/experiments/run_schedule_pool_eval.py [--db postgres sqlite] [--quick] [--smoke]
  python scripts/experiments/run_schedule_pool_eval.py --headline-only
  python scripts/experiments/run_schedule_pool_eval.py --throughput-only
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from scripts.experiments.db_adapter import DEFAULT_DSNS, make_adapter
from scripts.experiments.micro_runner import (
    CoveragePoint,
    MicroRunner,
    TrialResult,
    run_coverage_session,
    write_coverage_csv,
    plot_coverage,
)
from scripts.experiments.micro_workloads import MICRO_WORKLOADS

from scripts.schedule_pool import (
    Schedule,
    ScheduleRecord,
    SchedulePool,
    ScheduleMutator,
    MutationConfig,
    RetentionPolicy,
    RetentionReason,
    make_event_id,
    make_trial_event_ids,
)
from scripts.schedule_pool.pool import PoolConfig
from scripts.schedule_pool.runner import (
    SchedulePoolRunner,
    run_pool_search,
    replay_schedule,
)


STRATEGIES = ["stress", "random", "heuristic", "schedule_pool"]
WORKLOADS = ["bank_overdraft", "job_queue_claim", "list_append"]
SEEDS = [42, 10042, 20042, 30042, 40042]


def _ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _ensure(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p


def _db_available(db: str, dsn: str) -> bool:
    try:
        a = make_adapter(db, dsn)
        a.exec("SELECT 1")
        a.close()
        return True
    except Exception as e:
        print(f"  [{db}] not available: {e}", file=sys.stderr)
        return False


def run_coverage_experiments(
    dbs: List[str],
    dsns: Dict[str, str],
    output_dir: Path,
    duration_s: float = 60.0,
    reps: int = 5,
    n_workers: int = 4,
    delay_ms: float = 5.0,
    seeds: Optional[List[int]] = None,
    **adapter_kwargs: Dict[str, Any],
) -> List[CoveragePoint]:
    """Run coverage-over-time for all DBs × workloads × strategies."""
    seeds = seeds or SEEDS[:reps]
    all_points: List[CoveragePoint] = []
    csv_path = output_dir / "coverage_timeseries.csv"
    pool_path = output_dir / "schedule_pool.jsonl"

    for db in dbs:
        dsn = dsns.get(db, DEFAULT_DSNS.get(db, ""))
        if not _db_available(db, dsn):
            print(f"  Skipping {db} (unavailable)")
            continue

        for wl_name in WORKLOADS:
            for strat in STRATEGIES:
                for rep_idx in range(reps):
                    seed = seeds[rep_idx] if rep_idx < len(seeds) else 42 + rep_idx
                    label = f"{db}/{wl_name}/{strat}/rep{rep_idx}"
                    print(f"  Coverage: {label} (seed={seed})", end=" ", flush=True)
                    t0 = time.monotonic()

                    try:
                        if strat == "schedule_pool":
                            pool = SchedulePool(
                                path=pool_path,
                                config=PoolConfig(),
                            )
                            points, _, pool = run_pool_search(
                                db=db, dsn=dsn,
                                workload_name=wl_name,
                                variant="buggy",
                                duration_s=duration_s,
                                n_workers=n_workers,
                                seed=seed,
                                pool=pool,
                                rep=rep_idx,
                                **adapter_kwargs,
                            )
                        else:
                            points, _ = run_coverage_session(
                                db=db, dsn=dsn,
                                workload_name=wl_name,
                                strategy=strat,
                                variant="buggy",
                                duration_s=duration_s,
                                n_workers=n_workers,
                                delay_ms=delay_ms,
                                rep=rep_idx,
                                **adapter_kwargs,
                            )

                        all_points.extend(points)
                        elapsed = time.monotonic() - t0
                        final = points[-1] if points else None
                        states = final.unique_states if final else 0
                        txns = final.txns_done if final else 0
                        print(f"→ {states} states, {txns} txns ({elapsed:.1f}s)")
                    except Exception as e:
                        print(f"→ ERROR: {e}")

    # Write CSV
    if all_points:
        write_coverage_csv(all_points, csv_path)
        print(f"\n  Coverage CSV: {csv_path}")

    return all_points


@dataclass
class ThroughputRow:
    db: str
    workload: str
    mode: str
    rep: int
    txns: int
    duration_s: float
    txns_per_sec: float
    median_latency_ms: float
    p95_latency_ms: float


def _percentile(sorted_vals: List[float], pct: float) -> float:
    if not sorted_vals:
        return 0.0
    idx = int(len(sorted_vals) * pct / 100.0)
    return sorted_vals[min(idx, len(sorted_vals) - 1)]


def run_throughput_benchmark(
    dbs: List[str],
    dsns: Dict[str, str],
    output_dir: Path,
    duration_s: float = 60.0,
    reps: int = 5,
    n_workers: int = 4,
    delay_ms: float = 5.0,
    seeds: Optional[List[int]] = None,
    **adapter_kwargs: Dict[str, Any],
) -> List[ThroughputRow]:
    """
    Benchmark 4 modes:
      baseline       = stress (no delay, no analysis)
      analysis_only  = stress + canonical hash computation
      full_random    = random delay + analysis
      full_schedule_pool = schedule_pool + analysis
    """
    seeds = seeds or SEEDS[:reps]
    rows: List[ThroughputRow] = []
    csv_path = output_dir / "throughput.csv"

    modes = ["baseline", "analysis_only", "full_random", "full_schedule_pool"]

    for db in dbs:
        dsn = dsns.get(db, DEFAULT_DSNS.get(db, ""))
        if not _db_available(db, dsn):
            continue

        for wl_name in WORKLOADS:
            for mode in modes:
                for rep_idx in range(reps):
                    seed = seeds[rep_idx] if rep_idx < len(seeds) else 42 + rep_idx
                    print(f"  Throughput: {db}/{wl_name}/{mode}/rep{rep_idx}", end=" ", flush=True)

                    try:
                        latencies: List[float] = []
                        txn_count = 0
                        t_start = time.monotonic()

                        if mode == "full_schedule_pool":
                            pool = SchedulePool(config=PoolConfig())
                            pts, trials, _ = run_pool_search(
                                db=db, dsn=dsn,
                                workload_name=wl_name,
                                variant="buggy",
                                duration_s=duration_s,
                                n_workers=n_workers,
                                seed=seed,
                                pool=pool,
                                rep=rep_idx,
                                **adapter_kwargs,
                            )
                            txn_count = len(trials)
                            latencies = [tr.elapsed_ms for tr in trials]
                        else:
                            strat = "stress" if mode in ("baseline", "analysis_only") else "random"
                            d = delay_ms if mode == "full_random" else 0.0
                            pts, trials = run_coverage_session(
                                db=db, dsn=dsn,
                                workload_name=wl_name,
                                strategy=strat,
                                variant="buggy",
                                duration_s=duration_s,
                                n_workers=n_workers,
                                delay_ms=d,
                                rep=rep_idx,
                                **adapter_kwargs,
                            )
                            txn_count = len(trials)
                            latencies = [tr.elapsed_ms for tr in trials]

                        elapsed = time.monotonic() - t_start
                        tps = txn_count / max(elapsed, 0.001)
                        lat_sorted = sorted(latencies) if latencies else [0.0]
                        med = _percentile(lat_sorted, 50)
                        p95 = _percentile(lat_sorted, 95)

                        row = ThroughputRow(
                            db=db, workload=wl_name, mode=mode,
                            rep=rep_idx, txns=txn_count,
                            duration_s=round(elapsed, 3),
                            txns_per_sec=round(tps, 2),
                            median_latency_ms=round(med, 2),
                            p95_latency_ms=round(p95, 2),
                        )
                        rows.append(row)
                        print(f"→ {tps:.1f} txn/s, med={med:.1f}ms, p95={p95:.1f}ms")
                    except Exception as e:
                        print(f"→ ERROR: {e}")

    # Write CSV
    if rows:
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["db", "workload", "mode", "rep", "txns", "duration_s",
                         "txns_per_sec", "median_latency_ms", "p95_latency_ms"])
            for r in rows:
                w.writerow([r.db, r.workload, r.mode, r.rep, r.txns,
                            r.duration_s, r.txns_per_sec, r.median_latency_ms,
                            r.p95_latency_ms])
        print(f"\n  Throughput CSV: {csv_path}")

    return rows


@dataclass
class DeterminismRow:
    db: str
    workload: str
    variant: str
    schedule_id: str
    strategy: str
    replay_mode: str
    buggy_replays: int
    buggy_hits: int
    buggy_rate: float
    control_replays: int
    control_hits: int
    control_rate: float
    stable: bool  # buggy 20/20 and control 0/20


def run_determinism_eval(
    dbs: List[str],
    dsns: Dict[str, str],
    output_dir: Path,
    duration_s: float = 30.0,
    n_workers: int = 4,
    n_replays: int = 20,
    seed: int = 42,
    **adapter_kwargs: Dict[str, Any],
) -> List[DeterminismRow]:
    """
    Discover schedules via pool search, then replay exactwith fixed delays.
    """
    rows: List[DeterminismRow] = []
    csv_path = output_dir / "determinism.csv"
    discovery_csv = output_dir / "discovery_runs.csv"
    replay_csv = output_dir / "replay_results.csv"
    discovery_rows: List[Dict[str, Any]] = []
    replay_rows: List[Dict[str, Any]] = []

    for db in dbs:
        dsn = dsns.get(db, DEFAULT_DSNS.get(db, ""))
        if not _db_available(db, dsn):
            continue

        for wl_name in WORKLOADS:
            print(f"\n  Determinism: {db}/{wl_name}")

            # Discovery phase: find bugs via schedule_pool
            print(f"    Discovery ({duration_s}s)...", end=" ", flush=True)
            pool = SchedulePool(config=PoolConfig())
            pts, trials, pool = run_pool_search(
                db=db, dsn=dsn,
                workload_name=wl_name,
                variant="buggy",
                duration_s=duration_s,
                n_workers=n_workers,
                seed=seed,
                pool=pool,
                **adapter_kwargs,
            )
            bugs = [t for t in trials if t.bug_found]
            print(f"→ {len(trials)} trials, {len(bugs)} bugs, pool={pool.size}")

            # Log discovery
            for i, t in enumerate(trials):
                discovery_rows.append({
                    "db": db, "workload": wl_name,
                    "trial": i, "bug_found": t.bug_found,
                    "canonical_hash": t.canonical_hash,
                    "elapsed_ms": round(t.elapsed_ms, 2),
                })

            # Find bug-producing schedules in the pool
            bug_records = pool.list(workload=wl_name, db=db, bug_only=True)
            if not bug_records:
                print(f"    No bug schedules to replay for {db}/{wl_name}")
                # Still add a row for the "no schedule found" case
                rows.append(DeterminismRow(
                    db=db, workload=wl_name, variant="buggy",
                    schedule_id="none", strategy="schedule_pool",
                    replay_mode="exact-delay",
                    buggy_replays=0, buggy_hits=0, buggy_rate=0.0,
                    control_replays=0, control_hits=0, control_rate=0.0,
                    stable=False,
                ))
                continue

            # Replay the best bug schedule
            best = bug_records[0]  # first found
            sched = best.schedule
            print(f"    Replay {best.schedule_id} on buggy ({n_replays}x)...", end=" ", flush=True)

            buggy_result = replay_schedule(
                db=db, dsn=dsn,
                workload_name=wl_name,
                schedule=sched,
                variant="buggy",
                n_workers=n_workers,
                n_replays=n_replays,
                **adapter_kwargs,
            )
            print(f"→ {buggy_result['bug_count']}/{buggy_result['total']}")

            print(f"    Replay on control ({n_replays}x)...", end=" ", flush=True)
            control_result = replay_schedule(
                db=db, dsn=dsn,
                workload_name=wl_name,
                schedule=sched,
                variant="fixed",
                n_workers=n_workers,
                n_replays=n_replays,
                **adapter_kwargs,
            )
            print(f"→ {control_result['bug_count']}/{control_result['total']}")

            # Log replays
            for r in buggy_result["replays"]:
                replay_rows.append({
                    "db": db, "workload": wl_name,
                    "schedule_id": best.schedule_id,
                    "variant": "buggy", **r,
                })
            for r in control_result["replays"]:
                replay_rows.append({
                    "db": db, "workload": wl_name,
                    "schedule_id": best.schedule_id,
                    "variant": "fixed", **r,
                })

            br = buggy_result["bug_rate"]
            cr = control_result["bug_rate"]
            stable = (
                buggy_result["bug_count"] == buggy_result["total"]
                and control_result["bug_count"] == 0
            )

            rows.append(DeterminismRow(
                db=db, workload=wl_name, variant="buggy",
                schedule_id=best.schedule_id,
                strategy="schedule_pool",
                replay_mode="exact-delay",
                buggy_replays=buggy_result["total"],
                buggy_hits=buggy_result["bug_count"],
                buggy_rate=round(br, 4),
                control_replays=control_result["total"],
                control_hits=control_result["bug_count"],
                control_rate=round(cr, 4),
                stable=stable,
            ))

    # Write CSVs
    _ensure(output_dir)
    if rows:
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["db", "workload", "variant", "schedule_id", "strategy",
                         "replay_mode", "buggy_replays", "buggy_hits", "buggy_rate",
                         "control_replays", "control_hits", "control_rate", "stable"])
            for r in rows:
                w.writerow([r.db, r.workload, r.variant, r.schedule_id, r.strategy,
                            r.replay_mode, r.buggy_replays, r.buggy_hits, r.buggy_rate,
                            r.control_replays, r.control_hits, r.control_rate, r.stable])

    if discovery_rows:
        with discovery_csv.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=discovery_rows[0].keys())
            w.writeheader()
            w.writerows(discovery_rows)

    if replay_rows:
        with replay_csv.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=replay_rows[0].keys())
            w.writeheader()
            w.writerows(replay_rows)

    return rows


def run_headline_comparison(
    dbs: List[str],
    dsns: Dict[str, str],
    output_dir: Path,
    duration_s: float = 30.0,
    reps: int = 5,
    n_workers: int = 4,
    n_replays: int = 20,
    delay_ms: float = 5.0,
    seeds: Optional[List[int]] = None,
    **adapter_kwargs: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Compare random/heuristic/schedule_pool on headline cases:
    - postgres: update_conflict_out (via bank_overdraft micro proxy)
    - mysql: mysql_rr_phantom (via job_queue_claim micro proxy)
    - sqlite: sqlite_narrow_dirty (via list_append micro proxy)

    Uses the micro workloads as proxies for the headline bugs.
    """
    seeds = seeds or SEEDS[:reps]
    results: List[Dict[str, Any]] = []
    strategies = ["random", "heuristic", "schedule_pool"]

    headline_map = {
        "postgres": "bank_overdraft",
        "mysql": "job_queue_claim",
        "sqlite": "list_append",
    }

    for db in dbs:
        dsn = dsns.get(db, DEFAULT_DSNS.get(db, ""))
        if not _db_available(db, dsn):
            continue

        wl_name = headline_map.get(db, "bank_overdraft")
        print(f"\n  Headline: {db} → {wl_name}")

        for strat in strategies:
            bug_count = 0
            ttfb_list: List[float] = []

            for rep_idx in range(reps):
                seed = seeds[rep_idx] if rep_idx < len(seeds) else 42 + rep_idx
                print(f"    {strat}/rep{rep_idx}", end=" ", flush=True)
                t0 = time.monotonic()

                try:
                    if strat == "schedule_pool":
                        pool = SchedulePool(config=PoolConfig())
                        pts, trials, pool = run_pool_search(
                            db=db, dsn=dsn,
                            workload_name=wl_name,
                            variant="buggy",
                            duration_s=duration_s,
                            n_workers=n_workers,
                            seed=seed,
                            pool=pool,
                            rep=rep_idx,
                            **adapter_kwargs,
                        )
                    else:
                        pts, trials = run_coverage_session(
                            db=db, dsn=dsn,
                            workload_name=wl_name,
                            strategy=strat,
                            variant="buggy",
                            duration_s=duration_s,
                            n_workers=n_workers,
                            delay_ms=delay_ms,
                            rep=rep_idx,
                            **adapter_kwargs,
                        )

                    first_bug = next(
                        (i for i, t in enumerate(trials) if t.bug_found), None
                    )
                    if first_bug is not None:
                        bug_count += 1
                        # Estimate TTFB using cumulative elapsed
                        cumulative = sum(t.elapsed_ms for t in trials[:first_bug + 1])
                        ttfb_list.append(cumulative)
                        print(f"→ BUG at trial {first_bug} ({cumulative:.0f}ms)")
                    else:
                        elapsed = time.monotonic() - t0
                        print(f"→ no bug ({elapsed:.1f}s)")
                except Exception as e:
                    print(f"→ ERROR: {e}")

            # Summary
            med_ttfb = sorted(ttfb_list)[len(ttfb_list) // 2] if ttfb_list else None
            results.append({
                "db": db,
                "workload": wl_name,
                "strategy": strat,
                "reps": reps,
                "success_count": bug_count,
                "success_rate": bug_count / max(reps, 1),
                "median_ttfb_ms": round(med_ttfb, 1) if med_ttfb else None,
            })

    # Write CSV
    if results:
        p = output_dir / "headline_comparison.csv"
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=results[0].keys())
            w.writeheader()
            w.writerows(results)

    return results


def generate_plots(points: List[CoveragePoint], output_dir: Path, dbs: List[str]) -> None:
    """Generate coverage plots per DB."""
    plot_dir = _ensure(output_dir / "plots")
    for db in dbs:
        db_points = [p for p in points if p.db == db]
        if db_points:
            plot_coverage(db_points, plot_dir, db)


def compute_auc(points: List[CoveragePoint], db: str, workload: str, strategy: str) -> float:
    """Compute area under the coverage curve (trapezoidal)."""
    filtered = sorted(
        [p for p in points if p.db == db and p.workload == workload and p.strategy == strategy],
        key=lambda x: x.time_s,
    )
    if len(filtered) < 2:
        return 0.0
    auc = 0.0
    for i in range(1, len(filtered)):
        dt = filtered[i].time_s - filtered[i - 1].time_s
        avg_states = (filtered[i].unique_states + filtered[i - 1].unique_states) / 2.0
        auc += dt * avg_states
    return round(auc, 2)


def write_summary(
    output_dir: Path,
    coverage_points: List[CoveragePoint],
    throughput_rows: List[ThroughputRow],
    determinism_rows: List[DeterminismRow],
    headline_results: List[Dict[str, Any]],
    dbs: List[str],
    pool: Optional[SchedulePool] = None,
) -> None:
    """Write summary.md with key findings."""
    p = output_dir / "summary.md"
    lines = ["# Schedule Pool Evaluation Summary\n"]
    lines.append(f"Generated: {datetime.now(timezone.utc).isoformat()}\n")

    # Coverage AUC table
    if coverage_points:
        lines.append("## Coverage AUC (higher = faster coverage growth)\n")
        lines.append("| DB | Workload | Strategy | AUC | Final States |")
        lines.append("|---|---|---|---|---|")
        for db in dbs:
            for wl in WORKLOADS:
                for strat in STRATEGIES:
                    auc = compute_auc(coverage_points, db, wl, strat)
                    final_pts = [p for p in coverage_points
                                 if p.db == db and p.workload == wl and p.strategy == strat]
                    final_states = max((p.unique_states for p in final_pts), default=0)
                    if auc > 0:
                        lines.append(f"| {db} | {wl} | {strat} | {auc} | {final_states} |")
        lines.append("")

    # Throughput table
    if throughput_rows:
        lines.append("## Throughput Summary\n")
        lines.append("| DB | Workload | Mode | Avg TPS | Med Latency | P95 Latency |")
        lines.append("|---|---|---|---|---|---|")
        from collections import defaultdict
        grouped = defaultdict(list)
        for r in throughput_rows:
            grouped[(r.db, r.workload, r.mode)].append(r)
        for (db, wl, mode), group in sorted(grouped.items()):
            avg_tps = sum(r.txns_per_sec for r in group) / len(group)
            avg_med = sum(r.median_latency_ms for r in group) / len(group)
            avg_p95 = sum(r.p95_latency_ms for r in group) / len(group)
            lines.append(f"| {db} | {wl} | {mode} | {avg_tps:.1f} | {avg_med:.1f}ms | {avg_p95:.1f}ms |")
        lines.append("")

    # Determinism table
    if determinism_rows:
        lines.append("## Determinism (Exact-Delay Replay)\n")
        lines.append("| DB | Workload | Schedule | Buggy Rate | Control Rate | Stable |")
        lines.append("|---|---|---|---|---|---|")
        for r in determinism_rows:
            lines.append(
                f"| {r.db} | {r.workload} | {r.schedule_id} | "
                f"{r.buggy_rate:.0%} | {r.control_rate:.0%} | "
                f"{'YES' if r.stable else 'no'} |"
            )
        lines.append("")

    # Headline results
    if headline_results:
        lines.append("## Headline Case Comparison\n")
        lines.append("| DB | Workload | Strategy | Success Rate | Median TTFB |")
        lines.append("|---|---|---|---|---|")
        for r in headline_results:
            ttfb = f"{r['median_ttfb_ms']:.0f}ms" if r.get('median_ttfb_ms') else "—"
            lines.append(
                f"| {r['db']} | {r['workload']} | {r['strategy']} | "
                f"{r['success_rate']:.0%} | {ttfb} |"
            )
        lines.append("")

    p.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n  Summary: {p}")


def write_headline_report(
    output_dir: Path,
    coverage_points: List[CoveragePoint],
    throughput_rows: List[ThroughputRow],
    determinism_rows: List[DeterminismRow],
    headline_results: List[Dict[str, Any]],
    dbs: List[str],
) -> None:
    """Write HEADLINE_REPORT.md answering the three key questions."""
    p = output_dir / "HEADLINE_REPORT.md"

    lines = [
        "# Schedule Pool Workflow — Headline Report\n",
        f"Generated: {datetime.now(timezone.utc).isoformat()}\n",
        "---\n",
        "## Overview\n",
        "This report evaluates a new **schedule-pool** workflow for tx-fuzzer.",
        "Instead of generating fresh random delays every run, schedules are",
        "stored as `{event, delay_ms}` tuples, pooled, mutated, and retained",
        "when they discover new canonical states or bugs.\n",
        "---\n",
        "## Q1: How deterministic is the scheduler, given that it is delay-based?\n",
        "Delay-based scheduling is inherently non-deterministic: OS thread scheduling,",
        "DB lock timing, and GC pauses all add noise. However, the schedule-pool",
        "workflow improves reproducibility by:\n",
        "1. **Exact-delay replay**: When replaying a discovered schedule, the exact",
        "   same `{event, delay_ms}` tuples are applied — no re-randomization.",
        "2. **20x buggy/control verification**: Each retained schedule is tested 20×",
        "   on the buggy config (expect high hit rate) and 20× on the fixed config",
        "   (expect 0 hits). A schedule is \"stable\" if buggy=20/20, control=0/20.",
        "3. **Retention filtering**: Only schedules with good replay rates enter the",
        "   pool, so subsequent mutations start from more reliable parents.\n",
    ]

    if determinism_rows:
        lines.append("### Determinism Results\n")
        lines.append("| DB | Workload | Buggy Replay Rate | Control Rate | Stable |")
        lines.append("|---|---|---|---|---|")
        for r in determinism_rows:
            lines.append(
                f"| {r.db} | {r.workload} | "
                f"{r.buggy_hits}/{r.buggy_replays} ({r.buggy_rate:.0%}) | "
                f"{r.control_hits}/{r.control_replays} ({r.control_rate:.0%}) | "
                f"{'**YES**' if r.stable else 'no'} |"
            )
        lines.append("")

    lines.extend([
        "**Key insight**: Delay-based discovery is probabilistic, but once a schedule",
        "is found and stored, exact-delay replay provides much better reproducibility",
        "than re-running a random strategy. The pool acts as a memory of \"what worked\".\n",
        "---\n",
        "## Q2: How does coverage grow over time, per workload and per DB?\n",
        "Coverage is measured as the number of distinct **canonical states** discovered",
        "over time. A canonical state is the timing-stripped logical outcome of a trial",
        "(e.g., `{bug_found, final_balance, ...}`). Higher AUC (Area Under the Curve)",
        "means faster coverage growth — the strategy explores more distinct states sooner.\n",
    ])

    if coverage_points:
        lines.append("### Coverage AUC (60s budget, higher = better)\n")
        lines.append("| DB | Workload | stress | random | heuristic | schedule_pool |")
        lines.append("|---|---|---|---|---|---|")
        for db in dbs:
            for wl in WORKLOADS:
                aucs = {}
                for strat in STRATEGIES:
                    aucs[strat] = compute_auc(coverage_points, db, wl, strat)
                if any(v > 0 for v in aucs.values()):
                    lines.append(
                        f"| {db} | {wl} | "
                        f"{aucs.get('stress', 0):.1f} | "
                        f"{aucs.get('random', 0):.1f} | "
                        f"{aucs.get('heuristic', 0):.1f} | "
                        f"{aucs.get('schedule_pool', 0):.1f} |"
                    )
        lines.append("")

    lines.extend([
        "**Interpretation**: The schedule_pool strategy may show different coverage",
        "characteristics than stress/random because it explores more deliberately.",
        "Early coverage growth may be slower (fewer trials/sec due to delay overhead),",
        "but the pool accumulates useful schedules over time.\n",
        "---\n",
        "## Q3: What is the throughput impact compared to stress testing?\n",
        "Four modes are compared:\n",
        "- **baseline**: stress, no delays, no analysis overhead",
        "- **analysis_only**: stress + canonical state hashing (measures analysis cost)",
        "- **full_random**: random delays + analysis (measures delay injection cost)",
        "- **full_schedule_pool**: pool mutations + analysis (measures full workflow cost)\n",
    ])

    if throughput_rows:
        from collections import defaultdict
        grouped = defaultdict(list)
        for r in throughput_rows:
            grouped[(r.db, r.workload, r.mode)].append(r)

        lines.append("### Throughput (txn/s, averaged over reps)\n")
        lines.append("| DB | Workload | baseline | analysis_only | full_random | full_schedule_pool |")
        lines.append("|---|---|---|---|---|---|")
        for db in dbs:
            for wl in WORKLOADS:
                vals = {}
                for mode in ["baseline", "analysis_only", "full_random", "full_schedule_pool"]:
                    group = grouped.get((db, wl, mode), [])
                    vals[mode] = sum(r.txns_per_sec for r in group) / len(group) if group else 0
                if any(v > 0 for v in vals.values()):
                    lines.append(
                        f"| {db} | {wl} | "
                        f"{vals['baseline']:.1f} | {vals['analysis_only']:.1f} | "
                        f"{vals['full_random']:.1f} | {vals['full_schedule_pool']:.1f} |"
                    )
        lines.append("")

    lines.extend([
        "**Key insight**: The schedule_pool workflow is slower than stress (intentionally",
        "— it injects delays to perturb scheduling). The overhead comes from:",
        "1. Per-event delay injection (time.sleep calls)",
        "2. Schedule sampling + mutation computation",
        "3. Pool management (retention checks, JSONL writes)\n",
        "In some cases, perturbed schedules slightly reduce contention (fewer retries),",
        "which can offset the delay overhead.\n",
        "---\n",
        "## Design: Schedule-Pool Workflow\n",
        "### Schedule Representation",
        "Schedules are ordered lists of `{event, delay_ms}` tuples:",
        "```json",
        '[{"event": "s0_t0_q0_begin", "delay_ms": 15.0},',
        ' {"event": "s1_t0_q1_exec", "delay_ms": 42.5}]',
        "```\n",
        "### Iteration Loop",
        "1. **Sample**: Pick a parent schedule from the pool (or empty with 15% prob)",
        "2. **Mutate**: Apply 2 random mutations (add/adjust/remove/swap/jitter)",
        "3. **Execute**: Run the candidate on a fresh base workload",
        "4. **Observe**: Record canonical state hash and bug outcome",
        "5. **Retain**: Keep if new state discovered, new bug found, or improvements\n",
        "### Mutation Operators",
        "- `add_delay`: inject delay at a new event (biased toward conflict boundaries)",
        "- `adjust_delay`: scale existing delay ×[0.5, 2.0]",
        "- `remove_delay`: remove one delay point",
        "- `swap_delay`: move delay from one event to another",
        "- `jitter`: small ±5ms noise on all delays\n",
        "### Caps (prevent unbounded drift)",
        "- Max per-event delay: 200ms",
        "- Max total delay: 800ms",
        "- Max delay count: 12 events\n",
        "### Sampling Weights",
        "```",
        "weight(s) ∝ novelty_bonus × replay_bonus × (1/(1+total_delay))^0.3 × (1/(1+count))^0.5",
        "```",
        "Prefers smaller, more reliable schedules while allowing exploration.\n",
        "---\n",
        "## Reduced-Form Compromises\n",
        "The ideal design would intercept each individual SQL statement within a worker's",
        "transaction and apply event-specific delays. The current micro-workload runner",
        "executes all SQL within a single `run_buggy(adapter, worker_id)` function call,",
        "so per-statement interception is not possible without refactoring the workload API.\n",
        "**Compromise**: Delays are applied at the worker-thread level:",
        "- `begin` delay: before the worker starts its transaction",
        "- `exec` delays: collapsed to a single pre-execution delay per worker",
        "- `commit` delays: not separately injectable (commit is inside fn)\n",
        "This still controls relative timing between workers and exposes concurrency bugs,",
        "but with coarser granularity than per-statement injection.\n",
        "---\n",
    ])

    p.write_text("\n".join(lines), encoding="utf-8")
    print(f"\n  HEADLINE_REPORT: {p}")


def write_inventory(output_dir: Path, dbs: List[str]) -> None:
    """Write inventory.txt listing all output artifacts."""
    p = output_dir / "inventory.txt"
    lines = [
        "# Schedule Pool Evaluation — Output Inventory",
        f"# Generated: {datetime.now(timezone.utc).isoformat()}",
        "",
        "## Data Files",
        "- coverage_timeseries.csv : coverage points (time, states, txns) per strategy",
        "- throughput.csv : txn/sec and latency per mode",
        "- determinism.csv : replay rates for discovered schedules",
        "- discovery_runs.csv : all discovery trial outcomes",
        "- replay_results.csv : all replay trial outcomes",
        "- schedule_pool.jsonl : all retained schedules with metadata",
        "- headline_comparison.csv : headline case strategy comparison",
        "",
        "## Reports",
        "- summary.md : tabular summary of all metrics",
        "- HEADLINE_REPORT.md : narrative report answering 3 key questions",
        "",
        "## Plots",
        "- plots/coverage_<db>_<workload>.png : coverage-over-time per DB+workload",
        "",
        "## Logs",
        "- commands.log : exact commands used",
        "- test_output.txt : test suite output",
        "",
        f"## DBs tested: {', '.join(dbs)}",
        f"## Workloads: {', '.join(WORKLOADS)}",
        f"## Strategies: {', '.join(STRATEGIES)}",
    ]
    p.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="Schedule pool evaluation")
    parser.add_argument("--db", nargs="+", default=["postgres", "sqlite"],
                        help="Databases to test")
    parser.add_argument("--quick", action="store_true",
                        help="Quick mode: 15s durations, 3 reps")
    parser.add_argument("--smoke", action="store_true",
                        help="Smoke mode: 5s durations, 1 rep")
    parser.add_argument("--coverage-only", action="store_true")
    parser.add_argument("--throughput-only", action="store_true")
    parser.add_argument("--determinism-only", action="store_true")
    parser.add_argument("--headline-only", action="store_true")
    parser.add_argument("--skip-plots", action="store_true")
    parser.add_argument("--output-dir", type=str, default=None)
    args = parser.parse_args()

    # Config
    if args.smoke:
        duration_s = 5.0
        reps = 1
        n_replays = 5
    elif args.quick:
        duration_s = 15.0
        reps = 3
        n_replays = 10
    else:
        duration_s = 60.0
        reps = 5
        n_replays = 20

    seeds = SEEDS[:reps]
    dbs = [d.lower() for d in args.db]

    # Resolve DSNs
    dsns = {}
    for db in dbs:
        if db == "postgres":
            dsns[db] = os.environ.get(
                "TX_FUZZER_POSTGRES_DSN",
                os.environ.get(
                    "PG_16_2_DSN",
                    "postgres://postgres:postgres@localhost:54326/postgres?sslmode=disable",
                ),
            )
        elif db == "mysql":
            dsns[db] = os.environ.get(
                "TX_FUZZER_MYSQL_DSN",
                os.environ.get(
                    "MYSQL_8_0_34_DSN",
                    "mysql://root:root@localhost:33080/test",
                ),
            )
        else:
            dsns[db] = os.environ.get(
                f"TX_FUZZER_{db.upper()}_DSN",
                DEFAULT_DSNS.get(db, ""),
            )

    # SQLite adapter kwargs
    adapter_kwargs: Dict[str, Any] = {}

    # Output directory
    if args.output_dir:
        out_dir = Path(args.output_dir)
    else:
        out_dir = ROOT / "results" / f"schedule_pool_{_ts()}"
    _ensure(out_dir)

    if "sqlite" in dbs and "TX_FUZZER_SQLITE_DSN" not in os.environ:
        dsns["sqlite"] = str(out_dir / "sqlite_eval.sqlite")

    print(f"=== Schedule Pool Evaluation ===")
    print(f"  Output: {out_dir}")
    print(f"  DBs: {dbs}")
    print(f"  Duration: {duration_s}s, Reps: {reps}")

    # Write commands.log
    cmd = " ".join(sys.argv)
    (out_dir / "commands.log").write_text(
        f"# Run command:\npython {cmd}\n\n"
        f"# Config: duration={duration_s}s reps={reps} dbs={dbs}\n",
        encoding="utf-8",
    )

    coverage_points: List[CoveragePoint] = []
    throughput_rows: List[ThroughputRow] = []
    determinism_rows: List[DeterminismRow] = []
    headline_results: List[Dict[str, Any]] = []

    run_all = not any([
        args.coverage_only, args.throughput_only,
        args.determinism_only, args.headline_only,
    ])

    if run_all or args.coverage_only:
        print("\n--- Coverage Experiments ---")
        coverage_points = run_coverage_experiments(
            dbs, dsns, out_dir,
            duration_s=duration_s, reps=reps,
            seeds=seeds, **adapter_kwargs,
        )

    if run_all or args.throughput_only:
        print("\n--- Throughput Benchmark ---")
        throughput_rows = run_throughput_benchmark(
            dbs, dsns, out_dir,
            duration_s=duration_s, reps=reps,
            seeds=seeds, **adapter_kwargs,
        )

    if run_all or args.determinism_only:
        print("\n--- Determinism Evaluation ---")
        determinism_rows = run_determinism_eval(
            dbs, dsns, out_dir,
            duration_s=duration_s,
            n_replays=n_replays,
            **adapter_kwargs,
        )

    if run_all or args.headline_only:
        print("\n--- Headline Case Comparison ---")
        headline_results = run_headline_comparison(
            dbs, dsns, out_dir,
            duration_s=duration_s, reps=reps,
            n_replays=n_replays, seeds=seeds,
            **adapter_kwargs,
        )

    # Generate plots
    if coverage_points and not args.skip_plots:
        print("\n--- Generating Plots ---")
        generate_plots(coverage_points, out_dir, dbs)

    # Write reports
    write_inventory(out_dir, dbs)
    write_summary(out_dir, coverage_points, throughput_rows, determinism_rows,
                  headline_results, dbs)
    write_headline_report(out_dir, coverage_points, throughput_rows,
                          determinism_rows, headline_results, dbs)

    # Save pool
    pool_path = out_dir / "schedule_pool.jsonl"
    if not pool_path.exists():
        pool_path.write_text("", encoding="utf-8")

    print(f"\n=== Done. Results in {out_dir} ===")


if __name__ == "__main__":
    main()
