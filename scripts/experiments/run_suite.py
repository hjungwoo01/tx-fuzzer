#!/usr/bin/env python3
"""
Experiment harness for the tx-fuzzer PostgreSQL evaluation suite.

Reads a YAML config defining workloads, postgres targets, strategies,
and repetition counts, then executes the full matrix and produces
summary results with replay verification.

Usage:
    python scripts/experiments/run_suite.py --config experiments/postgres_quick.yaml
    python scripts/experiments/run_suite.py --config experiments/postgres_long.yaml --smoke
"""

from __future__ import annotations

import argparse
import copy
import csv
import datetime
import hashlib
import json
import os
import random
import math
import statistics
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

try:
    import psycopg2
except ImportError:
    print("ERROR: psycopg2 required. pip install psycopg2-binary", file=sys.stderr)
    sys.exit(1)

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from workloads import OracleResult, list_workloads, load_workload


@dataclass
class TrialMetrics:
    """Metrics from a single trial."""
    trial_idx: int = 0
    bug_found: bool = False
    got_40001: bool = False
    got_40P01: bool = False
    oracle_result: Optional[OracleResult] = None
    elapsed_s: float = 0.0
    state_hash: str = ""
    delays_used: Dict[str, float] = field(default_factory=dict)
    delay_tiers: List[str] = field(default_factory=list)
    error_count: int = 0
    details: str = ""


@dataclass
class ExperimentResult:
    """Aggregated results from one experiment (workload × strategy × isolation × pg)."""
    workload: str = ""
    strategy: str = ""
    isolation: str = ""
    pg_version: str = ""
    variant: str = ""
    repetition: int = 0
    trials_run: int = 0
    bugs_found: int = 0
    first_bug_trial: Optional[int] = None
    first_bug_time_s: Optional[float] = None
    anomaly_rate: float = 0.0
    rate_40001: float = 0.0
    serialization_fail_rate: float = 0.0  # 40001 only
    deadlock_rate: float = 0.0              # 40P01 only
    unique_states: int = 0
    total_time_s: float = 0.0
    trial_metrics: List[TrialMetrics] = field(default_factory=list)
    found_artifact: Optional[Dict] = None
    replay_success_rate: Optional[float] = None
    replay_min: Optional[float] = None
    replay_max: Optional[float] = None
    rng_seed: int = 0
    success_rate_within_budget: float = 0.0  # fraction of reps that found >= 1 bug
    ttfb_seconds_success_only: Optional[float] = None  # median TTFB among reps that found bugs
    ttfb_trials_success_only: Optional[int] = None
    p_value_heuristic_vs_random: Optional[float] = None


DEFAULT_PORTS = {"12.3": 54323, "12.4": 54324, "16.2": 54326}


def dsn_for_pg(version: str, port_override: Optional[int] = None) -> str:
    port = port_override or DEFAULT_PORTS.get(version, 5432)
    return f"postgres://postgres:postgres@localhost:{port}/postgres?sslmode=disable"


def wait_for_pg(dsn: str, timeout: float = 30.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            conn = psycopg2.connect(dsn)
            conn.close()
            return True
        except Exception:
            time.sleep(0.5)
    return False


def get_pg_version_string(dsn: str) -> str:
    try:
        conn = psycopg2.connect(dsn)
        conn.autocommit = True
        cur = conn.cursor()
        cur.execute("SELECT version()")
        v = cur.fetchone()[0]
        conn.close()
        return v
    except Exception as e:
        return f"ERROR: {e}"


def execute_sql_list(dsn: str, sqls: List[str]) -> None:
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    cur = conn.cursor()
    for sql in sqls:
        try:
            cur.execute(sql)
        except Exception:
            pass
    conn.close()


def hash_final_state(state: Any) -> str:
    """SHA-256 hash of JSON-encoded final state."""
    raw = json.dumps(state, sort_keys=True, default=str)
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def canonical_state_hash(wl_mod: Any, result: Any) -> str:
    """Compute a canonical logical-state hash for any workload result.

    Prefers wl_mod.canonical_state(result) if available (returns a
    JSON-serialisable dict/list describing only the logical outcome).
    Falls back to extracting 'final_state' from dict results, or a
    lightweight summary from TrialResult dataclasses.
    """
    if hasattr(wl_mod, 'canonical_state'):
        return hash_final_state(wl_mod.canonical_state(result))
    # Dict-style result
    if isinstance(result, dict):
        return hash_final_state(result.get('final_state', {}))
    # TrialResult dataclass — build a timing-free snapshot
    snap: Dict[str, Any] = {}
    for attr in ('bug_found', 'got_40001', 'foo_select_rows', 'bar_select_rows',
                 'foo_commit_error', 'bar_commit_error',
                 'trouble_write_rows_affected', 'variant'):
        if hasattr(result, attr):
            snap[attr] = getattr(result, attr)
    # Include final_state if present
    if hasattr(result, 'final_state'):
        snap['final_state'] = getattr(result, 'final_state')
    return hash_final_state(snap)


def wilson_ci(successes: int, trials: int, z: float = 1.96) -> Tuple[float, float]:
    """Wilson score interval for a binomial proportion (95% CI by default)."""
    if trials == 0:
        return (0.0, 0.0)
    p_hat = successes / trials
    denom = 1 + z * z / trials
    centre = (p_hat + z * z / (2 * trials)) / denom
    spread = z * math.sqrt((p_hat * (1 - p_hat) + z * z / (4 * trials)) / trials) / denom
    return (max(0.0, centre - spread), min(1.0, centre + spread))


def format_ci(lo: float, hi: float) -> str:
    """Format a Wilson CI as a compact string."""
    return f"[{lo:.1%}\u2013{hi:.1%}]"


def fisher_exact_one_sided(a: int, b: int, c: int, d: int) -> float:
    """One-sided Fisher's exact test (greater): tests if a/(a+b) > c/(c+d).

    Uses scipy if available, otherwise a conservative fallback via the
    hypergeometric distribution.

    2×2 table:  [[a, b], [c, d]]
    Null: proportion in row 1 <= proportion in row 2.
    Alternative: proportion in row 1 > proportion in row 2.
    """
    try:
        from scipy.stats import fisher_exact
        _, p = fisher_exact([[a, b], [c, d]], alternative='greater')
        return float(p)
    except ImportError:
        pass
    # Manual hypergeometric fallback
    try:
        from math import comb, log
        n = a + b + c + d
        r1 = a + b
        c1 = a + c
        p_val = 0.0
        for i in range(a, min(r1, c1) + 1):
            p_val += comb(r1, i) * comb(n - r1, c1 - i) / comb(n, c1)
        return float(p_val)
    except Exception:
        return 1.0


def _extract_got_40P01(result: Any) -> bool:
    """Extract whether a trial hit deadlock_detected (40P01) from the result."""
    # Check explicit field first
    if hasattr(result, 'got_40P01'):
        return bool(result.got_40P01)
    if isinstance(result, dict) and result.get('got_40P01'):
        return True
    # Scan error strings for 40P01
    errors = []
    if hasattr(result, 'all_errors'):
        errors = getattr(result, 'all_errors', [])
    elif isinstance(result, dict):
        errors = result.get('errors', [])
    for e in errors:
        if '40P01' in str(e) or 'deadlock detected' in str(e).lower():
            return True
    return False


def run_baseline_trials(
    wl_mod: Any,
    dsn: str,
    isolation: str,
    variant: str,
    num_trials: int,
    seed: int,
    **kwargs,
) -> List[TrialMetrics]:
    """Run trials with NO delays (baseline)."""
    metrics = []
    for i in range(num_trials):
        t0 = time.monotonic()
        if hasattr(wl_mod, 'run_trial'):
            result = wl_mod.run_trial(dsn, isolation=isolation, variant=variant, seed=seed + i)
        else:
            break
        elapsed = time.monotonic() - t0

        # Build oracle result
        if hasattr(result, 'bug_found'):
            # TrialResult dataclass (update_conflict_out)
            oracle_r = wl_mod.oracle(
                getattr(result, 'execution_log', []), None, trial_result=result)
            tm = TrialMetrics(
                trial_idx=i, bug_found=result.bug_found,
                got_40001=result.got_40001,
                got_40P01=_extract_got_40P01(result),
                oracle_result=oracle_r,
                elapsed_s=elapsed,
                state_hash=canonical_state_hash(wl_mod, result),
                error_count=len(getattr(result, 'all_errors', [])),
            )
        else:
            # Dict result (other workloads)
            oracle_r = wl_mod.oracle(
                result.get('execution_log', []),
                result.get('final_state'),
                trial_result=result,
            )
            tm = TrialMetrics(
                trial_idx=i, bug_found=result.get('bug_found', False),
                got_40001=result.get('got_40001', False),
                got_40P01=_extract_got_40P01(result),
                oracle_result=oracle_r,
                elapsed_s=elapsed,
                state_hash=canonical_state_hash(wl_mod, result),
                error_count=len(result.get('errors', [])),
            )
        metrics.append(tm)
    return metrics


def run_heuristic_trials(
    wl_mod: Any,
    dsn: str,
    isolation: str,
    variant: str,
    budget: int,
    k: int,
    seed: int,
    disable_commit_hold: bool = False,
    disable_key_tiers: bool = False,
    **kwargs,
) -> List[TrialMetrics]:
    """Run heuristic fuzzer trials with delay injection."""
    rng = random.Random(seed)
    trace = wl_mod.seed_trace(isolation=isolation, variant=variant, seed=seed)

    # Build delay candidates
    if hasattr(wl_mod, 'build_delay_candidates'):
        extra_kwargs = {}
        if 'clients' in wl_mod.seed_trace.__code__.co_varnames:
            pass  # default clients
        candidates = wl_mod.build_delay_candidates(trace)
    else:
        candidates = []

    # Apply ablations
    if disable_commit_hold:
        candidates = [c for c in candidates if c.get("tier") != "tier_commit_hold"]
    if disable_key_tiers:
        candidates = [c for c in candidates
                      if c.get("tier") in ("tier_commit_hold", "tier_fallback")]

    metrics = []
    for i in range(budget):
        # Select delays via heuristic lottery
        if candidates and hasattr(wl_mod, 'select_delays'):
            delays = wl_mod.select_delays(candidates, rng, k=k)
        else:
            delays = {}

        t0 = time.monotonic()
        if hasattr(wl_mod, 'run_trial'):
            result = wl_mod.run_trial(dsn, isolation=isolation, variant=variant,
                                       delays=delays, seed=seed + i)
        else:
            break
        elapsed = time.monotonic() - t0

        # Process result
        if hasattr(result, 'bug_found'):
            oracle_r = wl_mod.oracle(
                getattr(result, 'execution_log', []), None, trial_result=result)
            tm = TrialMetrics(
                trial_idx=i, bug_found=result.bug_found,
                got_40001=result.got_40001,
                got_40P01=_extract_got_40P01(result),
                oracle_result=oracle_r,
                elapsed_s=elapsed,
                state_hash=canonical_state_hash(wl_mod, result),
                delays_used=delays,
                delay_tiers=[],
                error_count=len(getattr(result, 'all_errors', [])),
            )
        else:
            oracle_r = wl_mod.oracle(
                result.get('execution_log', []),
                result.get('final_state'),
                trial_result=result,
            )
            tm = TrialMetrics(
                trial_idx=i, bug_found=result.get('bug_found', False),
                got_40001=result.get('got_40001', False),
                got_40P01=_extract_got_40P01(result),
                oracle_result=oracle_r,
                elapsed_s=elapsed,
                state_hash=canonical_state_hash(wl_mod, result),
                delays_used=delays,
                delay_tiers=[],
                error_count=len(result.get('errors', [])),
            )

        metrics.append(tm)
    return metrics


def run_random_delay_trials(
    wl_mod: Any,
    dsn: str,
    isolation: str,
    variant: str,
    num_trials: int,
    k: int,
    seed: int,
    **kwargs,
) -> List[TrialMetrics]:
    """Run trials with random (non-heuristic) delay injection."""
    rng = random.Random(seed)
    trace = wl_mod.seed_trace(isolation=isolation, variant=variant, seed=seed)
    metrics = []

    for i in range(num_trials):
        if hasattr(wl_mod, 'select_random_delays'):
            delays = wl_mod.select_random_delays(trace, rng, k=k)
        else:
            delays = {}

        t0 = time.monotonic()
        if hasattr(wl_mod, 'run_trial'):
            result = wl_mod.run_trial(dsn, isolation=isolation, variant=variant,
                                       delays=delays, seed=seed + i)
        else:
            break
        elapsed = time.monotonic() - t0

        if hasattr(result, 'bug_found'):
            oracle_r = wl_mod.oracle(
                getattr(result, 'execution_log', []), None, trial_result=result)
            tm = TrialMetrics(
                trial_idx=i, bug_found=result.bug_found,
                got_40001=result.got_40001,
                got_40P01=_extract_got_40P01(result),
                oracle_result=oracle_r,
                elapsed_s=elapsed,
                state_hash=canonical_state_hash(wl_mod, result),
                delays_used=delays,
                error_count=len(getattr(result, 'all_errors', [])),
            )
        else:
            oracle_r = wl_mod.oracle(
                result.get('execution_log', []),
                result.get('final_state'),
                trial_result=result,
            )
            tm = TrialMetrics(
                trial_idx=i, bug_found=result.get('bug_found', False),
                got_40001=result.get('got_40001', False),
                got_40P01=_extract_got_40P01(result),
                oracle_result=oracle_r,
                elapsed_s=elapsed,
                state_hash=canonical_state_hash(wl_mod, result),
                delays_used=delays,
                error_count=len(result.get('errors', [])),
            )
        metrics.append(tm)
    return metrics


def replay_verify(
    wl_mod: Any,
    dsn: str,
    isolation: str,
    variant: str,
    delays: Dict[str, float],
    num_replays: int = 3,
    seed: int = 42,
) -> Dict[str, Any]:
    """Replay a found artifact multiple times to check reproducibility."""
    successes = 0
    for i in range(num_replays):
        result = wl_mod.run_trial(dsn, isolation=isolation, variant=variant,
                                   delays=delays, seed=seed + i * 100)
        if hasattr(result, 'bug_found'):
            if result.bug_found:
                successes += 1
        elif isinstance(result, dict) and result.get('bug_found'):
            successes += 1
    return {
        "replays": num_replays,
        "successes": successes,
        "rate": successes / max(1, num_replays),
    }


def cross_version_verify(
    wl_mod: Any,
    fixed_dsn: str,
    isolation: str,
    variant: str,
    delays: Dict[str, float],
    num_replays: int = 3,
    seed: int = 42,
) -> Dict[str, Any]:
    """Run the same artifact on the fixed version to confirm it no longer triggers."""
    triggers = 0
    got_40001 = 0
    for i in range(num_replays):
        result = wl_mod.run_trial(fixed_dsn, isolation=isolation, variant=variant,
                                   delays=delays, seed=seed + i * 100)
        if hasattr(result, 'bug_found'):
            if result.bug_found:
                triggers += 1
            if result.got_40001:
                got_40001 += 1
        elif isinstance(result, dict):
            if result.get('bug_found'):
                triggers += 1
            if result.get('got_40001'):
                got_40001 += 1
    return {
        "replays": num_replays,
        "bug_triggers": triggers,
        "got_40001": got_40001,
        "fixed": triggers == 0,
    }


def aggregate_experiment(
    workload: str,
    strategy: str,
    isolation: str,
    pg_version: str,
    variant: str,
    repetition: int,
    trial_metrics: List[TrialMetrics],
    rng_seed: int,
    found_artifact_delays: Optional[Dict[str, float]] = None,
) -> ExperimentResult:
    """Aggregate trial metrics into an ExperimentResult."""
    bugs = [m for m in trial_metrics if m.bug_found]
    states = set(m.state_hash for m in trial_metrics)
    total_time = sum(m.elapsed_s for m in trial_metrics)

    first_bug_trial = None
    first_bug_time = None
    if bugs:
        first = bugs[0]
        first_bug_trial = first.trial_idx
        first_bug_time = sum(m.elapsed_s for m in trial_metrics[:first.trial_idx + 1])

    return ExperimentResult(
        workload=workload,
        strategy=strategy,
        isolation=isolation,
        pg_version=pg_version,
        variant=variant,
        repetition=repetition,
        trials_run=len(trial_metrics),
        bugs_found=len(bugs),
        first_bug_trial=first_bug_trial,
        first_bug_time_s=first_bug_time,
        anomaly_rate=len(bugs) / max(1, len(trial_metrics)),
        rate_40001=sum(1 for m in trial_metrics if m.got_40001) / max(1, len(trial_metrics)),
        serialization_fail_rate=sum(1 for m in trial_metrics if m.got_40001 and not m.got_40P01) / max(1, len(trial_metrics)),
        deadlock_rate=sum(1 for m in trial_metrics if m.got_40P01) / max(1, len(trial_metrics)),
        unique_states=len(states),
        total_time_s=total_time,
        trial_metrics=trial_metrics,
        found_artifact={"delays": found_artifact_delays} if found_artifact_delays else None,
        rng_seed=rng_seed,
        success_rate_within_budget=1.0 if bugs else 0.0,
        ttfb_seconds_success_only=first_bug_time if bugs else None,
        ttfb_trials_success_only=first_bug_trial if bugs else None,
    )


def run_experiment(
    workload_name: str,
    strategy_cfg: Dict[str, Any],
    isolation: str,
    pg_version: str,
    variant: str,
    repetition: int,
    base_seed: int,
    replay_count: int = 3,
    fixed_dsn: Optional[str] = None,
) -> ExperimentResult:
    """Run one experiment (one strategy × workload × isolation × pg × repetition)."""
    wl_mod = load_workload(workload_name)
    dsn = dsn_for_pg(pg_version)
    strategy_name = strategy_cfg["name"]
    seed = base_seed + repetition * 10000

    # Ensure DB schema exists
    execute_sql_list(dsn, wl_mod.setup_sql())
    if hasattr(wl_mod, '_init_sql'):
        execute_sql_list(dsn, wl_mod._init_sql())

    # Select strategy
    if strategy_name == "baseline_no_delays":
        trials = strategy_cfg.get("trials", 100)
        metrics = run_baseline_trials(wl_mod, dsn, isolation, variant, trials, seed)
    elif strategy_name == "random_delays":
        trials = strategy_cfg.get("trials", 100)
        k = strategy_cfg.get("k", 3)
        metrics = run_random_delay_trials(wl_mod, dsn, isolation, variant, trials, k, seed)
    elif strategy_name.startswith("heuristic"):
        budget = strategy_cfg.get("budget", 200)
        k = strategy_cfg.get("k", 3)
        disable_ch = strategy_cfg.get("disable_commit_hold", False)
        disable_kt = strategy_cfg.get("disable_key_tiers", False)
        metrics = run_heuristic_trials(
            wl_mod, dsn, isolation, variant, budget, k, seed,
            disable_commit_hold=disable_ch, disable_key_tiers=disable_kt)
    else:
        print(f"  WARNING: Unknown strategy {strategy_name}, skipping", file=sys.stderr)
        return ExperimentResult(workload=workload_name, strategy=strategy_name)

    # Find first artifact with a bug for replay verification
    found_delays = None
    for m in metrics:
        if m.bug_found and m.delays_used:
            found_delays = m.delays_used
            break

    result = aggregate_experiment(
        workload_name, strategy_name, isolation, pg_version, variant,
        repetition, metrics, seed, found_delays)

    # Replay verification if bug found
    if found_delays and replay_count > 0:
        replay_res = replay_verify(wl_mod, dsn, isolation, variant,
                                    found_delays, replay_count, seed + 99999)
        result.replay_success_rate = replay_res["rate"]

        # Cross-version check
        if fixed_dsn:
            cross_res = cross_version_verify(wl_mod, fixed_dsn, isolation, variant,
                                              found_delays, replay_count, seed + 88888)
            if result.found_artifact:
                result.found_artifact["cross_version"] = cross_res

    return result


def run_suite(config: Dict[str, Any], smoke: bool = False) -> List[ExperimentResult]:
    """Run the full experiment matrix defined in config."""
    results: List[ExperimentResult] = []
    repetitions = 1 if smoke else config.get("repetitions", 3)
    base_seed = config.get("base_seed", 42)

    experiments = config.get("experiments", [])
    total_exps = sum(
        len(exp.get("strategies", [])) *
        len(exp.get("isolation_levels", ["READ COMMITTED"])) *
        len(exp.get("pg_versions", ["16.2"])) *
        repetitions
        for exp in experiments
    )
    print(f"\n{'='*70}")
    print(f"  EXPERIMENT SUITE: {len(experiments)} workloads, {total_exps} total experiment runs")
    print(f"  Repetitions: {repetitions}, Base seed: {base_seed}")
    if smoke:
        print(f"  MODE: SMOKE TEST (1 repetition only)")
    print(f"{'='*70}\n")

    exp_idx = 0
    for exp_cfg in experiments:
        wl_name = exp_cfg["workload"]
        variants = exp_cfg.get("variants", ["default"])
        iso_levels = exp_cfg.get("isolation_levels", ["READ COMMITTED"])
        pg_versions = exp_cfg.get("pg_versions", ["16.2"])
        strategies = exp_cfg.get("strategies", [])
        fixed_dsn = exp_cfg.get("fixed_dsn")
        replay_count = 1 if smoke else exp_cfg.get("replay_count", 3)

        for variant in variants:
            for iso in iso_levels:
                for pg_ver in pg_versions:
                    # Verify PG is accessible
                    dsn = dsn_for_pg(pg_ver)
                    if not wait_for_pg(dsn, timeout=5.0):
                        print(f"  SKIP: PostgreSQL {pg_ver} not available")
                        continue

                    for strat_cfg in strategies:
                        strat_name = strat_cfg["name"]
                        # For smoke, reduce trials/budget
                        if smoke:
                            strat_cfg = copy.deepcopy(strat_cfg)
                            if "trials" in strat_cfg:
                                strat_cfg["trials"] = min(strat_cfg["trials"], 10)
                            if "budget" in strat_cfg:
                                strat_cfg["budget"] = min(strat_cfg["budget"], 10)

                        for rep in range(repetitions):
                            exp_idx += 1
                            print(f"  [{exp_idx}/{total_exps}] {wl_name}/{variant} "
                                  f"| {strat_name} | {iso} | PG {pg_ver} | rep {rep+1}/{repetitions}")

                            try:
                                r = run_experiment(
                                    wl_name, strat_cfg, iso, pg_ver, variant,
                                    rep, base_seed, replay_count, fixed_dsn)
                                results.append(r)

                                # Brief result summary
                                bugs = r.bugs_found
                                trials = r.trials_run
                                rate = r.anomaly_rate * 100
                                t = r.total_time_s
                                replay_s = f", replay={r.replay_success_rate:.0%}" if r.replay_success_rate is not None else ""
                                print(f"    → bugs={bugs}/{trials} ({rate:.1f}%), "
                                      f"states={r.unique_states}, "
                                      f"40001={r.rate_40001:.1%}, "
                                      f"time={t:.1f}s{replay_s}")
                            except Exception as e:
                                print(f"    → ERROR: {e}", file=sys.stderr)
                                import traceback
                                traceback.print_exc()

    return results


def generate_summary(results: List[ExperimentResult], output_dir: Path,
                     pg_versions_info: Dict[str, str]) -> None:
    """Generate summary files: JSONL, CSV, Markdown."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # JSONL
    jsonl_path = output_dir / "summary.jsonl"
    with open(jsonl_path, "w") as f:
        for r in results:
            row = {
                "workload": r.workload, "strategy": r.strategy,
                "isolation": r.isolation, "pg_version": r.pg_version,
                "variant": r.variant, "repetition": r.repetition,
                "trials_run": r.trials_run, "bugs_found": r.bugs_found,
                "first_bug_trial": r.first_bug_trial,
                "first_bug_time_s": round(r.first_bug_time_s, 3) if r.first_bug_time_s else None,
                "anomaly_rate": round(r.anomaly_rate, 4),
                "rate_40001": round(r.rate_40001, 4),
                "serialization_fail_rate": round(r.serialization_fail_rate, 4),
                "deadlock_rate": round(r.deadlock_rate, 4),
                "unique_states": r.unique_states,
                "total_time_s": round(r.total_time_s, 3),
                "replay_success_rate": r.replay_success_rate,
                "replay_min": r.replay_min,
                "replay_max": r.replay_max,
                "rng_seed": r.rng_seed,
                "success_rate_within_budget": round(r.success_rate_within_budget, 4),
                "ttfb_seconds_success_only": round(r.ttfb_seconds_success_only, 3) if r.ttfb_seconds_success_only else None,
                "ttfb_trials_success_only": r.ttfb_trials_success_only,
            }
            if r.p_value_heuristic_vs_random is not None:
                row["p_value_heuristic_vs_random"] = round(r.p_value_heuristic_vs_random, 6)
            if r.found_artifact:
                row["found_artifact"] = r.found_artifact
            f.write(json.dumps(row) + "\n")

    # CSV
    csv_path = output_dir / "summary.csv"
    fieldnames = [
        "workload", "strategy", "isolation", "pg_version", "variant",
        "repetition", "trials_run", "bugs_found", "anomaly_rate",
        "first_bug_trial", "first_bug_time_s", "rate_40001",
        "serialization_fail_rate", "deadlock_rate",
        "unique_states", "total_time_s", "replay_success_rate",
        "replay_min", "replay_max",
        "success_rate_within_budget", "ttfb_seconds_success_only",
        "ttfb_trials_success_only", "rng_seed",
    ]
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            writer.writerow({
                "workload": r.workload, "strategy": r.strategy,
                "isolation": r.isolation, "pg_version": r.pg_version,
                "variant": r.variant, "repetition": r.repetition,
                "trials_run": r.trials_run, "bugs_found": r.bugs_found,
                "anomaly_rate": round(r.anomaly_rate, 4),
                "first_bug_trial": r.first_bug_trial or "",
                "first_bug_time_s": round(r.first_bug_time_s, 3) if r.first_bug_time_s else "",
                "rate_40001": round(r.rate_40001, 4),
                "serialization_fail_rate": round(r.serialization_fail_rate, 4),
                "deadlock_rate": round(r.deadlock_rate, 4),
                "unique_states": r.unique_states,
                "total_time_s": round(r.total_time_s, 3),
                "replay_success_rate": round(r.replay_success_rate, 2) if r.replay_success_rate is not None else "",
                "replay_min": round(r.replay_min, 2) if r.replay_min is not None else "",
                "replay_max": round(r.replay_max, 2) if r.replay_max is not None else "",
                "success_rate_within_budget": round(r.success_rate_within_budget, 4),
                "ttfb_seconds_success_only": round(r.ttfb_seconds_success_only, 3) if r.ttfb_seconds_success_only else "",
                "ttfb_trials_success_only": r.ttfb_trials_success_only or "",
                "rng_seed": r.rng_seed,
            })

    # Markdown tables
    md_path = output_dir / "summary.md"
    _write_markdown_report(results, md_path, pg_versions_info)

    # Artifacts
    artifacts_dir = output_dir / "artifacts"
    artifacts_dir.mkdir(exist_ok=True)
    for r in results:
        if r.found_artifact and r.found_artifact.get("delays"):
            adir = artifacts_dir / r.workload / r.strategy
            adir.mkdir(parents=True, exist_ok=True)
            artifact_path = adir / f"found_trace_rep{r.repetition}.json"
            with open(artifact_path, "w") as f:
                json.dump({
                    "workload": r.workload, "strategy": r.strategy,
                    "isolation": r.isolation, "pg_version": r.pg_version,
                    "variant": r.variant, "repetition": r.repetition,
                    "delays": r.found_artifact["delays"],
                    "replay_success_rate": r.replay_success_rate,
                    "cross_version": r.found_artifact.get("cross_version"),
                    "rng_seed": r.rng_seed,
                }, f, indent=2)

    print(f"\n  Results written to: {output_dir}")
    print(f"    {jsonl_path}")
    print(f"    {csv_path}")
    print(f"    {md_path}")


def _write_markdown_report(
    results: List[ExperimentResult],
    path: Path,
    pg_versions_info: Dict[str, str],
) -> None:
    """Generate the Markdown summary report."""
    lines = []
    lines.append("# Evaluation Results\n")
    lines.append(f"Generated: {datetime.datetime.now(datetime.timezone.utc).isoformat()}\n")

    # PG versions
    lines.append("## PostgreSQL Versions\n")
    for ver, info in pg_versions_info.items():
        lines.append(f"- **{ver}**: `{info}`")
    lines.append("")

    # Group results by workload for aggregated tables
    from collections import defaultdict
    by_workload: Dict[str, List[ExperimentResult]] = defaultdict(list)
    for r in results:
        by_workload[r.workload].append(r)


    lines.append("## Main Results: Baseline vs Fuzzer\n")
    lines.append("| Workload | Strategy | Iso | PG | Var | "
                 "Bugs/Trials | Anomaly Rate | 95% CI | TTFB (med, success only) | States | "
                 "Serialization (40001) | Deadlock (40P01) | Success Rate |")
    lines.append("|----------|----------|-----|-----|-----|"
                 "------------|-------------|--------|--------------------------|--------|"
                 "----------------------|-----------------|--------------|")

    # Aggregate across repetitions
    agg = _aggregate_across_reps(results)
    for key, vals in sorted(agg.items()):
        wl, strat, iso, pg, var = key
        bugs_str = f"{vals['total_bugs']}/{vals['total_trials']}"
        ar = vals["mean_anomaly_rate"]
        ci_str = format_ci(vals["ci_lo"], vals["ci_hi"])
        ttfb = vals["median_ttfb_success_only"]
        us = vals["mean_unique_states"]
        ser_rate = vals["mean_serialization_fail_rate"]
        dl_rate = vals["mean_deadlock_rate"]
        sr = vals["success_rate_within_budget"]
        ttfb_str = f"{ttfb:.1f}s" if ttfb is not None else "\u2014"
        lines.append(
            f"| {wl} | {strat} | {iso} | {pg} | {var} | "
            f"{bugs_str} | {ar:.1%} | {ci_str} | {ttfb_str} | {us:.0f} | "
            f"{ser_rate:.1%} | {dl_rate:.1%} | {sr:.0%} |"
        )
    lines.append("")


    # Compute for each workload/iso/pg/variant pair
    fisher_lines = []
    for key, vals in sorted(agg.items()):
        wl, strat, iso, pg, var = key
        if not strat.startswith("heuristic_fuzzer"):
            continue
        # Find matching random
        rnd_key = (wl, "random_delays", iso, pg, var)
        if rnd_key not in agg:
            continue
        rnd = agg[rnd_key]
        h_bugs = vals["total_bugs"]
        h_trials = vals["total_trials"]
        r_bugs = rnd["total_bugs"]
        r_trials = rnd["total_trials"]
        p = fisher_exact_one_sided(h_bugs, h_trials - h_bugs,
                                    r_bugs, r_trials - r_bugs)
        fisher_lines.append(
            f"| {wl} | {pg} | {var} | {h_bugs}/{h_trials} ({vals['mean_anomaly_rate']:.1%}) | "
            f"{r_bugs}/{r_trials} ({rnd['mean_anomaly_rate']:.1%}) | {p:.4f} | "
            f"{'**Yes**' if p < 0.05 else 'No'} |"
        )
    if fisher_lines:
        lines.append("## Statistical Tests: Heuristic vs Random (Fisher's exact, one-sided)\n")
        lines.append("| Workload | PG | Variant | Heuristic | Random | p-value | Significant (p<0.05) |")
        lines.append("|----------|----|---------|-----------|--------|---------|----------------------|")
        lines.extend(fisher_lines)
        lines.append("")


    replay_results = [r for r in results if r.replay_success_rate is not None]
    if replay_results:
        lines.append("## Replay Verification\n")
        lines.append("Replay rates are computed per unique bug-triggering artifact "
                     "(3 replays per artifact in quick suite). Values show per-repetition "
                     "rate and min\u2013max across repetitions.\n")
        lines.append("| Workload | Strategy | PG | Reps w/ Artifact | Replay Rate (min\u2013max) | Cross-Version |")
        lines.append("|----------|----------|----|------------------|----------------------|---------------|")
        # Aggregate replay by (wl, strat, pg)
        replay_groups: Dict[tuple, List[ExperimentResult]] = defaultdict(list)
        for r in replay_results:
            replay_groups[(r.workload, r.strategy, r.pg_version)].append(r)
        for (wl, strat, pg), reps in sorted(replay_groups.items()):
            rates = [r.replay_success_rate for r in reps if r.replay_success_rate is not None]
            if not rates:
                continue
            min_r = min(rates)
            max_r = max(rates)
            cross_parts = []
            for r in reps:
                if r.found_artifact and r.found_artifact.get("cross_version"):
                    cv = r.found_artifact["cross_version"]
                    cross_parts.append("Yes" if cv.get("fixed") else
                                       f"No ({cv.get('bug_triggers', '?')}/{cv.get('replays', '?')})")
            cross = ", ".join(cross_parts) if cross_parts else "\u2014"
            rate_str = f"{min_r:.0%}" if min_r == max_r else f"{min_r:.0%}\u2013{max_r:.0%}"
            lines.append(
                f"| {wl} | {strat} | {pg} | {len(rates)} | "
                f"{rate_str} | {cross} |"
            )
        lines.append("")


    for wl_name, wl_results in sorted(by_workload.items()):
        lines.append(f"### {wl_name}\n")
        for r in wl_results:
            replay_str = ""
            if r.replay_success_rate is not None:
                replay_str = f", replay={r.replay_success_rate:.0%}"
            lines.append(f"- **{r.strategy}** (PG {r.pg_version}, {r.isolation}, {r.variant}, rep {r.repetition+1}): "
                         f"bugs={r.bugs_found}/{r.trials_run} ({r.anomaly_rate:.1%}), "
                         f"states={r.unique_states}, "
                         f"time={r.total_time_s:.1f}s{replay_str}")
        lines.append("")

    with open(path, "w") as f:
        f.write("\n".join(lines))


def _aggregate_across_reps(results: List[ExperimentResult]) -> Dict:
    """Aggregate experiment results across repetitions."""
    from collections import defaultdict
    groups = defaultdict(list)
    for r in results:
        key = (r.workload, r.strategy, r.isolation, r.pg_version, r.variant)
        groups[key].append(r)

    agg = {}
    for key, reps in groups.items():
        bugs_found_any = [1 if r.bugs_found > 0 else 0 for r in reps]
        anomaly_rates = [r.anomaly_rate for r in reps]
        ttfbs = [r.first_bug_time_s for r in reps if r.first_bug_time_s is not None]
        ttfb_trials = [r.first_bug_trial for r in reps if r.first_bug_trial is not None]
        unique_states_list = [r.unique_states for r in reps]
        r40001 = [r.rate_40001 for r in reps]
        ser_rates = [r.serialization_fail_rate for r in reps]
        dl_rates = [r.deadlock_rate for r in reps]

        # Pool all trials for Wilson CI
        total_bugs = sum(r.bugs_found for r in reps)
        total_trials = sum(r.trials_run for r in reps)
        ci_lo, ci_hi = wilson_ci(total_bugs, total_trials)

        # Replay min/max
        replay_rates = [r.replay_success_rate for r in reps if r.replay_success_rate is not None]

        agg[key] = {
            "success_rate_within_budget": sum(bugs_found_any) / max(1, len(bugs_found_any)),
            "mean_anomaly_rate": statistics.mean(anomaly_rates) if anomaly_rates else 0,
            "total_bugs": total_bugs,
            "total_trials": total_trials,
            "ci_lo": ci_lo,
            "ci_hi": ci_hi,
            "mean_ttfb": statistics.mean(ttfbs) if ttfbs else None,
            "median_ttfb_success_only": statistics.median(ttfbs) if ttfbs else None,
            "min_ttfb": min(ttfbs) if ttfbs else None,
            "max_ttfb": max(ttfbs) if ttfbs else None,
            "median_ttfb_trial": int(statistics.median(ttfb_trials)) if ttfb_trials else None,
            "mean_unique_states": statistics.mean(unique_states_list) if unique_states_list else 0,
            "mean_40001_rate": statistics.mean(r40001) if r40001 else 0,
            "mean_serialization_fail_rate": statistics.mean(ser_rates) if ser_rates else 0,
            "mean_deadlock_rate": statistics.mean(dl_rates) if dl_rates else 0,
            "repetitions": len(reps),
            "replay_min": min(replay_rates) if replay_rates else None,
            "replay_max": max(replay_rates) if replay_rates else None,
        }
    return agg


def main():
    parser = argparse.ArgumentParser(description="tx-fuzzer experiment suite runner")
    parser.add_argument("--config", required=True, help="YAML config file path")
    parser.add_argument("--smoke", action="store_true", help="Smoke test: 1 rep, minimal trials")
    parser.add_argument("--output-dir", default=None, help="Override output directory")
    args = parser.parse_args()

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"ERROR: Config file not found: {config_path}", file=sys.stderr)
        sys.exit(1)

    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Setup output directory
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    if args.output_dir:
        output_dir = Path(args.output_dir)
    else:
        output_dir = ROOT / "results" / timestamp

    # Collect PG version info
    pg_versions_info = {}
    for ver in set(v for exp in config.get("experiments", [])
                   for v in exp.get("pg_versions", ["16.2"])):
        dsn = dsn_for_pg(ver)
        pg_versions_info[ver] = get_pg_version_string(dsn)

    print(f"\nPostgreSQL instances:")
    for ver, info in pg_versions_info.items():
        print(f"  {ver}: {info}")

    # Run suite
    results = run_suite(config, smoke=args.smoke)

    # Generate outputs
    generate_summary(results, output_dir, pg_versions_info)

    # Quick summary to stdout
    total_bugs = sum(r.bugs_found for r in results)
    total_trials = sum(r.trials_run for r in results)
    print(f"\n{'='*70}")
    print(f"  SUITE COMPLETE: {total_bugs} bugs found across {total_trials} trials")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
