"""Multi-DB evaluation suite: coverage growth, case studies, and overhead."""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import inspect
import json
import os
import random
import statistics
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

ROOT = Path(__file__).resolve().parents[2]
THIS_DIR = Path(__file__).resolve().parent
if str(THIS_DIR) not in sys.path:
    sys.path.insert(0, str(THIS_DIR))
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from multidb_db import read_db_version, wait_for_db
from multidb_workloads import (
    MICRO_WORKLOADS,
    TrialResult,
    replay_trial,
    run_case_mysql_replace_bug_trial,
    run_case_mysql_rr_internal_trial,
    run_case_postgres_update_conflict_out_trial,
    run_case_sqlite_dirty_read_trial,
    run_micro_stress_trial,
    run_micro_trial,
    sqlite_version_string,
)
from plot_coverage import plot_coverage_timeseries


@dataclass
class Profile:
    name: str
    repetitions: int
    coverage_budget_s: int
    coverage_sample_s: int
    case_budget_s: Dict[str, int]
    overhead_trials: int
    replay_count: int


QUICK_PROFILE = Profile(
    name="quick",
    repetitions=5,
    coverage_budget_s=30,
    coverage_sample_s=2,
    case_budget_s={
        "pg_update_conflict_out": 30,
        "mysql_replace_bug_76927": 60,
        "mysql_rr_internal_consistency": 30,
        "sqlite_dirty_read": 20,
    },
    overhead_trials=40,
    replay_count=3,
)

LONG_PROFILE = Profile(
    name="long",
    repetitions=5,
    coverage_budget_s=300,
    coverage_sample_s=5,
    case_budget_s={
        "pg_update_conflict_out": 300,
        "mysql_replace_bug_76927": 1200,
        "mysql_rr_internal_consistency": 300,
        "sqlite_dirty_read": 300,
    },
    overhead_trials=300,
    replay_count=5,
)


def _default_targets(output_dir: Path) -> Dict[str, Dict[str, str]]:
    sqlite_path = output_dir / "sqlite" / "multidb.sqlite"
    sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    return {
        "postgres_12_3": {
            "db": "postgres",
            "dsn": os.getenv("PG_12_3_DSN", "postgres://postgres:postgres@localhost:54323/postgres?sslmode=disable"),
            "label": "PostgreSQL 12.3",
        },
        "postgres_12_4": {
            "db": "postgres",
            "dsn": os.getenv("PG_12_4_DSN", "postgres://postgres:postgres@localhost:54324/postgres?sslmode=disable"),
            "label": "PostgreSQL 12.4",
        },
        "postgres_16_2": {
            "db": "postgres",
            "dsn": os.getenv("PG_16_2_DSN", "postgres://postgres:postgres@localhost:54326/postgres?sslmode=disable"),
            "label": "PostgreSQL 16.2",
        },
        "mysql_5_6_vulnerable": {
            "db": "mysql",
            "dsn": os.getenv("MYSQL_5_6_DSN", "mysql://root:root@127.0.0.1:33078/test"),
            "label": "MySQL 5.6 (Bug#76927 vulnerable)",
        },
        "mysql_5_7_fixed": {
            "db": "mysql",
            "dsn": os.getenv("MYSQL_5_7_DSN", "mysql://root:root@127.0.0.1:33079/test"),
            "label": "MySQL 5.7 (Bug#76927 fixed)",
        },
        "mysql_8_0_34": {
            "db": "mysql",
            "dsn": os.getenv("MYSQL_8_0_34_DSN", "mysql://root:root@127.0.0.1:33080/test"),
            "label": "MySQL 8.0.34",
        },
        "sqlite_host": {
            "db": "sqlite",
            "dsn": os.getenv("SQLITE_DSN", str(sqlite_path)),
            "label": "SQLite (host)",
        },
    }


def _write_csv(path: Path, rows: Sequence[Dict[str, Any]], fieldnames: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fieldnames})


def _write_jsonl(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, sort_keys=True, default=str) + "\n")


def _percentile(values: Sequence[float], p: float) -> Optional[float]:
    vals = sorted(values)
    if not vals:
        return None
    if len(vals) == 1:
        return vals[0]
    idx = (len(vals) - 1) * p
    lo = int(idx)
    hi = min(lo + 1, len(vals) - 1)
    frac = idx - lo
    return vals[lo] * (1.0 - frac) + vals[hi] * frac


def _run_coverage_budget(
    *,
    db: str,
    dsn: str,
    workload: str,
    strategy: str,
    seed: int,
    budget_s: int,
    sample_s: int,
    k: int = 3,
) -> List[Dict[str, Any]]:
    rng = random.Random(seed)
    t0 = time.monotonic()
    next_sample = t0
    states: set[str] = set()
    trials = 0
    bugs = 0
    rows: List[Dict[str, Any]] = []
    while True:
        elapsed = time.monotonic() - t0
        if elapsed >= budget_s:
            break
        trial_seed = seed + trials * 17 + rng.randint(0, 7)
        if strategy == "stress":
            res = run_micro_stress_trial(db=db, dsn=dsn, workload=workload, seed=trial_seed)
        else:
            res = run_micro_trial(
                db=db,
                dsn=dsn,
                workload=workload,
                strategy=strategy,
                seed=trial_seed,
                k=k,
                analysis_only=False,
            )
        trials += 1
        if res.bug_found:
            bugs += 1
        states.add(res.state_hash)
        now = time.monotonic()
        if now >= next_sample or (now - t0) >= budget_s:
            elapsed_s = now - t0
            rows.append(
                {
                    "time_ms": int(elapsed_s * 1000),
                    "cumulative_unique_states": len(states),
                    "cumulative_trials": trials,
                    "cumulative_bugs": bugs,
                    "ops_per_sec": round(trials / max(0.001, elapsed_s), 3),
                }
            )
            next_sample = now + sample_s
    if not rows:
        rows.append(
            {
                "time_ms": 0,
                "cumulative_unique_states": 0,
                "cumulative_trials": 0,
                "cumulative_bugs": 0,
                "ops_per_sec": 0.0,
            }
        )
    return rows


def run_coverage_experiments(
    *,
    profile: Profile,
    base_seed: int,
    targets: Dict[str, Dict[str, str]],
    output_dir: Path,
) -> Tuple[List[Dict[str, Any]], List[Path]]:
    rows: List[Dict[str, Any]] = []
    micro_targets = [
        ("postgres", targets["postgres_16_2"]["dsn"], targets["postgres_16_2"]["label"]),
        ("mysql", targets["mysql_8_0_34"]["dsn"], targets["mysql_8_0_34"]["label"]),
        ("sqlite", targets["sqlite_host"]["dsn"], targets["sqlite_host"]["label"]),
    ]
    for db, dsn, label in micro_targets:
        if not wait_for_db(db, dsn, timeout_s=5.0):
            print(f"[coverage] skip {db}: unavailable ({dsn})")
            continue
        version = sqlite_version_string(dsn) if db == "sqlite" else read_db_version(db, dsn)
        for workload in MICRO_WORKLOADS:
            for strategy in ("stress", "random_delays", "heuristic_fuzzer"):
                for rep in range(profile.repetitions):
                    seed = base_seed + rep * 10000
                    points = _run_coverage_budget(
                        db=db,
                        dsn=dsn,
                        workload=workload,
                        strategy=strategy,
                        seed=seed,
                        budget_s=profile.coverage_budget_s,
                        sample_s=profile.coverage_sample_s,
                        k=3,
                    )
                    for point in points:
                        rows.append(
                            {
                                "db": db,
                                "db_label": label,
                                "db_version": version,
                                "workload": workload,
                                "strategy": strategy,
                                "repetition": rep,
                                "seed": seed,
                                **point,
                            }
                        )
                    final = points[-1]
                    print(
                        f"[coverage] {db}/{workload}/{strategy}/rep{rep+1}: "
                        f"states={final['cumulative_unique_states']} trials={final['cumulative_trials']}"
                    )
    cov_csv = output_dir / "coverage_timeseries.csv"
    _write_csv(
        cov_csv,
        rows,
        [
            "db",
            "db_label",
            "db_version",
            "workload",
            "strategy",
            "repetition",
            "seed",
            "time_ms",
            "cumulative_unique_states",
            "cumulative_trials",
            "cumulative_bugs",
            "ops_per_sec",
        ],
    )
    plots_dir = output_dir / "plots"
    try:
        generated = plot_coverage_timeseries(cov_csv, plots_dir)
    except Exception as e:
        print(f"[coverage] plot generation skipped: {e}")
        generated = []
    return rows, generated


def _time_to_first_bug(
    *,
    trial_fn: Callable[..., TrialResult],
    trial_kwargs: Dict[str, Any],
    budget_s: int,
    seed: int,
) -> Dict[str, Any]:
    t0 = time.monotonic()
    trials = 0
    bugs = 0
    first_bug_time_s: Optional[float] = None
    first_bug_trial: Optional[int] = None
    artifacts: List[Dict[str, Any]] = []
    while (time.monotonic() - t0) < budget_s:
        trial_seed = seed + trials
        kwargs = dict(trial_kwargs)
        kwargs["seed"] = trial_seed
        res = trial_fn(**kwargs)
        trials += 1
        if res.bug_found:
            bugs += 1
            if first_bug_time_s is None:
                first_bug_time_s = time.monotonic() - t0
                first_bug_trial = trials
            if res.artifact and not artifacts:
                artifacts.append(dict(res.artifact))
    elapsed = time.monotonic() - t0
    anomaly_rate = bugs / max(1, trials)
    return {
        "trials": trials,
        "bugs": bugs,
        "first_bug_time_s": first_bug_time_s,
        "first_bug_trial": first_bug_trial,
        "elapsed_s": elapsed,
        "anomaly_rate": anomaly_rate,
        "artifacts": artifacts,
    }


def run_case_studies(
    *,
    profile: Profile,
    base_seed: int,
    targets: Dict[str, Dict[str, str]],
    output_dir: Path,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    case_rows: List[Dict[str, Any]] = []
    replay_rows: List[Dict[str, Any]] = []

    def run_case(
        *,
        case_name: str,
        db: str,
        dsn: str,
        target_label: str,
        trial_fn: Callable[..., TrialResult],
        trial_kwargs_base: Dict[str, Any],
        strategies: Sequence[str],
        control: bool = False,
    ) -> None:
        if not wait_for_db(db, dsn, timeout_s=5.0):
            print(f"[case] skip {case_name} on {target_label}: unavailable")
            return
        for strategy in strategies:
            for rep in range(profile.repetitions):
                seed = base_seed + rep * 10000
                ttf = _time_to_first_bug(
                    trial_fn=trial_fn,
                    trial_kwargs={**trial_kwargs_base, "strategy": strategy},
                    budget_s=profile.case_budget_s[case_name],
                    seed=seed,
                )
                row = {
                    "case_name": case_name,
                    "db": db,
                    "target": target_label,
                    "strategy": strategy,
                    "repetition": rep,
                    "seed": seed,
                    "budget_s": profile.case_budget_s[case_name],
                    "trials": ttf["trials"],
                    "bugs": ttf["bugs"],
                    "anomaly_rate": round(ttf["anomaly_rate"], 6),
                    "first_bug_time_s": round(ttf["first_bug_time_s"], 6) if ttf["first_bug_time_s"] is not None else "",
                    "first_bug_trial": ttf["first_bug_trial"] or "",
                    "control": "yes" if control else "no",
                }
                case_rows.append(row)
                print(
                    f"[case] {case_name}/{target_label}/{strategy}/rep{rep+1}: "
                    f"bugs={row['bugs']} first={row['first_bug_time_s'] or '-'}"
                )

                # Replay verification for unique artifacts in quick/long.
                if strategy == "stress":
                    continue
                seen = set()
                for artifact in ttf["artifacts"]:
                    marker = json.dumps(artifact, sort_keys=True, default=str)
                    if marker in seen:
                        continue
                    seen.add(marker)
                    replay_kwargs = {**trial_kwargs_base, "strategy": strategy, "seed": seed}
                    accepted = set(inspect.signature(trial_fn).parameters.keys())
                    filtered_artifact = {k: v for k, v in artifact.items() if k in accepted}
                    replay_res = replay_trial(
                        lambda **kw: trial_fn(**{**kw, **filtered_artifact}),
                        replay_kwargs,
                        profile.replay_count,
                    )
                    replay_rows.append(
                        {
                            "case_name": case_name,
                            "db": db,
                            "target": target_label,
                            "strategy": strategy,
                            "repetition": rep,
                            "artifact": marker,
                            "replays": replay_res["replays"],
                            "successes": replay_res["successes"],
                            "rate": round(replay_res["rate"], 6),
                        }
                    )

    # A) Postgres update_conflict_out (12.3 vulnerable, 12.4 control)
    run_case(
        case_name="pg_update_conflict_out",
        db="postgres",
        dsn=targets["postgres_12_3"]["dsn"],
        target_label=targets["postgres_12_3"]["label"],
        trial_fn=run_case_postgres_update_conflict_out_trial,
        trial_kwargs_base={"dsn": targets["postgres_12_3"]["dsn"], "variant": "update", "k": 3},
        strategies=["stress", "random_delays", "heuristic_fuzzer"],
        control=False,
    )
    run_case(
        case_name="pg_update_conflict_out",
        db="postgres",
        dsn=targets["postgres_12_4"]["dsn"],
        target_label=targets["postgres_12_4"]["label"],
        trial_fn=run_case_postgres_update_conflict_out_trial,
        trial_kwargs_base={"dsn": targets["postgres_12_4"]["dsn"], "variant": "update", "k": 3},
        strategies=["stress", "random_delays", "heuristic_fuzzer"],
        control=True,
    )

    # B) MySQL bug #76927 (5.6 vulnerable, 5.7 fixed)
    run_case(
        case_name="mysql_replace_bug_76927",
        db="mysql",
        dsn=targets["mysql_5_6_vulnerable"]["dsn"],
        target_label=targets["mysql_5_6_vulnerable"]["label"],
        trial_fn=run_case_mysql_replace_bug_trial,
        trial_kwargs_base={"dsn": targets["mysql_5_6_vulnerable"]["dsn"], "k": 3},
        strategies=["stress", "random_delays", "heuristic_fuzzer"],
        control=False,
    )
    run_case(
        case_name="mysql_replace_bug_76927",
        db="mysql",
        dsn=targets["mysql_5_7_fixed"]["dsn"],
        target_label=targets["mysql_5_7_fixed"]["label"],
        trial_fn=run_case_mysql_replace_bug_trial,
        trial_kwargs_base={"dsn": targets["mysql_5_7_fixed"]["dsn"], "k": 3},
        strategies=["stress", "random_delays", "heuristic_fuzzer"],
        control=True,
    )

    # C) MySQL RR anomaly on 8.0.34 + fixed variant
    run_case(
        case_name="mysql_rr_internal_consistency",
        db="mysql",
        dsn=targets["mysql_8_0_34"]["dsn"],
        target_label=f"{targets['mysql_8_0_34']['label']} (buggy)",
        trial_fn=run_case_mysql_rr_internal_trial,
        trial_kwargs_base={"dsn": targets["mysql_8_0_34"]["dsn"], "k": 3, "fixed": False},
        strategies=["stress", "random_delays", "heuristic_fuzzer"],
        control=False,
    )
    run_case(
        case_name="mysql_rr_internal_consistency",
        db="mysql",
        dsn=targets["mysql_8_0_34"]["dsn"],
        target_label=f"{targets['mysql_8_0_34']['label']} (fixed)",
        trial_fn=run_case_mysql_rr_internal_trial,
        trial_kwargs_base={"dsn": targets["mysql_8_0_34"]["dsn"], "k": 3, "fixed": True},
        strategies=["stress", "random_delays", "heuristic_fuzzer"],
        control=True,
    )

    # D) SQLite dirty read + controls
    sqlite_shared_dsn = f"file:{output_dir / 'sqlite' / 'dirty_read.db'}?cache=shared"
    run_case(
        case_name="sqlite_dirty_read",
        db="sqlite",
        dsn=sqlite_shared_dsn,
        target_label="SQLite shared-cache + read_uncommitted=1",
        trial_fn=run_case_sqlite_dirty_read_trial,
        trial_kwargs_base={"sqlite_dsn": sqlite_shared_dsn, "k": 3, "read_uncommitted": True, "shared_cache_expected": True},
        strategies=["stress", "random_delays", "heuristic_fuzzer"],
        control=False,
    )
    sqlite_control_dsn = str(output_dir / "sqlite" / "dirty_read_control.db")
    run_case(
        case_name="sqlite_dirty_read",
        db="sqlite",
        dsn=sqlite_control_dsn,
        target_label="SQLite control read_uncommitted=0",
        trial_fn=run_case_sqlite_dirty_read_trial,
        trial_kwargs_base={"sqlite_dsn": sqlite_control_dsn, "k": 3, "read_uncommitted": False, "shared_cache_expected": False},
        strategies=["stress", "random_delays", "heuristic_fuzzer"],
        control=True,
    )

    return case_rows, replay_rows


def run_overhead(
    *,
    profile: Profile,
    base_seed: int,
    targets: Dict[str, Dict[str, str]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    db_targets = [
        ("postgres", targets["postgres_16_2"]["dsn"], targets["postgres_16_2"]["label"]),
        ("mysql", targets["mysql_8_0_34"]["dsn"], targets["mysql_8_0_34"]["label"]),
        ("sqlite", targets["sqlite_host"]["dsn"], targets["sqlite_host"]["label"]),
    ]
    for db, dsn, label in db_targets:
        if not wait_for_db(db, dsn, timeout_s=5.0):
            print(f"[overhead] skip {db}: unavailable")
            continue
        for mode in ("baseline", "analysis_only", "full_fuzzing"):
            latencies: List[float] = []
            rts: List[int] = []
            t0 = time.monotonic()
            for i in range(profile.overhead_trials):
                seed = base_seed + i * 13
                if mode == "baseline":
                    res = run_micro_trial(
                        db=db,
                        dsn=dsn,
                        workload="list_append_micro",
                        strategy="baseline",
                        seed=seed,
                        k=0,
                        analysis_only=False,
                    )
                elif mode == "analysis_only":
                    res = run_micro_trial(
                        db=db,
                        dsn=dsn,
                        workload="list_append_micro",
                        strategy="heuristic_fuzzer",
                        seed=seed,
                        k=3,
                        analysis_only=True,
                    )
                else:
                    res = run_micro_trial(
                        db=db,
                        dsn=dsn,
                        workload="list_append_micro",
                        strategy="heuristic_fuzzer",
                        seed=seed,
                        k=3,
                        analysis_only=False,
                    )
                latencies.append(res.elapsed_s)
                rts.append(res.round_trips)
            elapsed = time.monotonic() - t0
            rows.append(
                {
                    "db": db,
                    "target": label,
                    "mode": mode,
                    "trials": profile.overhead_trials,
                    "ops_per_sec": round(profile.overhead_trials / max(0.001, elapsed), 3),
                    "avg_latency_ms": round(1000.0 * statistics.mean(latencies), 3),
                    "median_latency_ms": round(1000.0 * statistics.median(latencies), 3),
                    "wall_clock_s": round(elapsed, 3),
                    "avg_round_trips": round(statistics.mean(rts), 3),
                }
            )
            print(
                f"[overhead] {db}/{mode}: "
                f"ops={rows[-1]['ops_per_sec']} med_ms={rows[-1]['median_latency_ms']}"
            )
    return rows


def _aggregate_case_rows(case_rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[Tuple[str, str, str, str], List[Dict[str, Any]]] = defaultdict(list)
    for row in case_rows:
        key = (row["case_name"], row["target"], row["strategy"], row["control"])
        grouped[key].append(row)
    out: List[Dict[str, Any]] = []
    for (case_name, target, strategy, control), rows in sorted(grouped.items()):
        ttfb = [float(r["first_bug_time_s"]) for r in rows if str(r["first_bug_time_s"]).strip() != ""]
        anomaly_rates = [float(r["anomaly_rate"]) for r in rows]
        out.append(
            {
                "case_name": case_name,
                "target": target,
                "strategy": strategy,
                "control": control,
                "repetitions": len(rows),
                "median_ttfb_s": round(statistics.median(ttfb), 4) if ttfb else "",
                "p25_ttfb_s": round(_percentile(ttfb, 0.25), 4) if ttfb else "",
                "p75_ttfb_s": round(_percentile(ttfb, 0.75), 4) if ttfb else "",
                "mean_anomaly_rate": round(statistics.mean(anomaly_rates), 6),
            }
        )
    return out


def _final_coverage_summary(rows: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[Tuple[str, str, str, int], Dict[str, Any]] = {}
    for row in rows:
        key = (row["db"], row["workload"], row["strategy"], int(row["repetition"]))
        prev = grouped.get(key)
        if prev is None or int(row["time_ms"]) >= int(prev["time_ms"]):
            grouped[key] = row
    out: List[Dict[str, Any]] = []
    merged: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = defaultdict(list)
    for (db, workload, strategy, _rep), row in grouped.items():
        merged[(db, workload, strategy)].append(row)
    for (db, workload, strategy), reps in sorted(merged.items()):
        out.append(
            {
                "db": db,
                "workload": workload,
                "strategy": strategy,
                "repetitions": len(reps),
                "mean_final_states": round(statistics.mean(float(r["cumulative_unique_states"]) for r in reps), 4),
                "mean_final_trials": round(statistics.mean(float(r["cumulative_trials"]) for r in reps), 4),
                "mean_final_bugs": round(statistics.mean(float(r["cumulative_bugs"]) for r in reps), 4),
            }
        )
    return out


def write_reports(
    *,
    output_dir: Path,
    profile: Profile,
    targets: Dict[str, Dict[str, str]],
    coverage_rows: List[Dict[str, Any]],
    plot_paths: List[Path],
    case_rows: List[Dict[str, Any]],
    replay_rows: List[Dict[str, Any]],
    overhead_rows: List[Dict[str, Any]],
) -> None:
    coverage_summary = _final_coverage_summary(coverage_rows)
    case_summary = _aggregate_case_rows(case_rows)

    # Machine-readable master files
    summary_rows: List[Dict[str, Any]] = []
    for row in coverage_summary:
        summary_rows.append({"section": "coverage", **row})
    for row in case_summary:
        summary_rows.append({"section": "case_study", **row})
    for row in overhead_rows:
        summary_rows.append({"section": "overhead", **row})

    _write_csv(output_dir / "summary.csv", summary_rows, sorted({k for row in summary_rows for k in row.keys()}))
    _write_jsonl(output_dir / "summary.jsonl", summary_rows)
    _write_csv(
        output_dir / "case_studies.csv",
        case_rows,
        [
            "case_name",
            "db",
            "target",
            "strategy",
            "repetition",
            "seed",
            "budget_s",
            "trials",
            "bugs",
            "anomaly_rate",
            "first_bug_time_s",
            "first_bug_trial",
            "control",
        ],
    )
    _write_csv(
        output_dir / "case_studies_summary.csv",
        case_summary,
        [
            "case_name",
            "target",
            "strategy",
            "control",
            "repetitions",
            "median_ttfb_s",
            "p25_ttfb_s",
            "p75_ttfb_s",
            "mean_anomaly_rate",
        ],
    )
    _write_csv(
        output_dir / "replay_summary.csv",
        replay_rows,
        [
            "case_name",
            "db",
            "target",
            "strategy",
            "repetition",
            "artifact",
            "replays",
            "successes",
            "rate",
        ],
    )
    _write_csv(
        output_dir / "overhead.csv",
        overhead_rows,
        [
            "db",
            "target",
            "mode",
            "trials",
            "ops_per_sec",
            "avg_latency_ms",
            "median_latency_ms",
            "wall_clock_s",
            "avg_round_trips",
        ],
    )

    md_summary = output_dir / "summary.md"
    lines: List[str] = []
    lines.append("# Multi-DB Quick Summary")
    lines.append("")
    lines.append(f"- Generated: {dt.datetime.now(dt.timezone.utc).isoformat()}")
    lines.append(f"- Profile: `{profile.name}`")
    lines.append("")
    lines.append("## DB Versions")
    for key, item in targets.items():
        try:
            version = sqlite_version_string(item["dsn"]) if item["db"] == "sqlite" else read_db_version(item["db"], item["dsn"])
        except Exception as e:
            version = f"unavailable ({e})"
        lines.append(f"- `{key}` ({item['label']}): `{version}`")
    lines.append("")
    lines.append("## Coverage Final (Mean Across Repetitions)")
    lines.append("| DB | Workload | Strategy | Mean Final States | Mean Trials | Mean Bugs |")
    lines.append("|----|----------|----------|-------------------|-------------|-----------|")
    for row in coverage_summary:
        lines.append(
            f"| {row['db']} | {row['workload']} | {row['strategy']} | "
            f"{row['mean_final_states']} | {row['mean_final_trials']} | {row['mean_final_bugs']} |"
        )
    lines.append("")
    lines.append("## Case Study TTFB")
    lines.append("| Case | Target | Strategy | Control | Median TTFB (s) | p25 | p75 | Mean Anomaly Rate |")
    lines.append("|------|--------|----------|---------|-----------------|-----|-----|-------------------|")
    for row in case_summary:
        lines.append(
            f"| {row['case_name']} | {row['target']} | {row['strategy']} | {row['control']} | "
            f"{row['median_ttfb_s'] or 'NA'} | {row['p25_ttfb_s'] or 'NA'} | {row['p75_ttfb_s'] or 'NA'} | "
            f"{row['mean_anomaly_rate']} |"
        )
    lines.append("")
    lines.append("## Overhead")
    lines.append("| DB | Mode | Ops/s | Avg Latency (ms) | Median Latency (ms) | Avg Round Trips |")
    lines.append("|----|------|-------|------------------|---------------------|-----------------|")
    for row in overhead_rows:
        lines.append(
            f"| {row['db']} | {row['mode']} | {row['ops_per_sec']} | "
            f"{row['avg_latency_ms']} | {row['median_latency_ms']} | {row['avg_round_trips']} |"
        )
    lines.append("")
    lines.append("## Replay Verification")
    lines.append("| Case | Target | Strategy | Replays | Successes | Rate |")
    lines.append("|------|--------|----------|---------|-----------|------|")
    for row in replay_rows:
        lines.append(
            f"| {row['case_name']} | {row['target']} | {row['strategy']} | "
            f"{row['replays']} | {row['successes']} | {row['rate']} |"
        )
    lines.append("")
    lines.append("## Coverage Plots")
    for p in sorted(plot_paths):
        rel = p.relative_to(output_dir)
        lines.append(f"- ![{rel}]({rel.as_posix()})")
    md_summary.write_text("\n".join(lines) + "\n", encoding="utf-8")

    overhead_md = output_dir / "overhead.md"
    overhead_lines = [
        "# Overhead",
        "",
        "| DB | Mode | Trials | Ops/s | Avg Latency (ms) | Median Latency (ms) | Wall Clock (s) | Avg Round Trips |",
        "|----|------|--------|-------|------------------|---------------------|----------------|-----------------|",
    ]
    for row in overhead_rows:
        overhead_lines.append(
            f"| {row['db']} | {row['mode']} | {row['trials']} | {row['ops_per_sec']} | "
            f"{row['avg_latency_ms']} | {row['median_latency_ms']} | {row['wall_clock_s']} | {row['avg_round_trips']} |"
        )
    overhead_md.write_text("\n".join(overhead_lines) + "\n", encoding="utf-8")

    report_md = output_dir / "report.md"
    report_lines: List[str] = []
    report_lines.append("# Multi-DB Evaluation Report")
    report_lines.append("")
    report_lines.append("## Scope")
    report_lines.append("- Micro suite coverage growth: Postgres + MySQL + SQLite, stress vs random delays vs heuristic fuzzing.")
    report_lines.append("- Case studies: Postgres update-conflict-out, MySQL Bug #76927 pair, MySQL RR internal consistency, SQLite dirty read control pair.")
    report_lines.append("- Overhead: baseline vs analysis-only vs full fuzzing.")
    report_lines.append("")

    # Case study role classification
    HEADLINE_CASES = {"pg_update_conflict_out", "mysql_replace_bug_76927"}
    SANITY_CASES = {"mysql_rr_internal_consistency", "sqlite_dirty_read"}

    report_lines.append("## Case Study Roles")
    report_lines.append("")
    report_lines.append("| Case | Role | Notes |")
    report_lines.append("|------|------|-------|")
    report_lines.append("| `pg_update_conflict_out` | **HEADLINE** | Historical Postgres SSI bug (12.3 vs 12.4) |")
    report_lines.append("| `mysql_replace_bug_76927` | **HEADLINE** | Historical InnoDB bug: duplicate unique key via REPLACE (5.6 vs 5.7) |")
    report_lines.append("| `mysql_rr_internal_consistency` | SANITY | Non-repeatable read under RR — harness validation |")
    report_lines.append("| `sqlite_dirty_read` | SANITY | Shared-cache dirty read — harness validation |")
    report_lines.append("")

    report_lines.append("## Coverage")
    for p in sorted(plot_paths):
        rel = p.relative_to(output_dir)
        report_lines.append(f"### {rel.stem}")
        report_lines.append(f"![{rel}]({rel.as_posix()})")
        report_lines.append("")
    report_lines.append("## Case Study Table")
    report_lines.append("| Case | Target | Strategy | Control | Median TTFB (s) | Mean Anomaly Rate |")
    report_lines.append("|------|--------|----------|---------|-----------------|-------------------|")
    for row in case_summary:
        report_lines.append(
            f"| {row['case_name']} | {row['target']} | {row['strategy']} | {row['control']} | "
            f"{row['median_ttfb_s'] or 'NA'} | {row['mean_anomaly_rate']} |"
        )
    report_lines.append("")
    report_lines.append("## Overhead Table")
    report_lines.append("| DB | Mode | Ops/s | Median Latency (ms) | Avg Round Trips |")
    report_lines.append("|----|------|-------|---------------------|-----------------|")
    for row in overhead_rows:
        report_lines.append(
            f"| {row['db']} | {row['mode']} | {row['ops_per_sec']} | {row['median_latency_ms']} | {row['avg_round_trips']} |"
        )
    report_lines.append("")
    report_lines.append("## Replay Success")
    report_lines.append("| Case | Target | Strategy | Replays | Successes | Rate |")
    report_lines.append("|------|--------|----------|---------|-----------|------|")
    for row in replay_rows:
        report_lines.append(
            f"| {row['case_name']} | {row['target']} | {row['strategy']} | {row['replays']} | {row['successes']} | {row['rate']} |"
        )
    report_lines.append("")
    report_lines.append("## Deadlocks vs Silent Anomalies")
    report_lines.append("")
    report_lines.append("Deadlocks (e.g., PostgreSQL 40P01, MySQL 1213) are **retriable transient errors**,")
    report_lines.append("not silent correctness violations. An application that retries on deadlock")
    report_lines.append("will eventually succeed with a correct outcome.")
    report_lines.append("")
    report_lines.append("In contrast, write skew and unique-key violations are **silent anomalies**:")
    report_lines.append("the transaction commits successfully but the database ends up in a state")
    report_lines.append("that violates an application invariant. No error is reported to the client.")
    report_lines.append("")
    report_lines.append("Our `doctors_write_skew` fixed variant (`fixed_ordered_locks`) avoids deadlocks")
    report_lines.append("entirely via consistent lock acquisition order (always lock doctor_id=1 then 2),")
    report_lines.append("while still preventing write skew. The older naive `SELECT … FOR UPDATE` variant")
    report_lines.append("prevented write skew but introduced ~100% deadlock rate due to inconsistent")
    report_lines.append("lock ordering between sessions.")
    report_lines.append("")
    report_md.write_text("\n".join(report_lines) + "\n", encoding="utf-8")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run multi-DB evaluation suite.")
    p.add_argument("--profile", choices=["quick", "long"], default="quick")
    p.add_argument("--smoke", action="store_true", help="Fast smoke mode: 1 repetition + tiny budgets.")
    p.add_argument("--output-dir", default=None, help="Output directory (default: results/multidb_<profile>)")
    p.add_argument("--base-seed", type=int, default=42)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    profile = QUICK_PROFILE if args.profile == "quick" else LONG_PROFILE
    if args.smoke:
        profile = Profile(
            name=f"{profile.name}-smoke",
            repetitions=1,
            coverage_budget_s=6,
            coverage_sample_s=1,
            case_budget_s={k: min(8, v) for k, v in profile.case_budget_s.items()},
            overhead_trials=8,
            replay_count=3 if args.profile == "quick" else 5,
        )

    output_dir = Path(args.output_dir) if args.output_dir else ROOT / "results" / f"multidb_{args.profile}"
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"[suite] output_dir={output_dir}")
    print(f"[suite] profile={profile.name} reps={profile.repetitions} base_seed={args.base_seed}")

    targets = _default_targets(output_dir)
    try:
        sqlite_ver = sqlite_version_string(targets["sqlite_host"]["dsn"])
        print(f"[suite] sqlite runtime version: {sqlite_ver}")
    except Exception as e:
        print(f"[suite] sqlite runtime version unavailable: {e}")

    coverage_rows, plot_paths = run_coverage_experiments(
        profile=profile,
        base_seed=args.base_seed,
        targets=targets,
        output_dir=output_dir,
    )
    case_rows, replay_rows = run_case_studies(
        profile=profile,
        base_seed=args.base_seed,
        targets=targets,
        output_dir=output_dir,
    )
    overhead_rows = run_overhead(
        profile=profile,
        base_seed=args.base_seed,
        targets=targets,
    )

    write_reports(
        output_dir=output_dir,
        profile=profile,
        targets=targets,
        coverage_rows=coverage_rows,
        plot_paths=plot_paths,
        case_rows=case_rows,
        replay_rows=replay_rows,
        overhead_rows=overhead_rows,
    )
    print(f"[suite] complete: {output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
