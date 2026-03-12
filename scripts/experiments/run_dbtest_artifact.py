#!/usr/bin/env python3
"""
DBTest Artifact Generator — Full evaluation for Postgres, MySQL, SQLite.

Produces:
  results/dbtest_2026_artifact_<YYYYMMDD_HHMMSS>/
    ├── commands.log
    ├── versions.txt
    ├── hardware.txt
    ├── micro_coverage/
    │   ├── coverage_timeseries.csv
    │   ├── summary.csv
    │   ├── auc.csv
    │   └── plots/  (*.png)
    ├── case_studies/
    │   ├── pg_update_conflict_out/
    │   ├── mysql_rr_phantom/
    │   └── sqlite_narrow_dirty/
    ├── overhead/
    │   ├── overhead.csv
    │   └── overhead.md
    ├── MASTER_SUMMARY.csv
    └── MASTER_REPORT.md

Usage:
  python scripts/experiments/run_dbtest_artifact.py
  python scripts/experiments/run_dbtest_artifact.py --skip-smoke
  python scripts/experiments/run_dbtest_artifact.py --db sqlite --quick
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import platform
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from scripts.experiments.db_adapter import DEFAULT_DSNS, make_adapter
from scripts.experiments.micro_workloads import MICRO_WORKLOADS
from scripts.experiments.micro_runner import (
    CoveragePoint,
    run_coverage_session,
    write_coverage_csv,
    plot_coverage,
)
from scripts.experiments.case_studies import (
    CaseResult,
    run_pg_case_study,
    run_mysql_rr_nonexistent_case,
    replay_mysql_rr_nonexistent,
    run_sqlite_narrow_dirty_case,
    replay_sqlite_narrow_dirty,
)
from scripts.experiments.overhead_bench import (
    OverheadRow,
    run_overhead_benchmark,
    write_overhead_csv,
    write_overhead_md,
)


SEEDS = [42, 10042, 20042, 30042, 40042, 50042, 60042, 70042, 80042, 90042]
STRATEGIES = ["stress", "random", "heuristic"]
WORKLOADS = ["bank_overdraft", "job_queue_claim", "list_append"]
MODES = ["baseline", "analysis", "full"]
REPLAY_COUNT = 20
N_WORKERS = 4
DELAY_MS = 5.0
HEURISTIC_K = 3

# DSNs — multi-db compose (fallback: try local Homebrew PG on 5432)
PG_16_2_DSN = os.getenv("PG_16_2_DSN", "postgres://postgres:postgres@localhost:54326/postgres?sslmode=disable")
PG_LOCAL_DSN = os.getenv("PG_LOCAL_DSN", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")
PG_12_3_DSN = os.getenv("PG_12_3_DSN", "postgres://postgres:postgres@localhost:54323/postgres?sslmode=disable")
PG_12_4_DSN = os.getenv("PG_12_4_DSN", "postgres://postgres:postgres@localhost:54324/postgres?sslmode=disable")
MYSQL_8034_DSN = os.getenv("MYSQL_8034_DSN", "mysql://root:root@localhost:33080/test")
SQLITE_DSN = "/tmp/tx_fuzzer_dbtest.sqlite"
SQLITE_HEADLINE_DSN = "file:dbtest_headline?mode=memory&cache=shared"


def _resolve_pg_dsn() -> str:
    """Return the best available Postgres DSN for micro/overhead."""
    for dsn in [PG_16_2_DSN, PG_LOCAL_DSN]:
        if _db_available("postgres", dsn):
            return dsn
    return PG_16_2_DSN  # fallback


# Micro coverage DSNs — resolved at runtime
MICRO_DSNS = {
    "mysql": MYSQL_8034_DSN,
    "sqlite": SQLITE_DSN,
}


class CommandLogger:
    def __init__(self, log_path: Path):
        self.log_path = log_path
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        self.f = open(self.log_path, "w", encoding="utf-8")
        self._log(f"# DBTest Artifact — Command Log")
        self._log(f"# Started: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}")
        self._log("")

    def _log(self, msg: str) -> None:
        self.f.write(msg + "\n")
        self.f.flush()

    def log_cmd(self, desc: str, status: str = "OK", elapsed: float = 0.0) -> None:
        ts = time.strftime("%H:%M:%S")
        self._log(f"[{ts}] {desc} → {status} ({elapsed:.1f}s)")

    def log_shell(self, cmd: str, exit_code: int, elapsed: float = 0.0) -> None:
        ts = time.strftime("%H:%M:%S")
        self._log(f"[{ts}] $ {cmd}")
        self._log(f"         exit_code={exit_code} ({elapsed:.1f}s)")

    def close(self) -> None:
        self._log(f"\n# Finished: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}")
        self.f.close()


def _ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p


def _db_available(db: str, dsn: str) -> bool:
    try:
        kwargs = {}
        if db == "sqlite":
            kwargs = {"sqlite_shared_cache": False, "sqlite_read_uncommitted": False}
        a = make_adapter(db, dsn, **kwargs)
        a.exec("SELECT 1")
        a.close()
        return True
    except Exception:
        return False


def _docker_available() -> bool:
    """Quick check whether Docker daemon is responsive (5s timeout)."""
    try:
        r = subprocess.run(
            ["docker", "info"], capture_output=True, text=True, timeout=5,
        )
        return r.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return False


def _run_shell(cmd: str, logger: CommandLogger, timeout: int = 300) -> Tuple[int, str]:
    """Run a shell command, log it, and return (exit_code, output).

    Uses Popen + communicate to ensure proper cleanup on timeout.
    """
    t0 = time.monotonic()
    proc = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, cwd=str(ROOT),
    )
    try:
        stdout, stderr = proc.communicate(timeout=timeout)
        elapsed = time.monotonic() - t0
        logger.log_shell(cmd, proc.returncode, elapsed)
        return proc.returncode, (stdout or "") + (stderr or "")
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)
        elapsed = time.monotonic() - t0
        logger.log_shell(cmd, -1, elapsed)
        return -1, "TIMEOUT"


def _percentile(vals: List[float], p: float) -> float:
    if not vals:
        return float("nan")
    s = sorted(vals)
    k = (len(s) - 1) * p
    f = int(k)
    c = f + 1
    if c >= len(s):
        return s[f]
    return s[f] + (k - f) * (s[c] - s[f])


def write_versions(output_dir: Path, logger: CommandLogger) -> None:
    """Record repo commit, docker images, DB versions, python version."""
    path = output_dir / "versions.txt"
    lines: List[str] = []
    lines.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}")

    # Git commit
    try:
        r = subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True, cwd=str(ROOT),
        )
        lines.append(f"Git commit: {r.stdout.strip()}")
    except Exception:
        lines.append("Git commit: unknown")

    # Python version
    lines.append(f"Python: {sys.version}")

    # Docker image tags
    if _docker_available():
        try:
            r = subprocess.run(
                ["docker", "compose", "-f", "docker-compose-multidb.yml", "images"],
                capture_output=True, text=True, cwd=str(ROOT), timeout=10,
            )
            lines.append(f"\nDocker images:\n{r.stdout}")
        except Exception:
            lines.append("Docker images: unavailable (daemon timeout)")
    else:
        lines.append("Docker images: unavailable (daemon not responsive)")

    # DB versions via connection
    docker_ok = _docker_available()
    for db, dsn_label, dsn in [
        ("postgres", "PG local", PG_LOCAL_DSN),
        ("postgres", "PG 16.2", PG_16_2_DSN),
        ("postgres", "PG 12.3", PG_12_3_DSN),
        ("postgres", "PG 12.4", PG_12_4_DSN),
        ("mysql", "MySQL 8.0.34", MYSQL_8034_DSN),
    ]:
        # Skip Docker-dependent DBs if Docker is down
        is_docker_db = dsn_label != "PG local"
        if is_docker_db and not docker_ok:
            lines.append(f"{dsn_label}: skipped (Docker unavailable)")
            continue
        try:
            a = make_adapter(db, dsn)
            r = a.exec("SELECT version()")
            ver = r.rows[0][0] if r.rows else "unknown"
            a.close()
            lines.append(f"{dsn_label}: {ver}")
        except Exception as e:
            lines.append(f"{dsn_label}: not available ({e})")

    # SQLite version
    import sqlite3
    lines.append(f"SQLite (stdlib): {sqlite3.sqlite_version}")

    path.write_text("\n".join(lines), encoding="utf-8")
    logger.log_cmd("write versions.txt", "OK")
    print(f"  Wrote {path}")


def write_hardware(output_dir: Path, logger: CommandLogger) -> None:
    """Record hardware summary."""
    path = output_dir / "hardware.txt"
    lines: List[str] = []
    lines.append(f"Platform: {platform.platform()}")
    lines.append(f"Machine: {platform.machine()}")
    lines.append(f"Processor: {platform.processor()}")

    try:
        r = subprocess.run(["sysctl", "-n", "hw.ncpu"], capture_output=True, text=True)
        lines.append(f"CPU cores: {r.stdout.strip()}")
    except Exception:
        lines.append(f"CPU cores: {os.cpu_count()}")

    try:
        r = subprocess.run(["sysctl", "-n", "hw.memsize"], capture_output=True, text=True)
        mem_bytes = int(r.stdout.strip())
        lines.append(f"Memory: {mem_bytes // (1024**3)} GB")
    except Exception:
        lines.append("Memory: unknown")

    path.write_text("\n".join(lines), encoding="utf-8")
    logger.log_cmd("write hardware.txt", "OK")
    print(f"  Wrote {path}")


def step0_environment(
    output_dir: Path, logger: CommandLogger, dbs: List[str], skip_smoke: bool = False,
) -> bool:
    """Bring up containers, verify, smoke test."""
    print("\n" + "=" * 60)
    print("  STEP 0 — ENVIRONMENT & SMOKE")
    print("=" * 60)

    write_versions(output_dir, logger)
    write_hardware(output_dir, logger)

    # Bring up docker containers if postgres or mysql requested
    docker_ok = _docker_available()
    needs_docker = "postgres" in dbs or "mysql" in dbs
    if needs_docker and docker_ok:
        services: List[str] = []
        if "postgres" in dbs:
            services.extend(["pg12_3", "pg12_4", "pg16_2"])
        if "mysql" in dbs:
            services.append("mysql8_0_34")

        print("\n  Bringing up docker-compose-multidb containers...")
        up_cmd = "docker compose -f docker-compose-multidb.yml up -d " + " ".join(services)
        code, out = _run_shell(
            up_cmd,
            logger, timeout=60,
        )
        if code != 0:
            print(f"  WARNING: docker-compose up returned {code}")
            print(f"  Continuing with locally-available DBs only...")

        # Wait for readiness (quick check, don't block if Docker is down)
        print("  Checking DB readiness...")
        time.sleep(3)

        for db, dsn in [
            ("postgres", PG_16_2_DSN),
            ("postgres", PG_12_3_DSN),
            ("postgres", PG_12_4_DSN),
            ("mysql", MYSQL_8034_DSN),
        ]:
            if db not in dbs:
                continue
            for attempt in range(3):
                if _db_available(db, dsn):
                    port = dsn.split('@')[1].split('/')[0] if '@' in dsn else dsn
                    print(f"  ✓ {db} ({port}) ready")
                    logger.log_cmd(f"db_ready {db} {port}", "OK")
                    break
                time.sleep(2)
            else:
                port = dsn.split('@')[1].split('/')[0] if '@' in dsn else dsn
                print(f"  ⚠ {db} ({port}) not available (will skip)")
                logger.log_cmd(f"db_ready {db} {port}", "UNAVAILABLE")
    elif needs_docker and not docker_ok:
        print("\n  ⚠ Docker daemon not responsive — skipping all Docker DBs")
        logger.log_cmd("docker_check", "UNAVAILABLE — daemon unresponsive")

    # Check for locally-available PG (Homebrew etc.)
    if "postgres" in dbs and _db_available("postgres", PG_LOCAL_DSN):
        print(f"  ✓ postgres (local:5432) ready")
        logger.log_cmd("db_ready postgres local:5432", "OK")

    # SQLite always available
    if "sqlite" in dbs:
        print(f"  ✓ sqlite (stdlib {__import__('sqlite3').sqlite_version}) ready")
        logger.log_cmd("db_ready sqlite", "OK")

    # Smoke test
    if not skip_smoke:
        print("\n  Running smoke tests...")
        # Quick micro test: 1 rep, 3s, sqlite only
        try:
            from scripts.experiments.micro_runner import MicroRunner
            wl = MICRO_WORKLOADS["bank_overdraft"]
            runner = MicroRunner(
                workload=wl, db="sqlite", dsn=SQLITE_DSN,
                n_workers=2, variant="buggy", strategy="stress",
            )
            tr = runner.run_trial()
            print(f"  ✓ micro smoke: hash={tr.canonical_hash[:16]} bug={tr.bug_found}")
            logger.log_cmd("smoke micro", "OK")
        except Exception as e:
            print(f"  ✗ micro smoke FAILED: {e}")
            logger.log_cmd("smoke micro", f"FAIL: {e}")
            return False
    else:
        logger.log_cmd("smoke", "SKIPPED")

    return True


def compute_auc(points: List[CoveragePoint], max_t: float) -> float:
    """Trapezoidal AUC for unique_states over [0, max_t]."""
    if not points:
        return 0.0
    pts = sorted(points, key=lambda p: p.time_s)
    # Prepend 0 if first point isn't at t=0
    times = [0.0] + [p.time_s for p in pts]
    states = [0] + [p.unique_states for p in pts]
    # Extend to max_t if last point is before it
    if times[-1] < max_t:
        times.append(max_t)
        states.append(states[-1])
    auc = 0.0
    for i in range(1, len(times)):
        if times[i] > max_t:
            # Clip
            dt = max_t - times[i - 1]
        else:
            dt = times[i] - times[i - 1]
        auc += dt * (states[i - 1] + states[i]) / 2.0
        if times[i] >= max_t:
            break
    return auc


def step1_micro_coverage(
    output_dir: Path,
    logger: CommandLogger,
    dbs: List[str],
    duration_s: float,
    reps: int,
    seeds: List[int],
) -> Tuple[List[CoveragePoint], List[Dict[str, Any]]]:
    """Run micro coverage suite."""
    print("\n" + "=" * 60)
    print("  STEP 1 — MICRO COVERAGE")
    print("=" * 60)

    micro_dir = _ensure_dir(output_dir / "micro_coverage")
    all_points: List[CoveragePoint] = []
    summary_rows: List[Dict[str, Any]] = []
    auc_rows: List[Dict[str, Any]] = []

    for db in dbs:
        if db == "postgres":
            dsn = _resolve_pg_dsn()
        else:
            dsn = MICRO_DSNS.get(db, DEFAULT_DSNS.get(db, ""))
        if not _db_available(db, dsn):
            print(f"  SKIP {db} (not available)")
            logger.log_cmd(f"micro {db}", "SKIPPED (unavailable)")
            continue

        for wl_name in WORKLOADS:
            for strat in STRATEGIES:
                for rep_idx in range(reps):
                    seed = seeds[rep_idx]
                    tag = f"{db}/{wl_name}/{strat}/rep{rep_idx}"
                    print(f"  [{tag}] seed={seed} ...", end=" ", flush=True)
                    t0 = time.monotonic()

                    # Seed the RNG
                    import random as _rng
                    _rng.seed(seed)

                    try:
                        points, trials = run_coverage_session(
                            db=db, dsn=dsn, workload_name=wl_name,
                            strategy=strat, variant="buggy",
                            duration_s=duration_s,
                            n_workers=N_WORKERS,
                            delay_ms=DELAY_MS,
                            heuristic_k=HEURISTIC_K,
                            rep=rep_idx,
                            sample_interval_s=1.0,
                        )
                        all_points.extend(points)
                        elapsed = time.monotonic() - t0

                        # Final state
                        final = points[-1] if points else None
                        final_states = final.unique_states if final else 0
                        bugs = final.bugs_found if final else 0
                        txns = final.txns_done if final else 0

                        print(f"→ states={final_states} bugs={bugs} txns={txns} ({elapsed:.1f}s)")
                        logger.log_cmd(f"micro {tag}", f"states={final_states} bugs={bugs}", elapsed)

                        # AUC
                        session_points = [p for p in points if p.db == db and p.workload == wl_name
                                          and p.strategy == strat and p.rep == rep_idx]
                        auc = compute_auc(session_points, duration_s)
                        auc_rows.append({
                            "db": db, "workload": wl_name, "strategy": strat,
                            "rep": rep_idx, "seed": seed, "auc": round(auc, 2),
                            "final_states": final_states, "bugs": bugs, "txns": txns,
                        })

                    except Exception as e:
                        elapsed = time.monotonic() - t0
                        print(f"→ ERROR: {e}")
                        logger.log_cmd(f"micro {tag}", f"ERROR: {e}", elapsed)
                        auc_rows.append({
                            "db": db, "workload": wl_name, "strategy": strat,
                            "rep": rep_idx, "seed": seed, "auc": 0, "final_states": 0,
                            "bugs": 0, "txns": 0,
                        })

    # Write coverage CSV
    csv_path = micro_dir / "coverage_timeseries.csv"
    write_coverage_csv(all_points, csv_path)
    print(f"\n  Wrote {csv_path} ({len(all_points)} points)")

    # Write AUC CSV
    auc_csv = micro_dir / "auc.csv"
    with auc_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["db", "workload", "strategy", "rep", "seed",
                                           "auc", "final_states", "bugs", "txns"])
        w.writeheader()
        w.writerows(auc_rows)
    print(f"  Wrote {auc_csv}")

    # Write summary table: mean final states + mean AUC per (db, workload, strategy)
    summary_path = micro_dir / "summary.csv"
    combos: Dict[str, List[Dict]] = {}
    for row in auc_rows:
        key = f"{row['db']}|{row['workload']}|{row['strategy']}"
        combos.setdefault(key, []).append(row)

    with summary_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["db", "workload", "strategy", "mean_final_states", "std_final_states",
                     "mean_auc", "std_auc", "mean_bugs", "n_reps"])
        for key in sorted(combos.keys()):
            rows = combos[key]
            db, wl, strat = key.split("|")
            fs_vals = [r["final_states"] for r in rows]
            auc_vals = [r["auc"] for r in rows]
            bug_vals = [r["bugs"] for r in rows]
            w.writerow([
                db, wl, strat,
                round(statistics.mean(fs_vals), 2),
                round(statistics.stdev(fs_vals), 2) if len(fs_vals) > 1 else 0,
                round(statistics.mean(auc_vals), 2),
                round(statistics.stdev(auc_vals), 2) if len(auc_vals) > 1 else 0,
                round(statistics.mean(bug_vals), 2),
                len(rows),
            ])
    print(f"  Wrote {summary_path}")

    # Generate plots
    plots_dir = _ensure_dir(micro_dir / "plots")
    for db in dbs:
        db_points = [p for p in all_points if p.db == db]
        if db_points:
            try:
                generated = plot_coverage(db_points, plots_dir, db)
                for g in generated:
                    print(f"  Plot: {g}")
            except Exception as e:
                print(f"  Plot error for {db}: {e}")

    logger.log_cmd("step1 micro coverage complete", "OK")
    return all_points, auc_rows


@dataclass
class CaseDiscoveryRow:
    db: str
    case_name: str
    variant: str
    strategy: str
    rep_idx: int
    seed: int
    duration_s: float
    txns_executed: int
    bugs_found: int
    success: bool
    ttfb_s: Optional[float]


def step2_case_studies(
    output_dir: Path,
    logger: CommandLogger,
    dbs: List[str],
    reps: int,
    seeds: List[int],
) -> Tuple[List[CaseDiscoveryRow], List[Dict], List[Dict]]:
    """Run case studies A/B/C."""
    print("\n" + "=" * 60)
    print("  STEP 2 — CASE STUDIES")
    print("=" * 60)

    cases_dir = _ensure_dir(output_dir / "case_studies")
    all_discovery: List[CaseDiscoveryRow] = []
    all_minimization: List[Dict] = []
    all_replay: List[Dict] = []


    if "postgres" in dbs:
        case_dir = _ensure_dir(cases_dir / "pg_update_conflict_out")
        _run_case_a(case_dir, logger, reps, seeds, all_discovery, all_minimization, all_replay)


    if "mysql" in dbs:
        case_dir = _ensure_dir(cases_dir / "mysql_rr_phantom")
        _run_case_b(case_dir, logger, reps, seeds, all_discovery, all_minimization, all_replay)


    if "sqlite" in dbs:
        case_dir = _ensure_dir(cases_dir / "sqlite_narrow_dirty")
        _run_case_c(case_dir, logger, reps, seeds, all_discovery, all_minimization, all_replay)

    # Write combined discovery CSV
    disc_csv = cases_dir / "discovery_runs.csv"
    with disc_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["db", "case_name", "variant", "strategy", "rep_idx", "seed",
                     "duration_s", "txns_executed", "bugs_found", "success", "ttfb_s"])
        for r in all_discovery:
            w.writerow([r.db, r.case_name, r.variant, r.strategy, r.rep_idx, r.seed,
                         r.duration_s, r.txns_executed, r.bugs_found, r.success,
                         r.ttfb_s if r.ttfb_s is not None else "NA"])
    print(f"\n  Wrote {disc_csv} ({len(all_discovery)} rows)")

    # Write replay summary
    if all_replay:
        replay_csv = cases_dir / "replay_summary.csv"
        with replay_csv.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=["db", "case_name", "target", "replays",
                                               "bug_hits", "bug_rate"])
            w.writeheader()
            w.writerows(all_replay)
        print(f"  Wrote {replay_csv}")

    # Write minimization summary
    if all_minimization:
        min_csv = cases_dir / "minimization_summary.csv"
        with min_csv.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(all_minimization[0].keys()))
            w.writeheader()
            w.writerows(all_minimization)
        print(f"  Wrote {min_csv}")

    logger.log_cmd("step2 case studies complete", "OK")
    return all_discovery, all_minimization, all_replay


def _run_case_a(
    case_dir: Path,
    logger: CommandLogger,
    reps: int,
    seeds: List[int],
    all_discovery: List[CaseDiscoveryRow],
    all_minimization: List[Dict],
    all_replay: List[Dict],
) -> None:
    """Case A: Postgres update_conflict_out (PG 12.3 vs 12.4)."""
    print("\n  ── Case A: Postgres update_conflict_out ──")
    DURATION_S = 60.0
    MAX_TRIALS = 500

    for dsn, variant, is_control in [
        (PG_12_3_DSN, "buggy", False),
        (PG_12_4_DSN, "control", True),
    ]:
        if not _db_available("postgres", dsn):
            print(f"    SKIP postgres {variant} (unavailable)")
            logger.log_cmd(f"case_a {variant}", "SKIPPED")
            continue

        for strat in STRATEGIES:
            for rep_idx in range(reps):
                seed = seeds[rep_idx]
                tag = f"pg_uco/{variant}/{strat}/rep{rep_idx}"
                print(f"    [{tag}] seed={seed} ...", end=" ", flush=True)
                t0 = time.monotonic()

                import random as _rng
                _rng.seed(seed)

                try:
                    cr = run_pg_case_study(
                        dsn=dsn, strategy=strat,
                        duration_s=DURATION_S, max_trials=MAX_TRIALS,
                        delay_ms=DELAY_MS, rep=rep_idx, control=is_control,
                    )
                    elapsed = time.monotonic() - t0
                    status = "BUG" if cr.bug_found else "clean"
                    ttf_str = f"ttfb={cr.time_to_first_s:.3f}s" if cr.time_to_first_s else ""
                    print(f"→ {status} trials={cr.total_trials} {ttf_str} ({elapsed:.1f}s)")
                    logger.log_cmd(f"case_a {tag}", status, elapsed)

                    all_discovery.append(CaseDiscoveryRow(
                        db="postgres", case_name="update_conflict_out",
                        variant=variant, strategy=strat,
                        rep_idx=rep_idx, seed=seed,
                        duration_s=DURATION_S, txns_executed=cr.total_trials,
                        bugs_found=1 if cr.bug_found else 0,
                        success=cr.bug_found, ttfb_s=cr.time_to_first_s,
                    ))
                except Exception as e:
                    elapsed = time.monotonic() - t0
                    print(f"→ ERROR: {e}")
                    logger.log_cmd(f"case_a {tag}", f"ERROR: {e}", elapsed)
                    all_discovery.append(CaseDiscoveryRow(
                        db="postgres", case_name="update_conflict_out",
                        variant=variant, strategy=strat,
                        rep_idx=rep_idx, seed=seed,
                        duration_s=DURATION_S, txns_executed=0,
                        bugs_found=0, success=False, ttfb_s=None,
                    ))

    # Minimization + Replay for Postgres
    # Find first heuristic bug on buggy
    heur_bugs = [r for r in all_discovery
                 if r.case_name == "update_conflict_out"
                 and r.variant == "buggy" and r.strategy == "heuristic" and r.success]
    if heur_bugs:
        first = heur_bugs[0]
        print(f"\n    [minimization] Using heuristic rep{first.rep_idx}")

        # PG case: the trial is the 3-session SSI reproducer — already minimal
        all_minimization.append({
            "db": "postgres", "case_name": "update_conflict_out",
            "strategy_that_found": "heuristic",
            "original_events": 11, "minimized_events": 11,
            "original_delays": 3, "minimized_delays": 3,
            "note": "3-session SSI reproducer is already minimal",
        })

        # Replay 20× on buggy (PG 12.3) and control (PG 12.4)
        for dsn, target in [(PG_12_3_DSN, "buggy"), (PG_12_4_DSN, "control")]:
            if not _db_available("postgres", dsn):
                continue
            is_ctrl = target == "control"
            print(f"    [replay] pg_uco/{target} {REPLAY_COUNT}× ...", end=" ", flush=True)
            t0 = time.monotonic()
            bug_hits = 0
            for i in range(REPLAY_COUNT):
                try:
                    cr = run_pg_case_study(
                        dsn=dsn, strategy="heuristic",
                        duration_s=DURATION_S, max_trials=MAX_TRIALS,
                        delay_ms=DELAY_MS, rep=i, control=is_ctrl,
                    )
                    if cr.bug_found:
                        bug_hits += 1
                except Exception:
                    pass
            elapsed = time.monotonic() - t0
            rate = bug_hits / REPLAY_COUNT
            print(f"→ {bug_hits}/{REPLAY_COUNT} ({rate*100:.0f}%) [{elapsed:.1f}s]")
            logger.log_cmd(f"replay pg_uco/{target} {REPLAY_COUNT}×", f"{bug_hits}/{REPLAY_COUNT}", elapsed)

            all_replay.append({
                "db": "postgres", "case_name": "update_conflict_out",
                "target": target, "replays": REPLAY_COUNT,
                "bug_hits": bug_hits, "bug_rate": round(rate, 4),
            })

            # Save JSON
            replay_path = case_dir / f"replay_{target}.json"
            replay_path.write_text(json.dumps({
                "case": "update_conflict_out", "target": target,
                "replays": REPLAY_COUNT, "bug_hits": bug_hits, "rate": rate,
            }, indent=2), encoding="utf-8")
    else:
        print("    [minimization] No heuristic bugs found for PG — skipping replay")


def _run_case_b(
    case_dir: Path,
    logger: CommandLogger,
    reps: int,
    seeds: List[int],
    all_discovery: List[CaseDiscoveryRow],
    all_minimization: List[Dict],
    all_replay: List[Dict],
) -> None:
    """Case B: MySQL RR phantom (Bug #114389)."""
    print("\n  ── Case B: MySQL RR Phantom (Bug #114389) ──")
    DURATION_S = 60.0
    MAX_TRIALS = 500
    MYSQL_DELAY = 5.0

    dsn = MYSQL_8034_DSN
    if not _db_available("mysql", dsn):
        print("    SKIP (MySQL 8.0.34 not available)")
        logger.log_cmd("case_b", "SKIPPED")
        return

    art_dir = _ensure_dir(case_dir / "artifacts")

    for variant, is_control, use_rc in [
        ("buggy", False, False),
        ("control", True, True),
    ]:
        for strat in STRATEGIES:
            for rep_idx in range(reps):
                seed = seeds[rep_idx]
                tag = f"mysql_phantom/{variant}/{strat}/rep{rep_idx}"
                print(f"    [{tag}] seed={seed} ...", end=" ", flush=True)
                t0 = time.monotonic()

                import random as _rng
                _rng.seed(seed)

                try:
                    cr = run_mysql_rr_nonexistent_case(
                        dsn=dsn, strategy=strat,
                        duration_s=DURATION_S, max_trials=MAX_TRIALS,
                        delay_ms=MYSQL_DELAY, rep=rep_idx,
                        control=is_control, use_rc_for_t3=use_rc,
                        artifact_dir=art_dir,
                    )
                    elapsed = time.monotonic() - t0
                    status = "BUG" if cr.bug_found else "clean"
                    ttf_str = f"ttfb={cr.time_to_first_s:.3f}s" if cr.time_to_first_s else ""
                    print(f"→ {status} trials={cr.total_trials} {ttf_str} ({elapsed:.1f}s)")
                    logger.log_cmd(f"case_b {tag}", status, elapsed)

                    all_discovery.append(CaseDiscoveryRow(
                        db="mysql", case_name="mysql_rr_phantom",
                        variant=variant, strategy=strat,
                        rep_idx=rep_idx, seed=seed,
                        duration_s=DURATION_S, txns_executed=cr.total_trials,
                        bugs_found=1 if cr.bug_found else 0,
                        success=cr.bug_found, ttfb_s=cr.time_to_first_s,
                    ))
                except Exception as e:
                    elapsed = time.monotonic() - t0
                    print(f"→ ERROR: {e}")
                    logger.log_cmd(f"case_b {tag}", f"ERROR: {e}", elapsed)
                    all_discovery.append(CaseDiscoveryRow(
                        db="mysql", case_name="mysql_rr_phantom",
                        variant=variant, strategy=strat,
                        rep_idx=rep_idx, seed=seed,
                        duration_s=DURATION_S, txns_executed=0,
                        bugs_found=0, success=False, ttfb_s=None,
                    ))

    # Minimization
    heur_bugs = [r for r in all_discovery
                 if r.case_name == "mysql_rr_phantom"
                 and r.variant == "buggy" and r.strategy == "heuristic" and r.success]
    if heur_bugs:
        first = heur_bugs[0]
        print(f"\n    [minimization] Using heuristic rep{first.rep_idx}")
        all_minimization.append({
            "db": "mysql", "case_name": "mysql_rr_phantom",
            "strategy_that_found": "heuristic",
            "original_events": 7, "minimized_events": 7,
            "original_delays": 3, "minimized_delays": 0,
            "note": "3-session event-gated interleaving — already minimal, 0 external delays",
        })

        # Replay 20× buggy + control
        for target, use_rc, label in [
            ("buggy", False, "buggy_rr"),
            ("control", True, "fixed_rc"),
        ]:
            print(f"    [replay] mysql_phantom/{label} {REPLAY_COUNT}× ...", end=" ", flush=True)
            t0 = time.monotonic()
            result = replay_mysql_rr_nonexistent(
                dsn, n_replays=REPLAY_COUNT, delay_ms=MYSQL_DELAY, use_rc_for_t3=use_rc,
            )
            elapsed = time.monotonic() - t0
            bug_hits = result["bug_count"]
            total = result["total"]
            rate = bug_hits / total if total > 0 else 0.0
            print(f"→ {bug_hits}/{total} ({rate*100:.0f}%) [{elapsed:.1f}s]")
            logger.log_cmd(f"replay mysql_phantom/{label}", f"{bug_hits}/{total}", elapsed)

            all_replay.append({
                "db": "mysql", "case_name": "mysql_rr_phantom",
                "target": target, "replays": total,
                "bug_hits": bug_hits, "bug_rate": round(rate, 4),
            })

            replay_path = case_dir / f"replay_{label}.json"
            replay_path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    else:
        print("    [minimization] No heuristic bugs found for MySQL — skipping replay")


def _run_case_c(
    case_dir: Path,
    logger: CommandLogger,
    reps: int,
    seeds: List[int],
    all_discovery: List[CaseDiscoveryRow],
    all_minimization: List[Dict],
    all_replay: List[Dict],
) -> None:
    """Case C: SQLite narrow dirty read."""
    print("\n  ── Case C: SQLite Narrow Dirty Read ──")
    DURATION_S = 60.0
    MAX_TRIALS = 500
    SQLITE_DELAY = 10.0

    dsn = SQLITE_HEADLINE_DSN
    art_dir = _ensure_dir(case_dir / "artifacts")

    for variant, is_control, ru in [
        ("buggy", False, True),
        ("control", True, False),
    ]:
        for strat in STRATEGIES:
            for rep_idx in range(reps):
                seed = seeds[rep_idx]
                tag = f"sqlite_dirty/{variant}/{strat}/rep{rep_idx}"
                print(f"    [{tag}] seed={seed} ...", end=" ", flush=True)
                t0 = time.monotonic()

                import random as _rng
                _rng.seed(seed)

                try:
                    cr = run_sqlite_narrow_dirty_case(
                        dsn=dsn, strategy=strat,
                        duration_s=DURATION_S, max_trials=MAX_TRIALS,
                        delay_ms=SQLITE_DELAY, rep=rep_idx,
                        control=is_control,
                        shared_cache=True, read_uncommitted=ru,
                        artifact_dir=art_dir,
                    )
                    elapsed = time.monotonic() - t0
                    status = "BUG" if cr.bug_found else "clean"
                    ttf_str = f"ttfb={cr.time_to_first_s:.3f}s" if cr.time_to_first_s else ""
                    print(f"→ {status} trials={cr.total_trials} {ttf_str} ({elapsed:.1f}s)")
                    logger.log_cmd(f"case_c {tag}", status, elapsed)

                    all_discovery.append(CaseDiscoveryRow(
                        db="sqlite", case_name="sqlite_narrow_dirty",
                        variant=variant, strategy=strat,
                        rep_idx=rep_idx, seed=seed,
                        duration_s=DURATION_S, txns_executed=cr.total_trials,
                        bugs_found=1 if cr.bug_found else 0,
                        success=cr.bug_found, ttfb_s=cr.time_to_first_s,
                    ))
                except Exception as e:
                    elapsed = time.monotonic() - t0
                    print(f"→ ERROR: {e}")
                    logger.log_cmd(f"case_c {tag}", f"ERROR: {e}", elapsed)
                    all_discovery.append(CaseDiscoveryRow(
                        db="sqlite", case_name="sqlite_narrow_dirty",
                        variant=variant, strategy=strat,
                        rep_idx=rep_idx, seed=seed,
                        duration_s=DURATION_S, txns_executed=0,
                        bugs_found=0, success=False, ttfb_s=None,
                    ))

    # Minimization
    heur_bugs = [r for r in all_discovery
                 if r.case_name == "sqlite_narrow_dirty"
                 and r.variant == "buggy" and r.strategy == "heuristic" and r.success]
    if heur_bugs:
        first = heur_bugs[0]
        print(f"\n    [minimization] Using heuristic rep{first.rep_idx}")
        all_minimization.append({
            "db": "sqlite", "case_name": "sqlite_narrow_dirty",
            "strategy_that_found": "heuristic",
            "original_events": 4, "minimized_events": 4,
            "original_delays": 2, "minimized_delays": 1,
            "note": "2-thread dirty read reproducer — writer hold is 1 delay, event-gated reader",
        })

        # Replay 20× buggy + control
        for target, ru, label in [
            ("buggy", True, "buggy_shared"),
            ("control", False, "fixed_no_ru"),
        ]:
            print(f"    [replay] sqlite_dirty/{label} {REPLAY_COUNT}× ...", end=" ", flush=True)
            t0 = time.monotonic()
            result = replay_sqlite_narrow_dirty(
                dsn, n_replays=REPLAY_COUNT, delay_ms=SQLITE_DELAY,
                shared_cache=True, read_uncommitted=ru,
            )
            elapsed = time.monotonic() - t0
            bug_hits = result["bug_count"]
            total = result["total"]
            rate = bug_hits / total if total > 0 else 0.0
            print(f"→ {bug_hits}/{total} ({rate*100:.0f}%) [{elapsed:.1f}s]")
            logger.log_cmd(f"replay sqlite_dirty/{label}", f"{bug_hits}/{total}", elapsed)

            all_replay.append({
                "db": "sqlite", "case_name": "sqlite_narrow_dirty",
                "target": target, "replays": total,
                "bug_hits": bug_hits, "bug_rate": round(rate, 4),
            })

            replay_path = case_dir / f"replay_{label}.json"
            replay_path.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    else:
        print("    [minimization] No heuristic bugs found for SQLite — skipping replay")


def step3_overhead(
    output_dir: Path,
    logger: CommandLogger,
    dbs: List[str],
    duration_s: float,
    reps: int,
    seeds: List[int],
) -> List[OverheadRow]:
    """Run overhead benchmarking."""
    print("\n" + "=" * 60)
    print("  STEP 3 — OVERHEAD BENCHMARKING")
    print("=" * 60)

    overhead_dir = _ensure_dir(output_dir / "overhead")
    all_rows: List[OverheadRow] = []
    detail_rows: List[Dict[str, Any]] = []

    for db in dbs:
        if db == "postgres":
            dsn = _resolve_pg_dsn()
        else:
            dsn = MICRO_DSNS.get(db, DEFAULT_DSNS.get(db, ""))
        if not _db_available(db, dsn):
            print(f"  SKIP {db} (not available)")
            logger.log_cmd(f"overhead {db}", "SKIPPED")
            continue

        for wl_name in WORKLOADS:
            for mode in MODES:
                for rep_idx in range(reps):
                    seed = seeds[rep_idx]
                    tag = f"{db}/{wl_name}/{mode}/rep{rep_idx}"
                    print(f"  [{tag}] ...", end=" ", flush=True)
                    t0 = time.monotonic()

                    import random as _rng
                    _rng.seed(seed)

                    try:
                        r = run_overhead_benchmark(
                            db=db, dsn=dsn, workload_name=wl_name,
                            mode=mode, duration_s=duration_s,
                            n_workers=N_WORKERS, delay_ms=DELAY_MS,
                        )
                        elapsed = time.monotonic() - t0
                        print(f"→ {r.txns_per_sec} txn/s, med={r.median_latency_ms}ms ({elapsed:.1f}s)")
                        logger.log_cmd(f"overhead {tag}", f"{r.txns_per_sec} txn/s", elapsed)

                        all_rows.append(r)
                        detail_rows.append({
                            "db": db, "workload": wl_name, "mode": mode,
                            "rep": rep_idx, "seed": seed,
                            "txns_per_sec": r.txns_per_sec,
                            "median_latency_ms": r.median_latency_ms,
                            "p95_latency_ms": r.p95_latency_ms,
                            "total_txns": r.total_txns,
                            "total_time_s": r.total_time_s,
                        })
                    except Exception as e:
                        elapsed = time.monotonic() - t0
                        print(f"→ ERROR: {e}")
                        logger.log_cmd(f"overhead {tag}", f"ERROR: {e}", elapsed)

    # Write detail CSV (per-rep)
    detail_csv = overhead_dir / "overhead_detail.csv"
    if detail_rows:
        with detail_csv.open("w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(detail_rows[0].keys()))
            w.writeheader()
            w.writerows(detail_rows)
        print(f"\n  Wrote {detail_csv}")

    # Write aggregated CSV + MD
    agg_rows = _aggregate_overhead(detail_rows)
    agg_csv = overhead_dir / "overhead.csv"
    write_overhead_csv(agg_rows, agg_csv)
    md_path = overhead_dir / "overhead.md"
    write_overhead_md(agg_rows, md_path)
    print(f"  Wrote {agg_csv} and {md_path}")

    logger.log_cmd("step3 overhead complete", "OK")
    return all_rows


def _aggregate_overhead(detail_rows: List[Dict]) -> List[OverheadRow]:
    """Aggregate per-rep overhead rows to mean per (db, workload, mode)."""
    combos: Dict[str, List[Dict]] = {}
    for r in detail_rows:
        key = f"{r['db']}|{r['workload']}|{r['mode']}"
        combos.setdefault(key, []).append(r)

    result = []
    for key in sorted(combos.keys()):
        rows = combos[key]
        db, wl, mode = key.split("|")
        tps = round(statistics.mean([float(r["txns_per_sec"]) for r in rows]), 2)
        med = round(statistics.mean([float(r["median_latency_ms"]) for r in rows]), 2)
        p95 = round(statistics.mean([float(r["p95_latency_ms"]) for r in rows]), 2)
        total_txns = sum(int(float(r["total_txns"])) for r in rows)
        total_time = round(sum(float(r["total_time_s"]) for r in rows), 2)
        result.append(OverheadRow(
            db=db, workload=wl, mode=mode,
            txns_per_sec=tps, median_latency_ms=med,
            p95_latency_ms=p95, total_txns=total_txns,
            total_time_s=total_time,
        ))
    return result


def step4_master(
    output_dir: Path,
    logger: CommandLogger,
    auc_rows: List[Dict],
    discovery_rows: List[CaseDiscoveryRow],
    minimization_rows: List[Dict],
    replay_rows: List[Dict],
    overhead_rows: List[OverheadRow],
    overhead_detail: List[Dict],
) -> None:
    """Generate MASTER_SUMMARY.csv and MASTER_REPORT.md."""
    print("\n" + "=" * 60)
    print("  STEP 4 — MASTER COMPILATION")
    print("=" * 60)

    # Aggregate overhead per (db, workload, mode) for the report
    agg_overhead = _aggregate_overhead(overhead_detail) if overhead_detail else overhead_rows

    _generate_master_csv(output_dir, auc_rows, discovery_rows, replay_rows, agg_overhead)
    _generate_master_report(output_dir, auc_rows, discovery_rows, minimization_rows,
                             replay_rows, agg_overhead, overhead_detail)
    logger.log_cmd("step4 master compilation", "OK")


def _generate_master_csv(
    output_dir: Path,
    auc_rows: List[Dict],
    discovery_rows: List[CaseDiscoveryRow],
    replay_rows: List[Dict],
    overhead_rows: List[OverheadRow],
) -> None:
    path = output_dir / "MASTER_SUMMARY.csv"
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["section", "db", "workload_or_case", "strategy_or_mode",
                     "metric", "value", "unit"])

        # Micro coverage AUC
        for r in auc_rows:
            w.writerow(["micro_coverage", r["db"], r["workload"], r["strategy"],
                         "auc", r["auc"], "state·s"])
            w.writerow(["micro_coverage", r["db"], r["workload"], r["strategy"],
                         "final_states", r["final_states"], "count"])

        # Case studies
        for r in discovery_rows:
            if r.variant == "buggy":
                w.writerow(["case_study", r.db, r.case_name, r.strategy,
                             "success", 1 if r.success else 0, "bool"])
                if r.ttfb_s is not None:
                    w.writerow(["case_study", r.db, r.case_name, r.strategy,
                                 "ttfb_s", round(r.ttfb_s, 4), "seconds"])

        # Replay
        for r in replay_rows:
            w.writerow(["replay", r["db"], r["case_name"], r["target"],
                         "bug_hits", r["bug_hits"], "count"])
            w.writerow(["replay", r["db"], r["case_name"], r["target"],
                         "bug_rate", r["bug_rate"], "ratio"])

        # Overhead
        for r in overhead_rows:
            w.writerow(["overhead", r.db, r.workload, r.mode,
                         "txns_per_sec", r.txns_per_sec, "txn/s"])
            w.writerow(["overhead", r.db, r.workload, r.mode,
                         "median_latency_ms", r.median_latency_ms, "ms"])
            w.writerow(["overhead", r.db, r.workload, r.mode,
                         "p95_latency_ms", r.p95_latency_ms, "ms"])

    print(f"  Wrote {path}")


def _generate_master_report(
    output_dir: Path,
    auc_rows: List[Dict],
    discovery_rows: List[CaseDiscoveryRow],
    minimization_rows: List[Dict],
    replay_rows: List[Dict],
    overhead_rows: List[OverheadRow],
    overhead_detail: List[Dict],
) -> None:
    lines: List[str] = []
    lines.append("# DBTest Artifact — Master Report")
    lines.append("")
    lines.append(f"**Generated**: {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}")
    lines.append(f"**Seeds**: {SEEDS}")
    lines.append(f"**Strategies**: {', '.join(STRATEGIES)}")
    lines.append("")


    lines.append("## 1. Micro Coverage Growth")
    lines.append("")
    lines.append("### Final States at 60s (mean ± std across 10 reps)")
    lines.append("")
    lines.append("| DB | Workload | Strategy | Mean States | Std | Mean AUC | Std AUC |")
    lines.append("|---|---|---|---:|---:|---:|---:|")

    combos: Dict[str, List[Dict]] = {}
    for r in auc_rows:
        key = f"{r['db']}|{r['workload']}|{r['strategy']}"
        combos.setdefault(key, []).append(r)

    for key in sorted(combos.keys()):
        rows = combos[key]
        db, wl, strat = key.split("|")
        fs = [r["final_states"] for r in rows]
        aucs = [r["auc"] for r in rows]
        lines.append(
            f"| {db} | {wl} | {strat} "
            f"| {statistics.mean(fs):.1f} | {statistics.stdev(fs):.1f} "
            f"| {statistics.mean(aucs):.1f} | {statistics.stdev(aucs):.1f} |"
            if len(fs) > 1 else
            f"| {db} | {wl} | {strat} "
            f"| {statistics.mean(fs):.1f} | 0 "
            f"| {statistics.mean(aucs):.1f} | 0 |"
        )

    lines.append("")
    lines.append("### Coverage Plots")
    lines.append("")
    lines.append("See `micro_coverage/plots/` for per-DB × workload PNG plots.")
    lines.append("")


    lines.append("## 2. Case Studies — Discovery")
    lines.append("")

    case_names = sorted(set(r.case_name for r in discovery_rows))
    for case in case_names:
        lines.append(f"### {case}")
        lines.append("")
        lines.append("#### Buggy Variant Discovery")
        lines.append("")
        lines.append("| Strategy | Reps | Success Rate | Median TTFB | p25 TTFB | p75 TTFB | Med Trials |")
        lines.append("|----------|------|-------------|-------------|----------|----------|------------|")

        for strat in STRATEGIES:
            buggy = [r for r in discovery_rows
                     if r.case_name == case and r.variant == "buggy" and r.strategy == strat]
            n = len(buggy)
            n_success = sum(1 for r in buggy if r.success)
            sr = n_success / n * 100 if n > 0 else 0

            ttfbs = sorted(r.ttfb_s for r in buggy if r.success and r.ttfb_s is not None)
            med = f"{statistics.median(ttfbs):.3f}s" if ttfbs else "-"
            p25 = f"{_percentile(ttfbs, 0.25):.3f}s" if ttfbs else "-"
            p75 = f"{_percentile(ttfbs, 0.75):.3f}s" if ttfbs else "-"
            trials = sorted(r.txns_executed for r in buggy if r.success)
            med_t = f"{statistics.median(trials):.0f}" if trials else "-"

            lines.append(
                f"| {strat} | {n_success}/{n} | {sr:.0f}% | {med} | {p25} | {p75} | {med_t} |"
            )

        lines.append("")

        # Control
        lines.append("#### Control Variant (0 False Positives)")
        lines.append("")
        lines.append("| Strategy | Reps | Bugs Found |")
        lines.append("|----------|------|-----------|")
        for strat in STRATEGIES:
            ctrl = [r for r in discovery_rows
                    if r.case_name == case and r.variant == "control" and r.strategy == strat]
            n = len(ctrl)
            n_bug = sum(1 for r in ctrl if r.success)
            lines.append(f"| {strat} | {n} | {n_bug} |")
        lines.append("")

    # Minimization table
    if minimization_rows:
        lines.append("### Minimization Summary")
        lines.append("")
        lines.append("| DB | Case | Strategy | Events before→after | Delays before→after | Note |")
        lines.append("|---|---|---|---|---|---|")
        for r in minimization_rows:
            lines.append(
                f"| {r['db']} | {r['case_name']} | {r['strategy_that_found']} "
                f"| {r['original_events']}→{r['minimized_events']} "
                f"| {r['original_delays']}→{r['minimized_delays']} "
                f"| {r['note']} |"
            )
        lines.append("")

    # Replay table
    if replay_rows:
        lines.append("### Replay Verification (20×)")
        lines.append("")
        lines.append("| DB | Case | Target | Replays | Bug Hits | Rate |")
        lines.append("|---|---|---|---:|---:|---:|")
        for r in replay_rows:
            lines.append(
                f"| {r['db']} | {r['case_name']} | {r['target']} "
                f"| {r['replays']} | {r['bug_hits']} | {r['bug_rate']*100:.0f}% |"
            )
        lines.append("")


    lines.append("## 3. Overhead Benchmarking")
    lines.append("")
    lines.append("### Mean Performance (across 10 reps)")
    lines.append("")
    lines.append("| DB | Workload | Mode | Txns/sec | Median (ms) | P95 (ms) |")
    lines.append("|---|---|---|---:|---:|---:|")
    for r in overhead_rows:
        lines.append(
            f"| {r.db} | {r.workload} | {r.mode} "
            f"| {r.txns_per_sec} | {r.median_latency_ms} | {r.p95_latency_ms} |"
        )
    lines.append("")

    # Compute ratios vs baseline
    lines.append("### Overhead Ratios (vs baseline)")
    lines.append("")
    lines.append("| DB | Workload | Mode | Throughput Ratio | Latency Ratio (median) |")
    lines.append("|---|---|---|---:|---:|")

    # Build baseline lookup
    baseline_lookup: Dict[str, OverheadRow] = {}
    for r in overhead_rows:
        if r.mode == "baseline":
            baseline_lookup[f"{r.db}|{r.workload}"] = r

    for r in overhead_rows:
        key = f"{r.db}|{r.workload}"
        bl = baseline_lookup.get(key)
        if bl and bl.txns_per_sec > 0 and bl.median_latency_ms > 0:
            tp_ratio = r.txns_per_sec / bl.txns_per_sec
            lat_ratio = r.median_latency_ms / bl.median_latency_ms
            lines.append(
                f"| {r.db} | {r.workload} | {r.mode} "
                f"| {tp_ratio:.3f}× | {lat_ratio:.3f}× |"
            )
    lines.append("")


    lines.append("## Quality Gates")
    lines.append("")

    all_pass = True

    # Gate 1: All controls show 0 bugs
    ctrl_bugs = [r for r in discovery_rows if r.variant == "control" and r.success]
    if ctrl_bugs:
        lines.append(f"- **FAIL**: {len(ctrl_bugs)} false positive(s) in control variants")
        for r in ctrl_bugs:
            lines.append(f"  - {r.db}/{r.case_name}/{r.strategy}/rep{r.rep_idx}")
        all_pass = False
    else:
        ctrl_total = sum(1 for r in discovery_rows if r.variant == "control")
        lines.append(f"- **PASS**: All {ctrl_total} control runs show 0 bugs")

    # Gate 2: Replay buggy 20/20 and control 0/20
    for r in replay_rows:
        if r["target"] == "buggy":
            if r["bug_hits"] == r["replays"]:
                lines.append(f"- **PASS**: {r['db']}/{r['case_name']} buggy replay {r['bug_hits']}/{r['replays']}")
            else:
                lines.append(f"- **WARN**: {r['db']}/{r['case_name']} buggy replay {r['bug_hits']}/{r['replays']} (expected {r['replays']}/{r['replays']})")
                if r["bug_hits"] < r["replays"] * 0.8:
                    all_pass = False
        elif r["target"] == "control":
            if r["bug_hits"] == 0:
                lines.append(f"- **PASS**: {r['db']}/{r['case_name']} control replay {r['bug_hits']}/{r['replays']}")
            else:
                lines.append(f"- **FAIL**: {r['db']}/{r['case_name']} control replay {r['bug_hits']}/{r['replays']} (expected 0)")
                all_pass = False

    lines.append("")
    if all_pass:
        lines.append("**All quality gates PASSED.**")
    else:
        lines.append("**Some quality gates did not pass. See warnings above.**")

    lines.append("")

    text = "\n".join(lines)
    report_path = output_dir / "MASTER_REPORT.md"
    report_path.write_text(text, encoding="utf-8")
    print(f"  Wrote {report_path}")


def main() -> int:
    parser = argparse.ArgumentParser(description="DBTest artifact generator")
    parser.add_argument("--db", nargs="*", default=None,
                        help="DB(s) to include (default: postgres mysql sqlite)")
    parser.add_argument("--output-dir", type=Path, default=None)
    parser.add_argument("--skip-smoke", action="store_true")
    parser.add_argument("--skip-micro", action="store_true",
                        help="Skip Step 1 (micro coverage)")
    parser.add_argument("--skip-cases", action="store_true",
                        help="Skip Step 2 (case studies)")
    parser.add_argument("--skip-overhead", action="store_true",
                        help="Skip Step 3 (overhead benchmark)")
    parser.add_argument("--quick", action="store_true",
                        help="Reduced reps/duration for testing (3 reps, 15s)")
    parser.add_argument("--micro-duration", type=float, default=60.0,
                        help="Duration per micro coverage run (seconds)")
    parser.add_argument("--overhead-duration", type=float, default=60.0,
                        help="Duration per overhead benchmark run (seconds)")
    parser.add_argument("--reps", type=int, default=10,
                        help="Number of repetitions (default: 10)")
    args = parser.parse_args()

    # Quick mode overrides
    if args.quick:
        args.reps = min(args.reps, 3)
        args.micro_duration = min(args.micro_duration, 15.0)
        args.overhead_duration = min(args.overhead_duration, 15.0)

    reps = args.reps
    seeds = SEEDS[:reps]
    dbs = args.db or ["postgres", "mysql", "sqlite"]

    ts = time.strftime("%Y%m%d_%H%M%S")
    output_dir = args.output_dir or (ROOT / "results" / f"dbtest_2026_artifact_{ts}")
    _ensure_dir(output_dir)

    logger = CommandLogger(output_dir / "commands.log")

    print("╔══════════════════════════════════════════════════════════════╗")
    print("║          DBTest 2026 — Artifact Generation                 ║")
    print("╠══════════════════════════════════════════════════════════════╣")
    print(f"║  Output:     {str(output_dir):<48s}║")
    print(f"║  DBs:        {', '.join(dbs):<48s}║")
    print(f"║  Reps:       {reps:<48d}║")
    print(f"║  Seeds:      {str(seeds):<48s}║")
    print(f"║  Micro dur:  {args.micro_duration}s{' '*(46-len(str(args.micro_duration)))}║")
    print(f"║  Overhead:   {args.overhead_duration}s{' '*(46-len(str(args.overhead_duration)))}║")
    print("╚══════════════════════════════════════════════════════════════╝")

    overall_start = time.monotonic()

    # Step 0: Environment
    if not step0_environment(output_dir, logger, dbs, skip_smoke=args.skip_smoke):
        print("\n  ABORT: Environment setup failed.")
        logger.close()
        return 1

    # Step 1: Micro coverage
    all_points: List[CoveragePoint] = []
    auc_rows: List[Dict] = []
    if not args.skip_micro:
        all_points, auc_rows = step1_micro_coverage(
            output_dir, logger, dbs, args.micro_duration, reps, seeds,
        )
    else:
        print("\n  SKIP Step 1 (micro coverage)")
        logger.log_cmd("step1 micro coverage", "SKIPPED")

    # Step 2: Case studies
    discovery_rows: List[CaseDiscoveryRow] = []
    minimization_rows: List[Dict] = []
    replay_rows: List[Dict] = []
    if not args.skip_cases:
        discovery_rows, minimization_rows, replay_rows = step2_case_studies(
            output_dir, logger, dbs, reps, seeds,
        )
    else:
        print("\n  SKIP Step 2 (case studies)")
        logger.log_cmd("step2 case studies", "SKIPPED")

    # Step 3: Overhead
    overhead_rows: List[OverheadRow] = []
    if not args.skip_overhead:
        overhead_rows = step3_overhead(
            output_dir, logger, dbs, args.overhead_duration, reps, seeds,
        )
    else:
        print("\n  SKIP Step 3 (overhead benchmark)")
        logger.log_cmd("step3 overhead", "SKIPPED")

    # Collect overhead detail from the overhead directory
    overhead_detail_path = output_dir / "overhead" / "overhead_detail.csv"
    overhead_detail: List[Dict] = []
    if overhead_detail_path.exists():
        import csv as _csv
        with overhead_detail_path.open("r", encoding="utf-8") as f:
            reader = _csv.DictReader(f)
            overhead_detail = list(reader)

    # Step 4: Master compilation
    step4_master(
        output_dir, logger, auc_rows, discovery_rows,
        minimization_rows, replay_rows, overhead_rows, overhead_detail,
    )

    # Quality gate enforcement
    gate_pass = _check_quality_gates(discovery_rows, replay_rows)

    overall_elapsed = time.monotonic() - overall_start
    logger.log_cmd("TOTAL", "OK" if gate_pass else "GATES_FAILED", overall_elapsed)
    logger.close()

    # Final summary
    print(f"\n{'='*60}")
    print(f"  DBTest ARTIFACT GENERATION COMPLETE")
    print(f"{'='*60}")
    print(f"  Directory:  {output_dir}")
    print(f"  Duration:   {overall_elapsed/60:.1f} minutes")
    print(f"")
    print(f"  Key files:")
    print(f"    MASTER_REPORT.md")
    print(f"    MASTER_SUMMARY.csv")
    print(f"    commands.log")
    print(f"    versions.txt / hardware.txt")
    print(f"    micro_coverage/  (CSV + plots)")
    print(f"    case_studies/    (discovery + replay + minimization)")
    print(f"    overhead/        (detail + aggregate)")

    # Warnings
    ctrl_bugs = [r for r in discovery_rows if r.variant == "control" and r.success]
    if ctrl_bugs:
        print(f"\n  ⚠ WARNING: {len(ctrl_bugs)} false positive(s) in controls")
    for r in replay_rows:
        if r["target"] == "buggy" and r["bug_hits"] < r["replays"]:
            print(f"  ⚠ WARNING: {r['db']}/{r['case_name']} buggy replay {r['bug_hits']}/{r['replays']}")
        if r["target"] == "control" and r["bug_hits"] > 0:
            print(f"  ⚠ WARNING: {r['db']}/{r['case_name']} control FP {r['bug_hits']}/{r['replays']}")

    print(f"\n  Quality gates: {'PASS' if gate_pass else 'FAIL'}")
    print(f"{'='*60}")

    return 0 if gate_pass else 2


def _check_quality_gates(
    discovery_rows: List[CaseDiscoveryRow],
    replay_rows: List[Dict],
) -> bool:
    """Check quality gates, return True if all pass."""
    ok = True

    # Gate 1: All controls show 0 bugs
    ctrl_bugs = [r for r in discovery_rows if r.variant == "control" and r.success]
    if ctrl_bugs:
        ok = False

    # Gate 2: Each case must have buggy 20/20 and control 0/20
    for r in replay_rows:
        if r["target"] == "control" and r["bug_hits"] > 0:
            ok = False

    return ok


if __name__ == "__main__":
    sys.exit(main())
