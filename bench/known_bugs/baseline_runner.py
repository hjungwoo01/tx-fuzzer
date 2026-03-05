from __future__ import annotations

import argparse
import datetime as dt
import time
from pathlib import Path
from typing import Any, Dict, Optional

from bench.known_bugs.common import (
    ROOT,
    RunnerSummary,
    apply_schema_and_setup,
    bug_dir,
    ensure_dir,
    load_checker_module,
    load_manifest,
    read_yaml,
    resolve_bug_entry,
    resolve_dsn,
    write_json,
)


def _utc_stamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")


def _validate_dimensions(workload: Dict[str, Any], *, clients: int, ops: int) -> None:
    expected_clients = int(workload.get("clients", 2))
    expected_ops = int(workload.get("ops_per_client", 4))
    if clients != expected_clients:
        raise ValueError(
            f"This workload expects clients={expected_clients}; got clients={clients}."
        )
    if ops != expected_ops:
        raise ValueError(f"This workload expects ops={expected_ops}; got ops={ops}.")


def run_baseline(
    *,
    bug_name: str,
    dsn: Optional[str],
    seed: int,
    max_trials: int,
    time_budget_s: float,
    clients: int,
    ops: int,
    concurrency: int,
    artifact_dir: Path,
    timeout_per_trial_s: float = 15.0,
    manifest_path: Optional[Path] = None,
) -> RunnerSummary:
    manifest = load_manifest(manifest_path)
    entry = resolve_bug_entry(manifest, bug_name)
    case_dir = bug_dir(entry)
    checker = load_checker_module(case_dir)
    workload = read_yaml(case_dir / str(entry.get("workload", "workload.yaml")))

    _validate_dimensions(workload, clients=clients, ops=ops)

    timing = workload.get("timing_model", {})
    baseline_schedule = timing.get("baseline_delays_ms")
    if not isinstance(baseline_schedule, list):
        raise ValueError(f"Missing timing_model.baseline_delays_ms in {case_dir / 'workload.yaml'}")

    isolation = str(workload.get("isolation_level", "READ COMMITTED"))
    resolved_dsn = resolve_dsn(dsn)

    schema_path = case_dir / str(entry.get("schema", "schema.sql"))
    setup_path = case_dir / str(entry.get("setup", "setup.sql"))

    ensure_dir(artifact_dir)
    trials_dir = ensure_dir(artifact_dir / "trials")
    histories_dir = ensure_dir(artifact_dir / "histories")

    config = {
        "runner": "baseline",
        "bug": bug_name,
        "seed": seed,
        "dsn": resolved_dsn,
        "manifest": str((manifest_path or (ROOT / "manifest.yaml")).resolve()),
        "case_dir": str(case_dir),
        "schema": str(schema_path),
        "setup": str(setup_path),
        "isolation_level": isolation,
        "max_trials": max_trials,
        "time_budget_s": time_budget_s,
        "timeout_per_trial_s": timeout_per_trial_s,
        "clients": clients,
        "ops": ops,
        "concurrency": concurrency,
        "schedule_ms": baseline_schedule,
    }
    write_json(artifact_dir / "config.json", config)

    started = time.monotonic()
    detected = False
    detection_reason = "budget_exhausted"
    time_to_first: Optional[float] = None
    trials_to_first: Optional[int] = None

    trial = 0
    while trial < max_trials and (time.monotonic() - started) < time_budget_s:
        trial += 1
        apply_schema_and_setup(resolved_dsn, schema_path, setup_path)

        trial_started = time.monotonic()
        trial_result = checker.run_trial(
            dsn=resolved_dsn,
            isolation_level=isolation,
            schedule_ms=baseline_schedule,
            clients=clients,
            ops=ops,
            concurrency=concurrency,
            timeout_s=timeout_per_trial_s,
            seed=seed,
        )
        check = checker.check_trial(trial_result)
        trial_elapsed = round(time.monotonic() - trial_started, 6)
        total_elapsed = round(time.monotonic() - started, 6)

        history = trial_result.get("history", [])
        write_json(histories_dir / f"trial_{trial:04d}_history.json", history)

        trial_payload = {
            "trial": trial,
            "seed": seed,
            "runner": "baseline",
            "schedule_ms": baseline_schedule,
            "trial_elapsed_s": trial_elapsed,
            "elapsed_since_start_s": total_elapsed,
            "check": check,
            "result": {k: v for k, v in trial_result.items() if k != "history"},
        }
        write_json(trials_dir / f"trial_{trial:04d}.json", trial_payload)

        if bool(check.get("found")):
            detected = True
            detection_reason = str(check.get("reason") or "detected")
            time_to_first = total_elapsed
            trials_to_first = trial
            break

    summary = RunnerSummary(
        bug=bug_name,
        runner="baseline",
        seed=seed,
        detected=detected,
        detection_reason=detection_reason,
        time_to_first_s=time_to_first,
        trials_to_first=trials_to_first,
        trials_executed=trial,
        time_budget_s=time_budget_s,
        max_trials=max_trials,
        artifact_dir=str(artifact_dir.resolve()),
    )

    write_json(
        artifact_dir / "detection.json",
        {
            "completed_at_utc": _utc_stamp(),
            "summary": summary.to_row(),
        },
    )
    return summary


def _default_artifact_root() -> Path:
    return ROOT / "artifacts" / f"manual_baseline_{_utc_stamp()}"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Naive baseline runner for known PostgreSQL anomalies.")
    parser.add_argument("--bug", required=True, help="Bug name from manifest.yaml")
    parser.add_argument("--dsn", default=None, help="PostgreSQL DSN (defaults to DATABASE_URL or local default)")
    parser.add_argument("--seed", type=int, default=1, help="Deterministic seed for reproducibility metadata")
    parser.add_argument("--max-trials", type=int, default=40, help="Maximum trials")
    parser.add_argument("--time-budget", type=float, default=30.0, help="Time budget in seconds")
    parser.add_argument("--clients", type=int, default=2, help="Client count")
    parser.add_argument("--ops", type=int, default=4, help="Operations per client")
    parser.add_argument("--concurrency", type=int, default=2, help="Concurrency level")
    parser.add_argument(
        "--artifact-dir",
        type=Path,
        default=None,
        help="Artifact directory (default: bench/known_bugs/artifacts/manual_baseline_<ts>)",
    )
    parser.add_argument("--manifest", type=Path, default=None, help="Override manifest path")
    parser.add_argument("--timeout-per-trial", type=float, default=15.0, help="Per-trial timeout seconds")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    artifact_dir = args.artifact_dir or _default_artifact_root()
    summary = run_baseline(
        bug_name=args.bug,
        dsn=args.dsn,
        seed=args.seed,
        max_trials=args.max_trials,
        time_budget_s=args.time_budget,
        clients=args.clients,
        ops=args.ops,
        concurrency=args.concurrency,
        artifact_dir=artifact_dir,
        timeout_per_trial_s=args.timeout_per_trial,
        manifest_path=args.manifest,
    )
    print(summary.to_row())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
