from __future__ import annotations

import argparse
import copy
import csv
import datetime as dt
import random
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from bench.known_bugs.analyze import analyze_results
from bench.known_bugs.baseline_runner import run_baseline
from bench.known_bugs.common import (
    ROOT,
    RunnerSummary,
    apply_schema_and_setup,
    bug_dir,
    clamp_ms,
    ensure_dir,
    load_checker_module,
    load_manifest,
    normalize_schedule,
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


def _tool_schedule(
    *,
    rng: random.Random,
    trial: int,
    base_schedule: List[List[float]],
    best_schedule: List[List[float]],
    max_delay_ms: float,
    jitter_ms: float,
    conflict_steps: List[int],
) -> List[List[float]]:
    if trial == 1:
        return normalize_schedule(copy.deepcopy(base_schedule))

    parent = best_schedule if trial % 3 == 0 else base_schedule
    candidate = normalize_schedule(copy.deepcopy(parent))

    mode = rng.random()
    rows = len(candidate)
    cols = len(candidate[0]) if rows else 0

    if mode < 0.45 and conflict_steps:
        anchor = rng.uniform(0.0, max_delay_ms)
        for step in conflict_steps:
            if step < 0 or step >= cols:
                continue
            for client in range(rows):
                mutated = anchor + rng.uniform(-jitter_ms, jitter_ms)
                candidate[client][step] = clamp_ms(mutated, 0.0, max_delay_ms)
    elif mode < 0.80 and rows and cols:
        client = rng.randrange(rows)
        step = rng.randrange(cols)
        candidate[client][step] = rng.uniform(0.0, max_delay_ms)
    else:
        for client in range(rows):
            for step in range(cols):
                mutated = candidate[client][step] + rng.uniform(-jitter_ms, jitter_ms)
                candidate[client][step] = clamp_ms(mutated, 0.0, max_delay_ms)

    # Keep at least some mutation pressure around conflict windows.
    for step in conflict_steps:
        if step < 0 or step >= cols:
            continue
        if rng.random() < 0.20:
            anchor = rng.uniform(0.0, max_delay_ms)
            for client in range(rows):
                candidate[client][step] = clamp_ms(anchor + rng.uniform(-jitter_ms, jitter_ms), 0.0, max_delay_ms)

    return normalize_schedule(candidate)


def _write_results_csv(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
    rows_list = list(rows)
    if not rows_list:
        return
    fieldnames = list(rows_list[0].keys())
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows_list)


def run_tool(
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
    base_schedule_raw = timing.get("baseline_delays_ms")
    if not isinstance(base_schedule_raw, list):
        raise ValueError(f"Missing timing_model.baseline_delays_ms in {case_dir / 'workload.yaml'}")
    base_schedule = normalize_schedule(base_schedule_raw)

    tool_cfg = timing.get("tool", {}) if isinstance(timing.get("tool"), dict) else {}
    max_delay_ms = float(tool_cfg.get("max_delay_ms", 35.0))
    jitter_ms = float(tool_cfg.get("mutation_jitter_ms", 6.0))
    conflict_steps = [int(v) for v in tool_cfg.get("conflict_steps", [1, 2])]

    isolation = str(workload.get("isolation_level", "READ COMMITTED"))
    resolved_dsn = resolve_dsn(dsn)

    schema_path = case_dir / str(entry.get("schema", "schema.sql"))
    setup_path = case_dir / str(entry.get("setup", "setup.sql"))

    ensure_dir(artifact_dir)
    trials_dir = ensure_dir(artifact_dir / "trials")
    histories_dir = ensure_dir(artifact_dir / "histories")

    config = {
        "runner": "tool",
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
        "base_schedule_ms": base_schedule,
        "tool_mutation": {
            "max_delay_ms": max_delay_ms,
            "mutation_jitter_ms": jitter_ms,
            "conflict_steps": conflict_steps,
        },
    }
    write_json(artifact_dir / "config.json", config)

    rng = random.Random(seed)
    best_schedule = copy.deepcopy(base_schedule)
    best_score = -1.0

    started = time.monotonic()
    detected = False
    detection_reason = "budget_exhausted"
    time_to_first: Optional[float] = None
    trials_to_first: Optional[int] = None

    trial = 0
    while trial < max_trials and (time.monotonic() - started) < time_budget_s:
        trial += 1
        apply_schema_and_setup(resolved_dsn, schema_path, setup_path)

        schedule = _tool_schedule(
            rng=rng,
            trial=trial,
            base_schedule=base_schedule,
            best_schedule=best_schedule,
            max_delay_ms=max_delay_ms,
            jitter_ms=jitter_ms,
            conflict_steps=conflict_steps,
        )

        trial_started = time.monotonic()
        trial_result = checker.run_trial(
            dsn=resolved_dsn,
            isolation_level=isolation,
            schedule_ms=schedule,
            clients=clients,
            ops=ops,
            concurrency=concurrency,
            timeout_s=timeout_per_trial_s,
            seed=seed,
        )
        check = checker.check_trial(trial_result)
        trial_elapsed = round(time.monotonic() - trial_started, 6)
        total_elapsed = round(time.monotonic() - started, 6)

        score = float(check.get("score", 0.0))
        if score >= best_score:
            best_score = score
            best_schedule = copy.deepcopy(schedule)

        history = trial_result.get("history", [])
        write_json(histories_dir / f"trial_{trial:04d}_history.json", history)

        trial_payload = {
            "trial": trial,
            "seed": seed,
            "runner": "tool",
            "schedule_ms": schedule,
            "best_score_so_far": best_score,
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
        runner="tool",
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
            "best_score": best_score,
            "best_schedule_ms": best_schedule,
        },
    )
    return summary


def run_benchmark(
    *,
    output_dir: Path,
    dsn: Optional[str],
    seeds: List[int],
    bug_names: List[str],
    max_trials: int,
    time_budget_s: float,
    clients: int,
    ops: int,
    concurrency: int,
    timeout_per_trial_s: float,
    manifest_path: Optional[Path],
) -> Dict[str, Any]:
    ensure_dir(output_dir)
    manifest = load_manifest(manifest_path)

    benchmark_config = {
        "created_at_utc": _utc_stamp(),
        "manifest": str((manifest_path or (ROOT / "manifest.yaml")).resolve()),
        "dsn": resolve_dsn(dsn),
        "seeds": seeds,
        "bugs": bug_names,
        "max_trials": max_trials,
        "time_budget_s": time_budget_s,
        "clients": clients,
        "ops": ops,
        "concurrency": concurrency,
        "timeout_per_trial_s": timeout_per_trial_s,
        "method": "head_to_head_tool_vs_baseline",
    }
    write_json(output_dir / "benchmark_config.json", benchmark_config)

    rows: List[Dict[str, Any]] = []
    results_csv = output_dir / "results.csv"

    for bug in bug_names:
        resolve_bug_entry(manifest, bug)
        bug_root = ensure_dir(output_dir / bug)
        for seed in seeds:
            seed_root = ensure_dir(bug_root / f"seed_{seed:04d}")

            # Alternate order to reduce warm-cache/order effects.
            order = ["tool", "baseline"] if (seed % 2 == 0) else ["baseline", "tool"]
            for runner_name in order:
                artifact_dir = ensure_dir(seed_root / runner_name)
                if runner_name == "tool":
                    summary = run_tool(
                        bug_name=bug,
                        dsn=dsn,
                        seed=seed,
                        max_trials=max_trials,
                        time_budget_s=time_budget_s,
                        clients=clients,
                        ops=ops,
                        concurrency=concurrency,
                        artifact_dir=artifact_dir,
                        timeout_per_trial_s=timeout_per_trial_s,
                        manifest_path=manifest_path,
                    )
                else:
                    summary = run_baseline(
                        bug_name=bug,
                        dsn=dsn,
                        seed=seed,
                        max_trials=max_trials,
                        time_budget_s=time_budget_s,
                        clients=clients,
                        ops=ops,
                        concurrency=concurrency,
                        artifact_dir=artifact_dir,
                        timeout_per_trial_s=timeout_per_trial_s,
                        manifest_path=manifest_path,
                    )
                rows.append(summary.to_row())
                _write_results_csv(results_csv, rows)

    summary_md = output_dir / "summary.md"
    plots_dir = output_dir / "plots"
    analysis = analyze_results(results_csv=results_csv, summary_md=summary_md, plots_dir=plots_dir)
    write_json(output_dir / "analysis.json", analysis)

    return {
        "output_dir": str(output_dir.resolve()),
        "results_csv": str(results_csv.resolve()),
        "summary_md": str(summary_md.resolve()),
        "plots_dir": str(plots_dir.resolve()),
    }


def _parse_seed_list(seed_count: int, start_seed: int, explicit: Optional[str]) -> List[int]:
    if explicit:
        out: List[int] = []
        for token in explicit.split(","):
            token = token.strip()
            if not token:
                continue
            out.append(int(token))
        if not out:
            raise ValueError("--seed-list was provided but empty")
        return out
    if seed_count <= 0:
        raise ValueError("--seeds must be > 0")
    return list(range(start_seed, start_seed + seed_count))


def _parse_bug_list(manifest: Dict[str, Any], raw: Optional[str]) -> List[str]:
    available = [str(entry.get("name")) for entry in manifest.get("bugs", [])]
    if raw:
        out = [item.strip() for item in raw.split(",") if item.strip()]
        missing = [name for name in out if name not in available]
        if missing:
            raise ValueError(f"Unknown bug(s): {', '.join(missing)}")
        return out
    return available


def _default_tool_artifact_root() -> Path:
    return ROOT / "artifacts" / f"manual_tool_{_utc_stamp()}"


def _default_benchmark_root() -> Path:
    return ROOT / "artifacts" / f"benchmark_{_utc_stamp()}"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Known bug suite tool runner and benchmark harness.")
    subparsers = parser.add_subparsers(dest="cmd", required=True)

    tool = subparsers.add_parser("tool", help="Run adaptive tool for one bug")
    tool.add_argument("--bug", required=True, help="Bug name from manifest.yaml")
    tool.add_argument("--dsn", default=None, help="PostgreSQL DSN")
    tool.add_argument("--seed", type=int, default=1, help="Deterministic seed")
    tool.add_argument("--max-trials", type=int, default=40, help="Maximum trials")
    tool.add_argument("--time-budget", type=float, default=30.0, help="Time budget (seconds)")
    tool.add_argument("--clients", type=int, default=2, help="Client count")
    tool.add_argument("--ops", type=int, default=4, help="Operations per client")
    tool.add_argument("--concurrency", type=int, default=2, help="Concurrency level")
    tool.add_argument("--timeout-per-trial", type=float, default=15.0, help="Per-trial timeout seconds")
    tool.add_argument(
        "--artifact-dir",
        type=Path,
        default=None,
        help="Artifact directory (default: bench/known_bugs/artifacts/manual_tool_<ts>)",
    )
    tool.add_argument("--manifest", type=Path, default=None, help="Override manifest path")

    bench = subparsers.add_parser("benchmark", help="Run tool vs baseline across known bug suite")
    bench.add_argument("--dsn", default=None, help="PostgreSQL DSN")
    bench.add_argument("--seeds", type=int, default=30, help="Number of seeds (default 30)")
    bench.add_argument("--start-seed", type=int, default=1, help="Starting seed value")
    bench.add_argument("--seed-list", default=None, help="Explicit comma-separated seed list (overrides --seeds)")
    bench.add_argument("--bugs", default=None, help="Comma-separated bug names (default all manifest bugs)")
    bench.add_argument("--max-trials", type=int, default=40, help="Maximum trials per seed/runner")
    bench.add_argument("--time-budget", type=float, default=30.0, help="Time budget per seed/runner (seconds)")
    bench.add_argument("--clients", type=int, default=2, help="Client count")
    bench.add_argument("--ops", type=int, default=4, help="Operations per client")
    bench.add_argument("--concurrency", type=int, default=2, help="Concurrency level")
    bench.add_argument("--timeout-per-trial", type=float, default=15.0, help="Per-trial timeout seconds")
    bench.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Benchmark output directory (default: bench/known_bugs/artifacts/benchmark_<ts>)",
    )
    bench.add_argument("--manifest", type=Path, default=None, help="Override manifest path")

    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    if args.cmd == "tool":
        artifact_dir = args.artifact_dir or _default_tool_artifact_root()
        summary = run_tool(
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

    if args.cmd == "benchmark":
        manifest = load_manifest(args.manifest)
        bug_names = _parse_bug_list(manifest, args.bugs)
        seeds = _parse_seed_list(args.seeds, args.start_seed, args.seed_list)
        output_dir = args.output_dir or _default_benchmark_root()

        result = run_benchmark(
            output_dir=output_dir,
            dsn=args.dsn,
            seeds=seeds,
            bug_names=bug_names,
            max_trials=args.max_trials,
            time_budget_s=args.time_budget,
            clients=args.clients,
            ops=args.ops,
            concurrency=args.concurrency,
            timeout_per_trial_s=args.timeout_per_trial,
            manifest_path=args.manifest,
        )
        print(result)
        return 0

    parser.error(f"Unknown command: {args.cmd}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
