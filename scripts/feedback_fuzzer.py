#!/usr/bin/env python3
"""
Feedback-driven workload fuzzer that iteratively adjusts workloads based on history analysis.

Workflow per iteration:
    1. Materialize a workload config derived from the previous iteration's feedback.
    2. Run the tx runner to produce an Elle-compatible history.
    3. Analyze the history using scripts.analyze_history for dependency metrics and feedback.
    4. Persist the analysis artifacts and mutate the workload for the next iteration.

Example usage:
    python scripts/feedback_fuzzer.py \
        --config workloads/example.yaml \
        --iterations 3 \
        --output-dir runs/demo \
        --runner-cmd "go run ./cmd/runner"

Dependencies:
    pip install pyyaml edn_format networkx matplotlib
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import shlex
import subprocess
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Sequence

try:
    import yaml
except ImportError as exc:  # pragma: no cover
    raise SystemExit(
        "[ERROR] Missing dependency PyYAML. Install with `pip install pyyaml`."
    ) from exc

try:
    # When executed from repository root we can import through the scripts package.
    from scripts.analyze_history import (
        AnalysisResult,
        analyze_history_file,
    )
except ImportError:  # pragma: no cover - fallback for direct invocation
    from analyze_history import (  # type: ignore
        AnalysisResult,
        analyze_history_file,
    )


RunnerCommand = Sequence[str]


@dataclass
class IterationContext:
    """Shared state carried across iterations."""

    base_keys: List[Any]
    last_target_keys: List[Any]
    iteration_index: int = 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Iteratively run workloads and refine them based on history feedback."
    )
    parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Base workload YAML config.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=3,
        help="Number of feedback iterations to run.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("feedback_runs"),
        help="Directory to store iteration configs, histories, and analysis artifacts.",
    )
    parser.add_argument(
        "--runner-cmd",
        type=str,
        default="go run ./cmd/runner",
        help="Command used to invoke the Go tx runner (quoted string).",
    )
    parser.add_argument(
        "--max-near-cycles",
        type=int,
        default=12,
        help="Maximum near-cycle paths to analyze per iteration.",
    )
    parser.add_argument(
        "--skip-run",
        action="store_true",
        help="Do not invoke the tx runner; assume histories already exist per iteration.",
    )
    parser.add_argument(
        "--keep-schema-init",
        action="store_true",
        help="Keep init_schema=true for every iteration (default resets to false after first).",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Emit verbose logs for config mutations.",
    )
    return parser.parse_args()


def load_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh)


def save_yaml(data: Dict[str, Any], path: Path) -> None:
    with path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(
            data,
            fh,
            sort_keys=False,
            default_flow_style=False,
        )


def ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def tokenize_runner(cmd: str) -> RunnerCommand:
    tokens = shlex.split(cmd)
    if not tokens:
        raise ValueError("Runner command must not be empty.")
    return tokens


def extract_candidate_keys(cfg: Dict[str, Any]) -> List[Any]:
    keys: List[Any] = []
    workload = cfg.get("workload", {})
    for txn in workload.get("transactions", []):
        for step in txn.get("steps", []):
            for arg in step.get("args", []):
                if isinstance(arg, dict) and "choice" in arg:
                    choice = arg["choice"]
                    if isinstance(choice, dict):
                        for item in choice.get("from", []):
                            keys.append(item)
                elif isinstance(arg, (int, str)):
                    keys.append(arg)
    deduped = []
    seen = set()
    for key in keys:
        if key in seen:
            continue
        seen.add(key)
        deduped.append(key)
    return deduped


def classify_transactions(cfg: Dict[str, Any]) -> Dict[str, Dict[str, bool]]:
    profiles: Dict[str, Dict[str, bool]] = {}
    workload = cfg.get("workload", {})
    for txn in workload.get("transactions", []):
        name = txn.get("name")
        if not name:
            continue
        writes = False
        reads = False
        for step in txn.get("steps", []):
            elle = step.get("elle")
            if not isinstance(elle, dict):
                continue
            f = str(elle.get("f", "")).lower()
            if f in {":write", ":w", ":append", ":inc"}:
                writes = True
            if f in {":read", ":r"}:
                reads = True
        profiles[name] = {"writes": writes, "reads": reads}
    return profiles


def prepare_config_for_iteration(
    cfg: Dict[str, Any],
    iteration: int,
    keep_schema_init: bool,
) -> Dict[str, Any]:
    cfg = copy.deepcopy(cfg)
    # Allow CLI -out flag to control history target by clearing config override.
    cfg.pop("out", None)
    if not keep_schema_init and iteration > 0:
        cfg["init_schema"] = False
    return cfg


def run_tx_runner(
    runner_cmd: RunnerCommand,
    config_path: Path,
    history_path: Path,
) -> None:
    cmd = list(runner_cmd) + [
        "-config",
        str(config_path),
        "-out",
        str(history_path),
    ]
    env = os.environ.copy()
    env["HISTORY_OUT"] = str(history_path)
    print(f"[RUN] {' '.join(cmd)}")
    subprocess.run(cmd, check=True, env=env)


def select_target_keys(
    result: AnalysisResult,
    ctx: IterationContext,
    max_keys: int = 4,
) -> List[Any]:
    counter: Counter[Any] = Counter()
    for near in result.feedback.near_cycles:
        if near.key is None:
            continue
        counter[near.key] += 3
    for key, count in result.feedback.key_pressure.items():
        counter[key] += count
    if not counter and ctx.last_target_keys:
        counter.update({key: 1 for key in ctx.last_target_keys})
    if not counter and ctx.base_keys:
        counter.update({key: 1 for key in ctx.base_keys})
    ranked = [key for key, _ in counter.most_common(max_keys)]
    if not ranked:
        return ctx.last_target_keys or ctx.base_keys
    return ranked


def retarget_transactions(
    transactions: List[Dict[str, Any]],
    target_keys: List[Any],
) -> None:
    if not target_keys:
        return
    for txn in transactions:
        for step in txn.get("steps", []):
            new_args: List[Any] = []
            for arg in step.get("args", []):
                new_args.append(_retarget_arg(arg, target_keys))
            if new_args:
                step["args"] = new_args


def _retarget_arg(arg: Any, target_keys: List[Any]) -> Any:
    if isinstance(arg, dict) and "choice" in arg:
        choice = arg["choice"]
        if isinstance(choice, dict):
            choice["from"] = list(target_keys)
        return arg
    if isinstance(arg, (int, str)) and len(target_keys) == 1:
        return target_keys[0]
    return arg


def adjust_mix(
    mix: List[Dict[str, Any]],
    profiles: Dict[str, Dict[str, bool]],
    result: AnalysisResult,
) -> None:
    if not mix:
        return
    weights: List[float] = []
    for item in mix:
        weight = float(item.get("weight", 1.0))
        profile = profiles.get(item.get("txn", ""), {})
        if result.edges_per_txn < 1.0 and profile.get("writes"):
            weight *= 1.2
        elif result.edges_per_txn > 2.0 and profile.get("reads") and not profile.get("writes"):
            weight *= 0.85
        weights.append(max(weight, 0.05))
    total = sum(weights) or 1.0
    for item, weight in zip(mix, weights, strict=False):
        item["weight"] = round(weight / total, 4)


def adjust_scheduling(
    sched: Dict[str, Any],
    result: AnalysisResult,
    iteration: int,
) -> None:
    clients = int(sched.get("clients", 4))
    if result.edges_per_txn < 1.0:
        clients = min(clients + 2, 32)
    elif result.edges_per_txn > 3.0:
        clients = max(clients - 1, 2)
    sched["clients"] = clients

    if result.feedback.near_cycles and sched.get("barrier_every", 0) == 0:
        sched["barrier_every"] = max(2, iteration + 1)


def build_next_config(
    cfg: Dict[str, Any],
    result: AnalysisResult,
    ctx: IterationContext,
) -> Dict[str, Any]:
    next_cfg = copy.deepcopy(cfg)
    workload = next_cfg.setdefault("workload", {})
    transactions = workload.get("transactions", [])
    target_keys = select_target_keys(result, ctx)
    retarget_transactions(transactions, target_keys)
    ctx.last_target_keys = target_keys

    profiles = classify_transactions(next_cfg)
    mix = workload.get("mix", [])
    adjust_mix(mix, profiles, result)

    scheduling = next_cfg.setdefault("scheduling", {})
    adjust_scheduling(scheduling, result, ctx.iteration_index)
    return next_cfg


def serialize_analysis(result: AnalysisResult) -> Dict[str, Any]:
    total_edges = sum(result.edge_counts.values())
    return {
        "transactions": len(result.transactions),
        "edge_counts": result.edge_counts,
        "total_edges": total_edges,
        "edges_per_txn": result.edges_per_txn,
        "dependency_density": result.dependency_density,
        "graph_density": result.complete_density,
        "cycles": result.cycles,
        "sccs": result.sccs,
        "near_cycles": [
            {
                "start": nc.start,
                "via": nc.via,
                "end": nc.end,
                "key": nc.key,
                "start_edge_types": list(nc.start_edge_types),
                "via_edge_types": list(nc.via_edge_types),
            }
            for nc in result.feedback.near_cycles
        ],
        "suggestions": result.feedback.suggestions,
        "key_pressure": result.feedback.key_pressure,
        "truncated_feedback": result.feedback.truncated,
    }


def save_analysis_json(result: AnalysisResult, path: Path) -> None:
    data = serialize_analysis(result)
    with path.open("w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=2, sort_keys=False)


def maybe_load_history(history_path: Path) -> Optional[AnalysisResult]:
    if not history_path.exists():
        print(f"[WARN] History file {history_path} missing; skipping analysis.")
        return None
    return analyze_history_file(history_path, max_near_paths=12)


def orchestrate(args: argparse.Namespace) -> None:
    base_cfg = load_yaml(args.config)
    ensure_output_dir(args.output_dir)
    runner_cmd = tokenize_runner(args.runner_cmd)
    context = IterationContext(
        base_keys=extract_candidate_keys(base_cfg),
        last_target_keys=[],
    )
    current_cfg = copy.deepcopy(base_cfg)

    for iteration in range(args.iterations):
        context.iteration_index = iteration
        iter_dir = args.output_dir / f"iter_{iteration:02d}"
        ensure_output_dir(iter_dir)

        iter_cfg = prepare_config_for_iteration(
            current_cfg,
            iteration,
            keep_schema_init=args.keep_schema_init,
        )

        config_path = iter_dir / "workload.yaml"
        history_path = iter_dir / "history.edn"
        analysis_path = iter_dir / "analysis.json"

        save_yaml(iter_cfg, config_path)
        print(f"[ITER {iteration}] Config saved to {config_path}")
        print(f"[ITER {iteration}] Target keys: {context.last_target_keys or context.base_keys}")

        if not args.skip_run:
            try:
                run_tx_runner(runner_cmd, config_path, history_path)
            except subprocess.CalledProcessError as exc:
                print(f"[ERROR] Runner failed (iteration {iteration}): {exc}")
                break
        else:
            print("[INFO] skip-run enabled; expecting history to pre-exist.")

        if not history_path.exists():
            print(f"[WARN] Skipping analysis; history not found at {history_path}")
            break

        result = analyze_history_file(
            history_path,
            max_near_paths=max(args.max_near_cycles, 1),
        )
        save_analysis_json(result, analysis_path)
        print(
            f"[ITER {iteration}] edges={sum(result.edge_counts.values())} "
            f"edges/txn={result.edges_per_txn:.3f} cycles={len(result.cycles)} "
            f"near={len(result.feedback.near_cycles)}"
        )
        if args.verbose:
            print(f"[ITER {iteration}] suggestions:")
            for suggestion in result.feedback.suggestions:
                print(f"    {suggestion}")

        current_cfg = build_next_config(iter_cfg, result, context)
        if args.verbose:
            next_mix = current_cfg.get("workload", {}).get("mix", [])
            sched = current_cfg.get("scheduling", {})
            print(
                f"[ITER {iteration}] next target keys: {context.last_target_keys} "
                f"clients={sched.get('clients')} mix={next_mix}"
            )


def main() -> int:
    args = parse_args()
    try:
        orchestrate(args)
    except Exception as exc:  # pragma: no cover
        print(f"[ERROR] feedback fuzzer failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
