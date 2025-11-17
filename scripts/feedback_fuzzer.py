"""
Feedback-driven workload fuzzer that iteratively adjusts workloads based on history analysis.

Workflow per iteration:
    1. Materialize a workload config derived from the previous iteration's feedback.
    2. Run the tx runner to produce an Elle-compatible history.
    3. Analyze the history using scripts.analyze_history for dependency metrics and feedback.
    4. Persist the analysis artifacts, render a minimal dependency graph highlighting the missing edge, and mutate the workload for the next iteration.

Example usage:
    python scripts/feedback_fuzzer.py \
        --config workloads/example.yaml \
        --iterations 3 \
        --output-dir runs/demo \
        --runner-cmd "go run ./cmd/runner"
    python scripts/feedback_fuzzer.py \
        --workload write_skew \
        --output-dir runs/write-skew-loop \
        --iterations 5

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
import yaml
import networkx as nx
import matplotlib.pyplot as plt
from collections import Counter
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    # When executed from repository root we can import through the scripts package.
    from scripts.analyze_history import (
        AnalysisResult,
        NearCycle,
        analyze_history_file,
    )
except ImportError:  # pragma: no cover - fallback for direct invocation
    from analyze_history import (  # type: ignore
        AnalysisResult,
        NearCycle,
        analyze_history_file,
    )

RunnerCommand = Sequence[str]


@dataclass
class IterationContext:
    """Shared state carried across iterations."""

    base_keys: List[Any]
    last_target_keys: List[Any]
    iteration_index: int = 0
    replay_failures: Dict[Tuple[str, str, str], int] = field(default_factory=dict)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Iteratively run workloads and refine them based on history feedback."
    )
    parser.add_argument(
        "--config",
        type=Path,
        help="Path to base workload YAML config (overrides --workload).",
    )
    parser.add_argument(
        "--workload",
        type=str,
        help="Name of a workload under --workload-dir (e.g. 'example').",
    )
    parser.add_argument(
        "--workload-dir",
        type=Path,
        default=Path("workloads"),
        help="Directory containing named workload YAML files.",
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
        "--run-replay",
        action="store_true",
        help="Execute generated replay schedules and fold the outcome back into key selection.",
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


def resolve_base_config(args: argparse.Namespace) -> Path:
    if args.config and args.workload:
        raise SystemExit("Specify only one of --config or --workload.")
    if args.config:
        return args.config
    if args.workload:
        candidate = args.workload_dir / f"{args.workload}.yaml"
        if not candidate.exists():
            raise SystemExit(
                f"Workload '{args.workload}' not found under {args.workload_dir}."
            )
        return candidate
    raise SystemExit("One of --config or --workload is required.")


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


def evaluate_replay(
    runner_cmd: RunnerCommand,
    replay_config: Path,
    iter_dir: Path,
    near_cycle: NearCycle,
    ctx: IterationContext,
    max_near_paths: int,
) -> Optional[bool]:
    history_path = iter_dir / "replay_history.edn"
    try:
        run_tx_runner(runner_cmd, replay_config, history_path)
    except subprocess.CalledProcessError as exc:
        print(f"[WARN] Replay runner failed: {exc}")
        summary = {
            "status": "error",
            "error": str(exc),
        }
        (iter_dir / "replay_result.json").write_text(
            json.dumps(summary, indent=2),
            encoding="utf-8",
        )
        cycle_key = (near_cycle.start, near_cycle.via, near_cycle.end)
        ctx.replay_failures[cycle_key] = ctx.replay_failures.get(cycle_key, 0) + 1
        return None

    replay_result = analyze_history_file(
        history_path,
        max_near_paths=max(max_near_paths, 1),
    )
    edge_present = replay_result.graph.has_edge(near_cycle.end, near_cycle.start)
    edge_matches_key = False
    if edge_present:
        rels = replay_result.graph[near_cycle.end][near_cycle.start].get("relationships", [])
        if near_cycle.key is None:
            edge_matches_key = True
        else:
            edge_matches_key = any(rel.key == near_cycle.key for rel in rels)

    success = edge_present and edge_matches_key
    summary = {
        "status": "ok",
        "edge_added": success,
        "edge_present": edge_present,
        "edge_matches_key": edge_matches_key,
        "target": {
            "start": near_cycle.start,
            "via": near_cycle.via,
            "end": near_cycle.end,
            "key": near_cycle.key,
        },
    }
    replay_result_path = iter_dir / "replay_result.json"
    replay_result_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    replay_analysis_path = iter_dir / "replay_analysis.json"
    replay_analysis_path.write_text(
        json.dumps(serialize_analysis(replay_result), indent=2),
        encoding="utf-8",
    )

    cycle_key = (near_cycle.start, near_cycle.via, near_cycle.end)
    if success:
        ctx.replay_failures.pop(cycle_key, None)
    else:
        ctx.replay_failures[cycle_key] = ctx.replay_failures.get(cycle_key, 0) + 1
    return success


def select_target_keys(
    result: AnalysisResult,
    ctx: IterationContext,
    max_keys: int = 4,
) -> List[Any]:
    counter: Counter[Any] = Counter()
    for near in result.feedback.near_cycles:
        if near.key is None:
            continue
        weight = 3
        cycle_key = (near.start, near.via, near.end)
        failures = ctx.replay_failures.get(cycle_key, 0)
        if failures:
            weight += 3 * failures
        counter[near.key] += weight
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


def build_replay_config(
    cfg: Dict[str, Any],
    result: AnalysisResult,
) -> Optional[Dict[str, Any]]:
    if not result.feedback.near_cycles:
        return None

    near = result.feedback.near_cycles[0]
    triplet = [
        ("start", near.start_template),
        ("via", near.via_template),
        ("end", near.end_template),
    ]

    if any(template is None for _, template in triplet):
        return None

    workload = cfg.get("workload", {})
    templates = {
        txn.get("name")
        for txn in workload.get("transactions", [])
        if isinstance(txn, dict)
    }

    missing = [
        (alias, template)
        for alias, template in triplet
        if template not in templates
    ]
    if missing:
        return None

    replay_cfg = copy.deepcopy(cfg)
    replay_cfg["replay"] = {
        "enabled": True,
        "transactions": [
            {"alias": alias, "txn": template} for alias, template in triplet
        ],
    }

    if near.key is not None:
        replay_cfg["replay"]["focus_key"] = near.key
        retarget_transactions(
            replay_cfg.get("workload", {}).get("transactions", []),
            [near.key],
        )

    step_info_available = any(
        value
        for value in (
            near.start_step,
            near.via_step_from_start,
            near.via_step_to_end,
            near.end_step,
        )
    )

    schedule: List[Dict[str, Any]] = []
    pause_after: Dict[str, List[int]] = {}

    if step_info_available:
        executed: Dict[str, int] = {}

        def add_step(alias: str, target: Optional[int]) -> None:
            if target is None:
                return
            prev = executed.get(alias, 0)
            if target <= prev:
                return
            schedule.append({"alias": alias, "action": "step", "steps": target - prev})
            executed[alias] = target

        if near.start_step:
            pause_after["start"] = [near.start_step]
        via_steps = sorted(
            {step for step in (near.via_step_from_start, near.via_step_to_end) if step}
        )
        if via_steps:
            pause_after["via"] = via_steps
        if near.end_step:
            pause_after["end"] = [near.end_step]

        add_step("start", near.start_step)
        for step in via_steps:
            add_step("via", step)
        add_step("end", near.end_step)
    else:
        # Fall back to draining transactions sequentially before commit.
        pause_after = {}

    for alias in ("start", "via", "end"):
        schedule.append({"alias": alias, "action": "step", "steps": 0})

    schedule.extend(
        [
            {"alias": "via", "action": "commit"},
            {"alias": "end", "action": "commit"},
            {"alias": "start", "action": "commit"},
        ]
    )

    replay_cfg["replay"]["schedule"] = schedule
    if pause_after:
        replay_cfg["replay"]["pause_after"] = pause_after

    scheduling = replay_cfg.setdefault("scheduling", {})
    scheduling["clients"] = 1
    scheduling.pop("barrier_every", None)
    return replay_cfg


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
                "start_template": nc.start_template,
                "via_template": nc.via_template,
                "end_template": nc.end_template,
                "start_step": nc.start_step,
                "via_step_from_start": nc.via_step_from_start,
                "via_step_to_end": nc.via_step_to_end,
                "end_step": nc.end_step,
                "replay_attempts": nc.replay_attempts,
            }
            for nc in result.feedback.near_cycles
        ],
        "candidate_missing_edges": [
            {
                "source": nc.end,
                "target": nc.start,
                "via": nc.via,
                "key": nc.key,
                "source_template": nc.end_template,
                "target_template": nc.start_template,
                "via_template": nc.via_template,
                "start_step": nc.start_step,
                "via_step": nc.via_step_to_end,
                "target_step": nc.end_step,
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


def draw_minimal_graph(result: AnalysisResult, path: Path) -> Optional[str]:
    if not result.feedback.near_cycles:
        print("[WARN] No near cycles detected; skipping minimal graph export.")
        return None

    path.parent.mkdir(parents=True, exist_ok=True)
    near = result.feedback.near_cycles[0]
    g = nx.DiGraph()
    nodes = {near.start, near.via, near.end}
    for node in nodes:
        g.add_node(node)

    edge_labels: Dict[Tuple[str, str], str] = {}

    def copy_edge(src: str, dst: str) -> None:
        if result.graph.has_edge(src, dst):
            data = result.graph[src][dst]
            label = data.get("label", "")
            g.add_edge(src, dst, label=label, missing=False)
            edge_labels[(src, dst)] = label

    copy_edge(near.start, near.via)
    copy_edge(near.via, near.end)

    missing_label = f"add edge on key {near.key}" if near.key is not None else "add conflicting edge"
    g.add_edge(near.end, near.start, label=missing_label, missing=True)
    edge_labels[(near.end, near.start)] = missing_label

    plt.figure(figsize=(6, 4))
    pos = nx.circular_layout(g)
    nx.draw_networkx_nodes(g, pos, node_size=1400, node_color="#1f77b4", alpha=0.9)
    nx.draw_networkx_labels(g, pos, font_size=10, font_color="white")

    existing_edges = [(u, v) for u, v, data in g.edges(data=True) if not data.get("missing")]
    missing_edges = [(u, v) for u, v, data in g.edges(data=True) if data.get("missing")]

    if existing_edges:
        nx.draw_networkx_edges(
            g,
            pos,
            edgelist=existing_edges,
            edge_color="#2ca02c",
            arrows=True,
            arrowstyle="->",
            arrowsize=20,
            width=2.0,
        )
    if missing_edges:
        nx.draw_networkx_edges(
            g,
            pos,
            edgelist=missing_edges,
            edge_color="#d62728",
            arrows=True,
            arrowstyle="->",
            arrowsize=20,
            width=2.5,
            style="dashed",
        )

    nx.draw_networkx_edge_labels(g, pos, edge_labels=edge_labels, font_size=9, font_color="#333333")
    plt.title("Minimal dependency subgraph", fontsize=12)
    plt.tight_layout()
    plt.savefig(path, dpi=200)
    plt.close()
    description = f"{near.end} -> {near.start} on key {near.key}" if near.key is not None else f"{near.end} -> {near.start}"
    print(f"[INFO] Saved minimal dependency graph to {path} (missing edge {description})")
    return description


def maybe_load_history(history_path: Path) -> Optional[AnalysisResult]:
    if not history_path.exists():
        print(f"[WARN] History file {history_path} missing; skipping analysis.")
        return None
    return analyze_history_file(history_path, max_near_paths=12)


def orchestrate(args: argparse.Namespace) -> None:
    config_path = resolve_base_config(args)
    base_cfg = load_yaml(config_path)
    print(f"[CONFIG] Using base workload {config_path}")
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

        iter_cfg_path = iter_dir / "workload.yaml"
        history_path = iter_dir / "history.edn"
        analysis_path = iter_dir / "analysis.json"

        save_yaml(iter_cfg, iter_cfg_path)
        print(f"[ITER {iteration}] Config saved to {iter_cfg_path}")
        print(f"[ITER {iteration}] Target keys: {context.last_target_keys or context.base_keys}")

        if not args.skip_run:
            try:
                run_tx_runner(runner_cmd, iter_cfg_path, history_path)
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

        current_cycle_keys = {
            (nc.start, nc.via, nc.end) for nc in result.feedback.near_cycles
        }
        context.replay_failures = {
            key: value
            for key, value in context.replay_failures.items()
            if key in current_cycle_keys
        }

        minimal_graph_path = iter_dir / "minimal_dependency_graph.png"
        missing_edge = draw_minimal_graph(result, minimal_graph_path)
        if missing_edge:
            print(
                f"[ITER {iteration}] Missing edge to target → {missing_edge}"
            )

        replay_cfg = build_replay_config(iter_cfg, result)
        replay_outcome: Optional[bool] = None
        if replay_cfg:
            replay_cfg_path = iter_dir / "replay.yaml"
            save_yaml(replay_cfg, replay_cfg_path)
            print(
                f"[ITER {iteration}] Deterministic replay config → {replay_cfg_path}"
            )
            if args.run_replay and result.feedback.near_cycles:
                replay_outcome = evaluate_replay(
                    runner_cmd,
                    replay_cfg_path,
                    iter_dir,
                    result.feedback.near_cycles[0],
                    context,
                    args.max_near_cycles,
                )
                if replay_outcome is True:
                    print(
                        f"[ITER {iteration}] Replay closed edge {result.feedback.near_cycles[0].end}→{result.feedback.near_cycles[0].start}"
                    )
                elif replay_outcome is False:
                    print(
                        f"[ITER {iteration}] Replay did not close edge {result.feedback.near_cycles[0].end}→{result.feedback.near_cycles[0].start}; boosting priority"
                    )
                else:
                    print(f"[ITER {iteration}] Replay execution failed; will retry later")
        print(
            f"[ITER {iteration}] edges={sum(result.edge_counts.values())} "
            f"edges/txn={result.edges_per_txn:.3f} cycles={len(result.cycles)} "
            f"near={len(result.feedback.near_cycles)}"
        )
        if args.verbose:
            print(f"[ITER {iteration}] suggestions:")
            for suggestion in result.feedback.suggestions:
                print(f"    {suggestion}")

        for idx, nc in enumerate(result.feedback.near_cycles):
            cycle_key = (nc.start, nc.via, nc.end)
            attempts = context.replay_failures.get(cycle_key, 0)
            result.feedback.near_cycles[idx] = replace(nc, replay_attempts=attempts)

        save_analysis_json(result, analysis_path)

        current_cfg = build_next_config(iter_cfg, result, context)
        next_mix = current_cfg.get("workload", {}).get("mix", [])
        sched = current_cfg.get("scheduling", {})
        mix_str = ", ".join(
            f"{item.get('txn')}:{item.get('weight')}" for item in next_mix
        )
        summary_msg = (
            f"[ITER {iteration}] Next workload → clients={sched.get('clients')} "
            f"barrier_every={sched.get('barrier_every', 0)} mix=[{mix_str}]"
        )
        print(summary_msg)
        if args.verbose:
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
