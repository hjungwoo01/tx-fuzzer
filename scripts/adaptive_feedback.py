"""
Adaptive feedback loop for Elle transaction histories.

This module consumes the latest `history.edn`, builds a dependency graph,
highlights near-miss dependency cycles, and mutates the next workload to
intensify contention on the hottest keys.  Each iteration is archived under
`runs/iter_XX/` with the history, analysis artifacts, generated workload,
notes, and (optionally) the freshly executed follow-up run.

Usage example:

    python scripts/adaptive_feedback.py \
        --history history.edn \
        --prev-workload workloads/current.yaml \
        --output-root runs \
        --iterations 1 \
        --execute
"""

from __future__ import annotations

import argparse
import copy
import json
import os
import shutil
import subprocess
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

import networkx as nx
import yaml

try:
        from scripts.analyze_history import (  # type: ignore
            EdgeDetail,
            Transaction,
            build_dependency_graph as _build_dependency_graph,
            extract_transactions,
            load_history,
        )
except ImportError:
    try:  # pragma: no cover - fallback for execution inside scripts/
        from analyze_history import (  # type: ignore
            EdgeDetail,
            Transaction,
            build_dependency_graph as _build_dependency_graph,
            extract_transactions,
            load_history,
        )
    except ImportError as exc:  # pragma: no cover
        raise SystemExit(
            "[ERROR] Could not import analyze_history. "
            "Run this module from the repository root."
        ) from exc


# ---------------------------------------------------------------------------
# Parsing & graph construction helpers
# ---------------------------------------------------------------------------


def parse_history(file_path: Path) -> List[Transaction]:
    """Parse an Elle history, returning committed transactions only."""
    events = load_history(file_path)
    transactions = extract_transactions(events)
    return transactions


@dataclass
class GraphArtifacts:
    graph: nx.DiGraph
    edges: Dict[str, List[EdgeDetail]]


def build_dependency_graph(transactions: Sequence[Transaction]) -> GraphArtifacts:
    """Construct a dependency graph (WR/WW/RW edges) using NetworkX."""
    graph, edges = _build_dependency_graph(transactions)
    return GraphArtifacts(graph=graph, edges=edges)


# ---------------------------------------------------------------------------
# Analysis routines
# ---------------------------------------------------------------------------


def find_near_miss_cycles(
    graph: nx.DiGraph,
    templates: Optional[Mapping[str, Optional[str]]] = None,
) -> List[Dict[str, Any]]:
    """
    Detect length-2 paths (A -> B -> C) where the closing edge (C -> A) is absent.

    Returns a list of dicts ordered by frequency, each containing:
        - start (A), via (B), end (C)
        - key hints collected from the existing edges
        - count (frequency)
    """
    triplet_counter: Counter[Tuple[str, str, str]] = Counter()
    key_hints: Dict[Tuple[str, str, str], List[Any]] = defaultdict(list)

    for src, mid in graph.edges():
        rels = graph[src][mid].get("relationships", [])
        src_mid_keys = [rel.key for rel in rels if rel.key is not None]
        for dst in graph.successors(mid):
            if dst == src:
                continue
            if graph.has_edge(dst, src):
                continue
            triplet = (src, mid, dst)
            triplet_counter[triplet] += 1
            rels_mid_dst = graph[mid][dst].get("relationships", [])
            mid_dst_keys = [rel.key for rel in rels_mid_dst if rel.key is not None]
            key_hints[triplet].extend(src_mid_keys or mid_dst_keys)

    ranked: List[Dict[str, Any]] = []
    templates = templates or {}
    for (start, via, end), count in triplet_counter.most_common():
        keys = key_hints.get((start, via, end)) or []
        ranked.append(
            {
                "start": start,
                "via": via,
                "end": end,
                "keys": list(dict.fromkeys(keys)),  # preserve order, dedupe
                "count": count,
                "start_template": templates.get(start),
                "via_template": templates.get(via),
                "end_template": templates.get(end),
            }
        )
    return ranked


def analyze_hot_keys(graph: nx.DiGraph, top_n: int = 5) -> List[Any]:
    """Return the most common keys appearing on dependency edges."""
    counter: Counter[Any] = Counter()
    for u, v, data in graph.edges(data=True):
        for rel in data.get("relationships", []):
            if rel.key is not None:
                counter[rel.key] += 1
    return [key for key, _ in counter.most_common(top_n)]


def summarize_edge_stats(artifacts: GraphArtifacts) -> Dict[str, Any]:
    """Aggregate useful metrics for reporting."""
    edge_counts = {dep: len(details) for dep, details in artifacts.edges.items()}
    total_edges = sum(edge_counts.values())
    sccs = [
        sorted(comp)
        for comp in nx.strongly_connected_components(artifacts.graph)
        if len(comp) > 1
    ]
    return {
        "edge_counts": edge_counts,
        "total_edges": total_edges,
        "sccs": sccs,
    }


# ---------------------------------------------------------------------------
# Workload mutation helpers
# ---------------------------------------------------------------------------


def _ensure_list(obj: Iterable[Any]) -> List[Any]:
    return list(obj) if isinstance(obj, Iterable) and not isinstance(obj, (str, bytes)) else [obj]


def _coerce_keys(sample_args: List[Any], hot_keys: List[Any]) -> List[Any]:
    """
    Try to preserve the scalar type (int vs str) when narrowing key choices.
    """
    if not hot_keys:
        return []
    canonical = hot_keys
    if sample_args:
        exemplar = sample_args[0]
        if isinstance(exemplar, int):
            canonical = [int(k) for k in hot_keys]
        elif isinstance(exemplar, str):
            canonical = [str(k) for k in hot_keys]
    return canonical


def _mutate_args(args: List[Any], hot_keys: List[Any], changes: List[str]) -> None:
    """Restrict key-choice arguments to the provided hot_keys."""
    if not hot_keys:
        return
    for idx, arg in enumerate(args):
        if isinstance(arg, dict) and "choice" in arg:
            choice = arg["choice"]
            if isinstance(choice, dict) and "from" in choice:
                original = _ensure_list(choice.get("from", []))
                narrowed = _coerce_keys(original, hot_keys)
                if narrowed and narrowed != original:
                    choice["from"] = narrowed
                    changes.append(f"Restricted choice list {original} → {narrowed}")
        elif isinstance(arg, (int, str)):
            if arg not in hot_keys and len(hot_keys) == 1:
                original = arg
                replacement = hot_keys[0]
                args[idx] = replacement
                changes.append(f"Replaced literal key {original} → {replacement}")


def _detect_writer_transactions(workload_cfg: Dict[str, Any]) -> set[str]:
    writers: set[str] = set()
    for txn in workload_cfg.get("transactions", []):
        name = txn.get("name")
        if not name:
            continue
        for step in txn.get("steps", []):
            elle = step.get("elle", {})
            if isinstance(elle, dict) and str(elle.get("f", "")).lower() in {":write", ":w"}:
                writers.add(name)
                break
    return writers


def generate_mutations(
    near_misses: List[Dict[str, Any]],
    hot_keys: List[Any],
    prev_workload_path: Path,
    changes: List[str],
) -> Dict[str, Any]:
    """Load and mutate the prior workload configuration."""
    with prev_workload_path.open("r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)

    workload = cfg.setdefault("workload", {})
    transactions = workload.get("transactions", [])
    for txn in transactions:
        for step in txn.get("steps", []):
            args = step.get("args", [])
            if isinstance(args, list):
                _mutate_args(args, hot_keys, changes)

    # Inject a follow-up SELECT to close the most frequent missing edge.
    if near_misses:
        first_miss = near_misses[0]
        target_key = (first_miss.get("keys") or hot_keys or [None])[0]
        if target_key is not None and transactions:
            target_txn = transactions[0]
            new_step = {
                "sql": "SELECT v FROM kv WHERE k=$1",
                "args": [{"choice": {"from": [target_key]}}],
                "elle": {"f": ":read", "key": "$1", "value_from_result_col": 0},
            }
            target_txn.setdefault("steps", []).append(new_step)
            changes.append(
                f"Appended follow-up SELECT in txn '{target_txn.get('name')}' "
                f"to encourage edge {first_miss['end']}→{first_miss['start']} on key {target_key}"
            )

    # Mix adjustments favouring writer-heavy transactions.
    mix = workload.get("mix", [])
    writer_txns = _detect_writer_transactions(workload)
    inflated = []
    for item in mix:
        name = item.get("txn")
        weight = float(item.get("weight", 1.0))
        if name in writer_txns:
            weight *= 1.2
        else:
            weight *= 0.9
        inflated.append(max(weight, 0.05))
    total = sum(inflated) or 1.0
    for item, weight in zip(mix, inflated):
        original = item.get("weight")
        item["weight"] = round(weight / total, 4)
        if original != item["weight"]:
            changes.append(
                f"Adjusted mix weight for txn '{item.get('txn')}' {original} → {item['weight']}"
            )

    scheduling = cfg.setdefault("scheduling", {})
    original_clients = int(scheduling.get("clients", 4))
    new_clients = min(original_clients + 2, 32)
    if new_clients != original_clients:
        scheduling["clients"] = new_clients
        changes.append(f"Increased clients {original_clients} → {new_clients}")
    else:
        scheduling["clients"] = original_clients

    original_barrier = scheduling.get("barrier_every")
    if original_barrier in (None, 0):
        scheduling["barrier_every"] = 2
        changes.append("Enabled barrier_every=2 to synchronize conflicting txns")


    return cfg


def build_replay_config(
    base_cfg: Dict[str, Any],
    near_miss: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if not near_miss:
        return None

    triplet = [
        ("start", near_miss.get("start_template")),
        ("via", near_miss.get("via_template")),
        ("end", near_miss.get("end_template")),
    ]
    if any(template is None for _, template in triplet):
        return None

    workload = base_cfg.get("workload", {})
    transactions = workload.get("transactions", [])
    template_names = {
        txn.get("name")
        for txn in transactions
        if isinstance(txn, dict)
    }
    for _, template in triplet:
        if template not in template_names:
            return None

    replay_cfg = copy.deepcopy(base_cfg)
    replay_cfg["replay"] = {
        "enabled": True,
        "transactions": [
            {"alias": alias, "txn": template}
            for alias, template in triplet
        ],
        "schedule": [
            {"alias": "start", "action": "step", "steps": 0},
            {"alias": "via", "action": "step", "steps": 0},
            {"alias": "end", "action": "step", "steps": 0},
            {"alias": "via", "action": "commit"},
            {"alias": "end", "action": "commit"},
            {"alias": "start", "action": "commit"},
        ],
    }

    focus_keys = near_miss.get("keys") or []
    if focus_keys:
        replay_cfg["replay"]["focus_key"] = focus_keys[0]
        dummy_changes: List[str] = []
        for txn in replay_cfg.get("workload", {}).get("transactions", []):
            for step in txn.get("steps", []):
                args = step.get("args", [])
                if isinstance(args, list):
                    _mutate_args(args, [focus_keys[0]], dummy_changes)

    scheduling = replay_cfg.setdefault("scheduling", {})
    scheduling["clients"] = 1
    scheduling.pop("barrier_every", None)
    return replay_cfg


# ---------------------------------------------------------------------------
# Reporting helpers
# ---------------------------------------------------------------------------


def write_analysis_json(path: Path, payload: Dict[str, Any]) -> None:
    with path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=False)


def document_changes(
    notes_path: Path,
    iteration_label: str,
    metrics: Dict[str, Any],
    hot_keys: List[Any],
    near_misses: List[Dict[str, Any]],
    changes: List[str],
) -> None:
    """Emit a Markdown summary of analysis metrics and workload mutations."""
    lines = [
        f"### {iteration_label} Summary",
        f"- **Transactions:** {metrics.get('transactions')} (edges={metrics['edge_counts']} total={metrics['total_edges']})",
        f"- **SCCs:** {len(metrics.get('sccs', []))} component(s)",
        f"- **Near-miss paths:** {len(near_misses)}",
        f"- **Hot keys:** {', '.join(map(str, hot_keys)) or 'n/a'}",
        "",
        "### Workload Changes",
    ]
    if changes:
        lines.extend(f"- {change}" for change in changes)
    else:
        lines.append("- No changes applied.")
    if near_misses:
        lines.extend(
            [
                "",
                "### Top Near-Miss Candidates",
            ]
        )
        for miss in near_misses[:5]:
            lines.append(
                f"- {miss['start']} → {miss['via']} → {miss['end']} "
                f"(keys={', '.join(map(str, miss.get('keys', []))) or 'n/a'}, count={miss['count']})"
            )

    notes_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def visualize_graph(graph: nx.DiGraph, path: Path, max_nodes: int = 40) -> None:
    """Render a manageable subset of the dependency graph."""
    try:
        import matplotlib.pyplot as plt  # type: ignore
    except ImportError as exc:  # pragma: no cover
        print(f"[WARN] matplotlib not available; skipping graph render: {exc}")
        return

    nodes_sorted = sorted(
        graph.nodes,
        key=lambda n: graph.nodes[n].get("index", 0),
    )
    subset = nodes_sorted[:max_nodes]
    subgraph = graph.subgraph(subset).copy()

    plt.figure(figsize=(12, 8))
    pos = nx.spring_layout(subgraph, seed=42)
    nx.draw_networkx_nodes(subgraph, pos, node_size=900, node_color="#1f77b4", alpha=0.85)
    nx.draw_networkx_labels(subgraph, pos, font_size=8, font_color="white")

    edge_colors = [
        "#d62728" if "rw" in data.get("types", set()) else "#2ca02c"
        for _, _, data in subgraph.edges(data=True)
    ]
    nx.draw_networkx_edges(
        subgraph,
        pos,
        arrows=True,
        arrowstyle="->",
        arrowsize=12,
        edge_color=edge_colors,
    )
    edge_labels = {(u, v): data.get("label", "") for u, v, data in subgraph.edges(data=True)}
    nx.draw_networkx_edge_labels(subgraph, pos, edge_labels=edge_labels, font_size=7)
    plt.tight_layout()
    plt.savefig(path, dpi=200)
    plt.close()


def draw_minimal_near_miss(
    graph: nx.DiGraph,
    near_misses: List[Dict[str, Any]],
    path: Path,
) -> Optional[str]:
    """Render a 3-node subgraph representing the top near-miss candidate."""
    if not near_misses:
        return None
    try:
        import matplotlib.pyplot as plt  # type: ignore
    except ImportError as exc:  # pragma: no cover
        print(f"[WARN] matplotlib not available; skipping minimal graph render: {exc}")
        return None

    candidate = near_misses[0]
    nodes = {candidate["start"], candidate["via"], candidate["end"]}
    subgraph = nx.DiGraph()
    subgraph.add_nodes_from(nodes)

    def _copy_edge(src: str, dst: str) -> None:
        if graph.has_edge(src, dst):
            data = graph[src][dst]
            subgraph.add_edge(src, dst, label=data.get("label", ""), missing=False)

    _copy_edge(candidate["start"], candidate["via"])
    _copy_edge(candidate["via"], candidate["end"])

    label = "add conflicting edge"
    if candidate.get("keys"):
        label = f"add edge on key {candidate['keys'][0]}"
    subgraph.add_edge(candidate["end"], candidate["start"], label=label, missing=True)

    plt.figure(figsize=(6, 4))
    pos = nx.circular_layout(subgraph)
    nx.draw_networkx_nodes(subgraph, pos, node_size=1400, node_color="#1f77b4", alpha=0.9)
    nx.draw_networkx_labels(subgraph, pos, font_size=10, font_color="white")

    existing_edges = [(u, v) for u, v, d in subgraph.edges(data=True) if not d.get("missing")]
    missing_edges = [(u, v) for u, v, d in subgraph.edges(data=True) if d.get("missing")]

    nx.draw_networkx_edges(
        subgraph,
        pos,
        edgelist=existing_edges,
        arrows=True,
        arrowstyle="->",
        arrowsize=18,
        edge_color="#2ca02c",
        width=2.0,
    )
    if missing_edges:
        nx.draw_networkx_edges(
            subgraph,
            pos,
            edgelist=missing_edges,
            arrows=True,
            arrowstyle="->",
            arrowsize=22,
            edge_color="#d62728",
            width=2.5,
            style="dashed",
        )
    edge_labels = {(u, v): d.get("label", "") for u, v, d in subgraph.edges(data=True)}
    nx.draw_networkx_edge_labels(subgraph, pos, edge_labels=edge_labels, font_size=9)
    plt.title("Top Near-Miss Dependency", fontsize=12)
    plt.tight_layout()
    plt.savefig(path, dpi=200)
    plt.close()
    return f"{candidate['end']} → {candidate['start']} ({label})"


# ---------------------------------------------------------------------------
# Iteration orchestration
# ---------------------------------------------------------------------------


def determine_next_iteration(output_root: Path) -> int:
    """Return the next iteration index based on existing run directories."""
    existing = []
    for entry in output_root.glob("iter_*"):
        if entry.is_dir():
            try:
                existing.append(int(entry.name.split("_")[-1]))
            except ValueError:
                continue
    return (max(existing) + 1) if existing else 1


def run_next_iteration(workload_path: Path, history_out: Path) -> subprocess.CompletedProcess[str]:
    """Execute the Go runner inside the tx-fuzzer container."""
    cmd = [
        "docker",
        "exec",
        "tx-fuzzer",
        "env",
        "GOTOOLCHAIN=auto",
        "go",
        "run",
        "./cmd/runner",
        "-config",
        str(workload_path),
        "-out",
        str(history_out),
    ]
    env = os.environ.copy()
    env.setdefault(
        "DATABASE_URL",
        "postgres://postgres:postgres@txfuzz-pg:5432/postgres?sslmode=disable",
    )
    print("[EXEC]", " ".join(cmd))
    return subprocess.run(cmd, check=True, text=True, capture_output=False, env=env)


# ---------------------------------------------------------------------------
# Main driver
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Adaptive feedback loop that mutates workloads based on Elle histories."
    )
    parser.add_argument(
        "--history",
        type=Path,
        default=Path("history.edn"),
        help="Path to the latest history.edn file.",
    )
    parser.add_argument(
        "--prev-workload",
        type=Path,
        default=Path("workloads/current.yaml"),
        help="Workload YAML that produced the history.",
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("runs"),
        help="Directory where iteration artifacts are stored.",
    )
    parser.add_argument(
        "--workload-root",
        type=Path,
        default=Path("workloads"),
        help="Directory where generated workload YAML files are written.",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=1,
        help="Number of feedback iterations to perform.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Run the generated workload automatically inside docker.",
    )
    parser.add_argument(
        "--graph-max-nodes",
        type=int,
        default=40,
        help="Maximum nodes to include in the rendered dependency graph.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    history_path = args.history.resolve()
    workload_path = args.prev_workload.resolve()
    output_root = args.output_root.resolve()
    workload_root = args.workload_root.resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    workload_root.mkdir(parents=True, exist_ok=True)

    next_index = determine_next_iteration(output_root)

    for iteration in range(args.iterations):
        iteration_index = next_index + iteration
        iteration_label = f"iter_{iteration_index:02d}"
        iter_dir = output_root / iteration_label
        iter_dir.mkdir(parents=True, exist_ok=True)

        if not history_path.exists():
            raise SystemExit(f"[ERROR] History file not found: {history_path}")
        if not workload_path.exists():
            raise SystemExit(f"[ERROR] Workload file not found: {workload_path}")

        print(f"[INFO] Processing {iteration_label} using {history_path} and {workload_path}")

        with workload_path.open("r", encoding="utf-8") as cfg_fh:
            base_workload_cfg = yaml.safe_load(cfg_fh)

        transactions = parse_history(history_path)
        artifacts = build_dependency_graph(transactions)

        template_lookup = {txn.txn_id: txn.template for txn in transactions}
        near_misses = find_near_miss_cycles(artifacts.graph, template_lookup)
        hot_keys = analyze_hot_keys(artifacts.graph, top_n=5)
        stats = summarize_edge_stats(artifacts)
        stats["transactions"] = len(transactions)
        stats["near_misses"] = len(near_misses)
        stats["hot_keys"] = hot_keys

        # Persist baseline artifacts
        shutil.copy2(history_path, iter_dir / "history.edn")
        shutil.copy2(workload_path, iter_dir / "input_workload.yaml")
        visualize_graph(
            artifacts.graph,
            iter_dir / "dependency_graph.png",
            max_nodes=args.graph_max_nodes,
        )
        focus_edge = draw_minimal_near_miss(
            artifacts.graph,
            near_misses,
            iter_dir / "minimal_dependency_graph.png",
        )
        if focus_edge:
            print(f"[INFO] Top near-miss edge to target: {focus_edge}")

        analysis_json_path = iter_dir / "analysis.json"
        write_analysis_json(analysis_json_path, stats)

        replay_cfg = build_replay_config(base_workload_cfg, near_misses[0] if near_misses else None)
        if replay_cfg:
            replay_path = iter_dir / "replay.yaml"
            with replay_path.open("w", encoding="utf-8") as fh:
                yaml.safe_dump(replay_cfg, fh, sort_keys=False, default_flow_style=False)
            print(f"[INFO] Replay config saved to {replay_path}")

        changes: List[str] = []
        mutated_cfg = generate_mutations(near_misses, hot_keys, workload_path, changes)
        new_workload_path = iter_dir / "workload.yaml"
        with new_workload_path.open("w", encoding="utf-8") as fh:
            yaml.safe_dump(mutated_cfg, fh, sort_keys=False, default_flow_style=False)
        # Persist a copy in the workloads directory for convenience.
        workload_copy_path = workload_root / f"{iteration_label}.yaml"
        with workload_copy_path.open("w", encoding="utf-8") as fh:
            yaml.safe_dump(mutated_cfg, fh, sort_keys=False, default_flow_style=False)

        notes_path = iter_dir / "notes.md"
        document_changes(
            notes_path,
            iteration_label.title().replace("_", " "),
            stats,
            hot_keys,
            near_misses,
            changes,
        )

        print(f"[INFO] Generated next workload at {new_workload_path}")
        if args.execute:
            next_history_path = iter_dir / "next_history.edn"
            try:
                run_next_iteration(new_workload_path, next_history_path)
                history_path = next_history_path
                workload_path = new_workload_path
                print(f"[INFO] Execution complete; next history at {history_path}")
            except subprocess.CalledProcessError as exc:
                print(f"[ERROR] Runner failed: {exc}", file=sys.stderr)
                return exc.returncode
        else:
            # Prepare for potential manual follow-up.
            history_path = iter_dir / "history.edn"
            workload_path = new_workload_path

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
