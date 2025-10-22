"""
Analyze Elle-style EDN histories and extract transaction dependency graphs.

Features:
    * Parse EDN histories (full vectors or newline-delimited maps).
    * Group operations into committed transactions.
    * Build wr/ww/rw dependency edges using NetworkX.
    * Detect serialization anomalies via strongly connected components.
    * Surface near-miss cycles and provide feedback to tighten workloads.
    * Persist a visual graph (PNG) for manual inspection.

Requires:
    pip install edn_format networkx matplotlib
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence, Tuple

try:
    import networkx as nx
except ImportError as exc:  # pragma: no cover - clear error path
    raise SystemExit(
        "[ERROR] Missing dependency networkx. Install with `pip install networkx`."
    ) from exc

try:
    import matplotlib.pyplot as plt
except ImportError as exc:  # pragma: no cover - clear error path
    raise SystemExit(
        "[ERROR] Missing dependency matplotlib. Install with `pip install matplotlib`."
    ) from exc

try:
    from edn_format import Keyword, Symbol, loads
except ImportError as exc:  # pragma: no cover - clear error path
    raise SystemExit(
        "[ERROR] Missing dependency edn_format. Install with `pip install edn_format`."
    ) from exc


@dataclass(frozen=True)
class Operation:
    """Single read/write within a transaction."""

    op_type: str  # 'r' or 'w'
    key: Any
    value: Any


@dataclass
class Transaction:
    """Committed transaction reconstructed from invoke/ok events."""

    txn_id: str
    process: Any
    status: str
    ops: List[Operation]
    start_time: Optional[int]
    end_time: Optional[int]
    index: int  # Commit order in history
    template: Optional[str]

    def writes(self) -> Iterable[Operation]:
        return (op for op in self.ops if op.op_type == "w")

    def reads(self) -> Iterable[Operation]:
        return (op for op in self.ops if op.op_type == "r")


@dataclass(frozen=True)
class EdgeDetail:
    """Metadata describing a dependency edge."""

    source: str
    target: str
    dep_type: str  # wr, ww, rw
    key: Any
    write_value: Any = None
    read_value: Any = None
    source_step: Optional[int] = None
    target_step: Optional[int] = None

    def label(self) -> str:
        value_bits = []
        if self.write_value is not None:
            value_bits.append(f"write={self.write_value}")
        if self.read_value is not None:
            value_bits.append(f"read={self.read_value}")
        value_info = f" ({', '.join(value_bits)})" if value_bits else ""
        return f"{self.dep_type.upper()} {self.source}->{self.target} key={self.key}{value_info}"


@dataclass(frozen=True)
class NearCycle:
    """Two-edge path that could close into a cycle with an additional dependency."""

    start: str
    via: str
    end: str
    key: Any
    start_edge_types: Tuple[str, ...]
    via_edge_types: Tuple[str, ...]
    start_template: Optional[str] = None
    via_template: Optional[str] = None
    end_template: Optional[str] = None
    start_step: Optional[int] = None
    via_step_from_start: Optional[int] = None
    via_step_to_end: Optional[int] = None
    end_step: Optional[int] = None
    replay_attempts: int = 0

    def describe(self) -> str:
        key_part = f"key {self.key}" if self.key is not None else "shared keys"
        start_types = "/".join(self.start_edge_types) if self.start_edge_types else "?"
        via_types = "/".join(self.via_edge_types) if self.via_edge_types else "?"
        tmpl_bits = []
        if self.start_template:
            tmpl_bits.append(f"{self.start}:{self.start_template}")
        if self.via_template:
            tmpl_bits.append(f"{self.via}:{self.via_template}")
        if self.end_template:
            tmpl_bits.append(f"{self.end}:{self.end_template}")
        tmpl_part = " | ".join(tmpl_bits)
        detail_bits = []
        if tmpl_part:
            detail_bits.append(f"templates {tmpl_part}")
        step_bits = []
        if self.start_step:
            step_bits.append(f"start@{self.start_step}")
        if self.via_step_from_start:
            step_bits.append(f"via@{self.via_step_from_start}")
        if self.via_step_to_end and self.via_step_to_end != self.via_step_from_start:
            step_bits.append(f"via#2@{self.via_step_to_end}")
        if self.end_step:
            step_bits.append(f"end@{self.end_step}")
        if step_bits:
            detail_bits.append("steps " + ", ".join(step_bits))
        detail = f" ({'; '.join(detail_bits)})" if detail_bits else ""
        return (
            f"{self.start}->{self.via}->{self.end} via {key_part} "
            f"(edges {start_types} then {via_types}){detail}"
        )


@dataclass
class FeedbackResult:
    near_cycles: List[NearCycle]
    suggestions: List[str]
    key_pressure: Dict[Any, int]
    truncated: bool = False


@dataclass
class AnalysisResult:
    transactions: List[Transaction]
    graph: nx.DiGraph
    edges: Dict[str, List[EdgeDetail]]
    cycles: List[List[str]]
    sccs: List[List[str]]
    feedback: FeedbackResult
    dependency_density: float
    complete_density: float
    edges_per_txn: float
    edge_counts: Dict[str, int]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a dependency graph from an Elle history."
    )
    parser.add_argument("history", type=Path, help="Path to history.edn")
    parser.add_argument(
        "--graph-output",
        type=Path,
        default=Path("dependency_graph.png"),
        help="Where to save the rendered dependency graph (PNG).",
    )
    parser.add_argument(
        "--max-edge-display",
        type=int,
        default=50,
        help="Maximum number of edges to print per dependency type.",
    )
    parser.add_argument(
        "--max-suggestions",
        type=int,
        default=10,
        help="Maximum near-miss paths to analyze when generating feedback.",
    )
    parser.add_argument(
        "--max-graph-nodes",
        type=int,
        default=200,
        help="Skip graph rendering when node count exceeds this threshold (0 disables).",
    )
    parser.add_argument(
        "--skip-graph",
        action="store_true",
        help="Disable dependency graph PNG rendering.",
    )
    return parser.parse_args()


def convert_edn(value: Any) -> Any:
    """Recursively convert EDN data structures into plain Python structures."""
    if isinstance(value, Keyword):
        return value.name
    if isinstance(value, Symbol):
        return value.name
    if isinstance(value, Mapping):
        return {convert_edn(k): convert_edn(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, Sequence)) and not isinstance(value, (str, bytes)):
        return [convert_edn(v) for v in value]
    if isinstance(value, set):
        return {convert_edn(v) for v in value}
    return value


def load_history(path: Path) -> List[Dict[str, Any]]:
    """Parse EDN history file into a list of ordered operation maps."""
    raw_text = path.read_text(encoding="utf-8")
    stripped = raw_text.strip()
    items: List[Any] = []
    if stripped.startswith("["):
        parsed = loads(raw_text)
        items = list(parsed if isinstance(parsed, Sequence) else [parsed])
    else:
        for raw_line in raw_text.splitlines():
            line = raw_line.strip()
            if not line or line.startswith(";"):
                continue
            items.append(loads(line))
    return [convert_edn(item) for item in items]


def normalize_operation(item: Any) -> Optional[Operation]:
    """Convert EDN operation tuples like [:r k v] into Operation objects."""
    if isinstance(item, Operation):
        return item
    if isinstance(item, Mapping):
        op_type = str(item.get("type") or item.get("op") or item.get("f")).lower()
        key = item.get("key")
        value = item.get("value")
        return Operation(op_type=op_type, key=key, value=value)
    if isinstance(item, Sequence) and not isinstance(item, (str, bytes)):
        seq = [convert_edn(elem) for elem in item]
        if not seq:
            return None
        op_type = str(seq[0]).lower().lstrip(":")
        key = seq[1] if len(seq) > 1 else None
        value = seq[2] if len(seq) > 2 else None
        return Operation(op_type=op_type, key=key, value=value)
    return None


def extract_transactions(events: Sequence[Mapping[str, Any]]) -> List[Transaction]:
    """Pair invoke/ok events into committed transactions."""
    inflight: Dict[Any, Dict[str, Any]] = {}
    transactions: List[Transaction] = []
    txn_counter = 0

    for idx, event in enumerate(events):
        event_type = str(event.get("type", "")).lower()
        process = event.get("process")
        if event_type == "invoke":
            inflight[process] = dict(event)
            inflight[process]["history_index"] = idx
            continue

        if event_type in {"ok", "fail"}:
            start = inflight.pop(process, {})
            status = event_type
            txn_id = (
                event.get("txn")
                or start.get("txn")
                or f"txn_{txn_counter + 1}"
            )
            txn_counter += 1
            raw_ops = event.get("value") or start.get("value") or []
            ops: List[Operation] = []
            for op_item in raw_ops:
                normalized = normalize_operation(op_item)
                if normalized and normalized.op_type in {"r", "read", "w", "write"}:
                    op_type = normalized.op_type[0]  # collapse read/write into r/w
                    ops.append(
                        Operation(
                            op_type=op_type,
                            key=normalized.key,
                            value=normalized.value,
                        )
                    )
            if status == "ok":
                meta = {}
                start_meta = start.get("meta") if isinstance(start, Mapping) else None
                if isinstance(start_meta, Mapping):
                    meta.update(start_meta)
                event_meta = event.get("meta") if isinstance(event, Mapping) else None
                if isinstance(event_meta, Mapping):
                    meta.update(event_meta)

                template_name = None
                if meta:
                    raw_name = meta.get("txn_name") or meta.get("name")
                    if isinstance(raw_name, str):
                        template_name = raw_name

                transactions.append(
                    Transaction(
                        txn_id=str(txn_id),
                        process=process,
                        status=status,
                        ops=ops,
                        start_time=start.get("time"),
                        end_time=event.get("time"),
                        index=len(transactions),
                        template=template_name,
                    )
                )
        # Ignore other terminal types like :info/:fail for now.
    return transactions


def build_dependency_graph(
    transactions: Sequence[Transaction],
) -> Tuple[nx.DiGraph, Dict[str, List[EdgeDetail]]]:
    graph = nx.DiGraph()
    edges: Dict[str, List[EdgeDetail]] = {"wr": [], "ww": [], "rw": []}

    txn_lookup: Dict[str, Transaction] = {txn.txn_id: txn for txn in transactions}
    for txn in transactions:
        graph.add_node(
            txn.txn_id,
            process=txn.process,
            status=txn.status,
            index=txn.index,
            start_time=txn.start_time,
            end_time=txn.end_time,
        )

    writes_by_key: Dict[Any, List[Dict[str, Any]]] = defaultdict(list)
    last_readers: Dict[Any, Dict[str, Any]] = defaultdict(dict)

    def record_edge(detail: EdgeDetail) -> None:
        dep_bucket = edges[detail.dep_type]
        if detail in dep_bucket:
            return
        dep_bucket.append(detail)
        graph.add_edge(detail.source, detail.target)
        edge_data = graph[detail.source][detail.target]
        edge_data.setdefault("relationships", []).append(detail)
        type_set = edge_data.setdefault("types", set())
        type_set.add(detail.dep_type)
        edge_data["label"] = ",".join(sorted(type_set))

    for txn in transactions:
        for op_index, op in enumerate(txn.ops):
            step_index = op_index + 1
            if op.op_type == "r":
                matching_writer: Optional[Dict[str, Any]] = None
                if op.value is not None:
                    for candidate in reversed(writes_by_key.get(op.key, [])):
                        if candidate["value"] == op.value:
                            matching_writer = candidate
                            break
                if matching_writer and matching_writer["txn_id"] != txn.txn_id:
                    detail = EdgeDetail(
                        source=matching_writer["txn_id"],
                        target=txn.txn_id,
                        dep_type="wr",
                        key=op.key,
                        write_value=matching_writer["value"],
                        read_value=op.value,
                        source_step=matching_writer.get("step_index"),
                        target_step=step_index,
                    )
                    record_edge(detail)
                last_readers[op.key][txn.txn_id] = {
                    "read_value": op.value,
                    "txn_index": txn.index,
                    "step_index": step_index,
                }
            elif op.op_type == "w":
                prior_writes = writes_by_key.get(op.key, [])
                if prior_writes:
                    last_writer = prior_writes[-1]
                    if last_writer["txn_id"] != txn.txn_id:
                        detail = EdgeDetail(
                            source=last_writer["txn_id"],
                            target=txn.txn_id,
                            dep_type="ww",
                            key=op.key,
                            write_value=op.value,
                            source_step=last_writer.get("step_index"),
                            target_step=step_index,
                        )
                        record_edge(detail)

                readers = last_readers.get(op.key, {})
                for reader_txn_id, info in readers.items():
                    if reader_txn_id == txn.txn_id:
                        continue
                    detail = EdgeDetail(
                        source=reader_txn_id,
                        target=txn.txn_id,
                        dep_type="rw",
                        key=op.key,
                        read_value=info.get("read_value"),
                        write_value=op.value,
                        source_step=info.get("step_index"),
                        target_step=step_index,
                    )
                    record_edge(detail)
                last_readers[op.key] = {}

                writes_by_key[op.key].append(
                    {
                        "txn_id": txn.txn_id,
                        "value": op.value,
                        "txn_index": txn.index,
                        "step_index": step_index,
                    }
                )
    return graph, edges


def derive_feedback(
    graph: nx.DiGraph,
    edges: Mapping[str, List[EdgeDetail]],
    transactions: Sequence[Transaction],
    max_paths: int = 10,
) -> FeedbackResult:
    if graph.number_of_nodes() == 0:
        return FeedbackResult(near_cycles=[], suggestions=[], key_pressure={})

    txn_lookup = {txn.txn_id: txn for txn in transactions}
    suggestions: List[str] = []
    near_cycles: List[NearCycle] = []
    seen_paths: set[Tuple[str, str, str]] = set()
    path_counter = 0
    truncated = False

    for src, mid in graph.edges():
        if truncated:
            break
        for dst in graph.successors(mid):
            if dst == src or graph.has_edge(dst, src):
                continue
            path = (src, mid, dst)
            if path in seen_paths:
                continue
            seen_paths.add(path)

            rels_src_mid = graph[src][mid].get("relationships", [])
            rels_mid_dst = graph[mid][dst].get("relationships", [])
            key_hint = None
            edge_sm = rels_src_mid[-1] if rels_src_mid else None
            edge_md = rels_mid_dst[-1] if rels_mid_dst else None
            if edge_sm and edge_sm.key is not None:
                key_hint = edge_sm.key
            if key_hint is None and edge_md and edge_md.key is not None:
                key_hint = edge_md.key

            start_edge_types = tuple(sorted(graph[src][mid].get("types", set())))
            via_edge_types = tuple(sorted(graph[mid][dst].get("types", set())))
            start_txn = txn_lookup.get(src)
            via_txn = txn_lookup.get(mid)
            end_txn = txn_lookup.get(dst)
            near_cycles.append(
                NearCycle(
                    start=src,
                    via=mid,
                    end=dst,
                    key=key_hint,
                    start_edge_types=start_edge_types,
                    via_edge_types=via_edge_types,
                    start_template=start_txn.template if start_txn else None,
                    via_template=via_txn.template if via_txn else None,
                    end_template=end_txn.template if end_txn else None,
                    start_step=edge_sm.source_step if edge_sm else None,
                    via_step_from_start=edge_sm.target_step if edge_sm else None,
                    via_step_to_end=edge_md.source_step if edge_md else None,
                    end_step=edge_md.target_step if edge_md else None,
                )
            )

            key_phrase = f"key {key_hint}" if key_hint is not None else "shared keys"
            suggestions.append(
                f"[SUGGESTION] Increase contention on {key_phrase} between {src} and {dst} to close a potential cycle via {mid}."
            )
            suggestions.append(
                "[SUGGESTION] Schedule "
                f"{dst} closer to {src} (reduced delay or weaker isolation like Read Committed) to encourage the missing dependency."
            )
            src_reads = sum(1 for op in txn_lookup[src].ops if op.op_type == "r")
            src_writes = sum(1 for op in txn_lookup[src].ops if op.op_type == "w")
            dst_reads = sum(1 for op in txn_lookup[dst].ops if op.op_type == "r")
            dst_writes = sum(1 for op in txn_lookup[dst].ops if op.op_type == "w")
            if src_reads + dst_reads and src_writes + dst_writes:
                suggestions.append(
                    "[SUGGESTION] Adjust read/write mix so "
                    f"{dst} performs an additional read of {src}'s updated {key_phrase}."
                )

            path_counter += 1
            if path_counter >= max_paths:
                truncated = True
                break

    key_pressure: Dict[Any, int] = defaultdict(int)
    for bucket in edges.values():
        for detail in bucket:
            if detail.key is None:
                continue
            key_pressure[detail.key] += 1

    underused_keys = [key for key, count in key_pressure.items() if count <= 1]
    if underused_keys:
        key_list = ", ".join(map(str, underused_keys[:5]))
        suggestions.append(
            "[SUGGESTION] Broaden contention by increasing overlapping reads and writes on keys "
            f"{key_list} to surface additional dependency edges."
        )

    return FeedbackResult(
        near_cycles=near_cycles,
        suggestions=suggestions,
        key_pressure=dict(key_pressure),
        truncated=truncated,
    )


def analyze_transactions(
    transactions: Sequence[Transaction],
    max_near_paths: int = 10,
) -> AnalysisResult:
    graph, edges = build_dependency_graph(transactions)
    cycles = list(nx.simple_cycles(graph))
    sccs = [
        sorted(component)
        for component in nx.strongly_connected_components(graph)
        if len(component) > 1
    ]
    feedback = derive_feedback(graph, edges, transactions, max_paths=max_near_paths)

    total_edges = sum(len(bucket) for bucket in edges.values())
    node_count = graph.number_of_nodes()
    txn_count = len(transactions)
    dependency_density = total_edges / node_count if node_count else 0.0
    complete_density = (
        total_edges / (node_count * (node_count - 1))
        if node_count > 1
        else 0.0
    )
    edges_per_txn = total_edges / txn_count if txn_count else 0.0
    edge_counts = {dep: len(bucket) for dep, bucket in edges.items()}

    return AnalysisResult(
        transactions=list(transactions),
        graph=graph,
        edges=edges,
        cycles=cycles,
        sccs=sccs,
        feedback=feedback,
        dependency_density=dependency_density,
        complete_density=complete_density,
        edges_per_txn=edges_per_txn,
        edge_counts=edge_counts,
    )


def analyze_history_file(
    history_path: Path,
    max_near_paths: int = 10,
) -> AnalysisResult:
    events = load_history(history_path)
    transactions = extract_transactions(events)
    if not transactions:
        empty_graph = nx.DiGraph()
        empty_edges = {"wr": [], "ww": [], "rw": []}
        return AnalysisResult(
            transactions=[],
            graph=empty_graph,
            edges=empty_edges,
            cycles=[],
            sccs=[],
            feedback=FeedbackResult(near_cycles=[], suggestions=[], key_pressure={}),
            dependency_density=0.0,
            complete_density=0.0,
            edges_per_txn=0.0,
            edge_counts={dep: 0 for dep in ("wr", "ww", "rw")},
        )
    return analyze_transactions(transactions, max_near_paths=max_near_paths)


def emit_metric_summary(result: AnalysisResult) -> None:
    total_edges = sum(result.edge_counts.values())
    print(f"[INFO] Transactions analyzed: {len(result.transactions)}")
    print(
        "[INFO] Total dependency edges: "
        f"{total_edges} "
        f"(WR={result.edge_counts.get('wr', 0)}, "
        f"WW={result.edge_counts.get('ww', 0)}, "
        f"RW={result.edge_counts.get('rw', 0)})"
    )
    print(
        "[INFO] Edge density per transaction: "
        f"{result.edges_per_txn:.3f}; graph density: {result.complete_density:.5f}"
    )
    if result.feedback.near_cycles:
        trunc_note = " (truncated)" if result.feedback.truncated else ""
        print(
            f"[INFO] Near-miss cycles considered: "
            f"{len(result.feedback.near_cycles)}{trunc_note}"
        )
    if result.feedback.key_pressure:
        top_items = sorted(
            result.feedback.key_pressure.items(), key=lambda kv: kv[1], reverse=True
        )[:5]
        formatted = ", ".join(f"{key}:{count}" for key, count in top_items)
        print(f"[INFO] Top key pressure → {formatted}")


def emit_edge_report(result: AnalysisResult, max_display: int = 50) -> None:
    for dep_type in ("wr", "ww", "rw"):
        bucket = result.edges.get(dep_type, [])
        print(f"[INFO] {dep_type.upper()} edges ({len(bucket)}):")
        for idx, detail in enumerate(bucket):
            if idx >= max_display:
                remaining = len(bucket) - max_display
                print(
                    f"    ... {remaining} more {dep_type.upper()} edge(s) omitted for brevity."
                )
                break
            print(f"    {detail.label()}")


def emit_cycle_report(result: AnalysisResult) -> None:
    if not result.cycles:
        print("[INFO] No directed cycles detected; history appears serializable.")
        return
    print(f"[INFO] Detected {len(result.cycles)} cycle(s) indicating potential anomalies:")
    for cycle in result.cycles:
        formatted = " -> ".join(cycle + [cycle[0]])
        print(f"    [CYCLE] {formatted} (possible G2-item)")


def emit_scc_report(result: AnalysisResult) -> None:
    if not result.sccs:
        return
    print("[INFO] Strongly connected components:")
    for comp in result.sccs:
        print(f"    {', '.join(comp)}")


def emit_feedback_report(result: AnalysisResult) -> None:
    fb = result.feedback
    if not fb.suggestions and not fb.near_cycles:
        print("[INFO] No feedback suggestions generated.")
        return

    if fb.near_cycles:
        print("[INFO] Near-miss paths:")
        display = min(5, len(fb.near_cycles))
        for nc in fb.near_cycles[:display]:
            print(f"    [NEAR] {nc.describe()}")
        if len(fb.near_cycles) > display:
            print(f"    ... {len(fb.near_cycles) - display} more near-cycle(s).")
        if fb.truncated:
            print(
                f"[INFO] Feedback truncated after analyzing {len(fb.near_cycles)} near-cycle paths."
            )

    for suggestion in fb.suggestions:
        print(suggestion)


def draw_graph(
    graph: nx.DiGraph,
    output_path: Path,
    max_nodes: int = 200,
) -> None:
    if graph.number_of_nodes() == 0:
        print("[WARN] No transactions to render; skipping graph export.")
        return
    if max_nodes and graph.number_of_nodes() > max_nodes:
        print(
            "[WARN] Graph has more than "
            f"{max_nodes} nodes; skipping PNG export to avoid oversized render."
        )
        return
    plt.figure(figsize=(max(6, graph.number_of_nodes()), 6))
    pos = nx.circular_layout(graph)
    nx.draw_networkx_nodes(graph, pos, node_color="#1f77b4", node_size=1200, alpha=0.9)
    nx.draw_networkx_labels(graph, pos, font_size=9, font_color="white")
    edge_colors = ["#d62728" if "rw" in data.get("types", set()) else "#2ca02c" for _, _, data in graph.edges(data=True)]
    nx.draw_networkx_edges(graph, pos, arrowstyle="->", arrowsize=20, edge_color=edge_colors)
    edge_labels = {
        (u, v): data.get("label", "")
        for u, v, data in graph.edges(data=True)
    }
    nx.draw_networkx_edge_labels(graph, pos, edge_labels=edge_labels, font_size=8)
    plt.tight_layout()
    plt.savefig(output_path, dpi=200)
    plt.close()
    print(f"[INFO] Saved dependency graph to {output_path}")


def main() -> int:
    args = parse_args()
    if not args.history.exists():
        print(f"[ERROR] History file {args.history} not found.", file=sys.stderr)
        return 1

    events = load_history(args.history)
    if not events:
        print("[WARN] No events parsed from history.")
        return 0

    transactions = extract_transactions(events)
    result = analyze_transactions(
        transactions,
        max_near_paths=max(args.max_suggestions, 0),
    )

    if not result.transactions:
        print("[WARN] No committed transactions detected in history.")
    emit_metric_summary(result)
    emit_edge_report(result, max_display=max(args.max_edge_display, 0))
    emit_cycle_report(result)
    emit_scc_report(result)
    emit_feedback_report(result)

    if not args.skip_graph:
        draw_graph(
            result.graph,
            args.graph_output,
            max_nodes=args.max_graph_nodes,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
