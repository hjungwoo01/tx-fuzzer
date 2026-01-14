"""
Analyze a Jepsen history, find a "best" RW edge, and emit target_profile.json.

This script:
  1) Parses history.edn.
  2) Builds WR/WW dependency edges.
  3) Identifies RW (anti-dependency) candidates.
  4) Picks the candidate that closes the smallest cycle via BFS.
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

try:
    import networkx as nx
except ImportError as exc:
    raise SystemExit(
        "[ERROR] Missing dependency networkx. Install with `pip install networkx`."
    ) from exc

try:
    from edn_format import Keyword, Symbol, loads
except ImportError as exc:
    raise SystemExit(
        "[ERROR] Missing dependency edn_format. Install with `pip install edn_format`."
    ) from exc


@dataclass(frozen=True)
class Operation:
    op_type: str  # "r" or "w"
    key: Any
    value: Any


@dataclass
class Transaction:
    txn_id: str
    ops: List[Operation]
    index: int


@dataclass(frozen=True)
class ReadRecord:
    txn_id: str
    value: Any


@dataclass(frozen=True)
class EdgeCandidate:
    reader: str
    writer: str
    key: Any
    read_value: Any
    write_value: Any


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
    """Parse EDN history into a list of operation maps."""
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


def looks_like_txn(value: Any) -> bool:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return False
    if not value:
        return False
    head = value[0]
    if isinstance(head, Mapping):
        return True
    if isinstance(head, Sequence) and not isinstance(head, (str, bytes)):
        return True
    return False


def normalize_operation(item: Any) -> Optional[Operation]:
    if isinstance(item, Mapping):
        op_type = item.get("type") or item.get("op") or item.get("f")
        if op_type is None:
            return None
        op_str = str(op_type).lower().lstrip(":")
        key = item.get("key") if "key" in item else item.get("k")
        value = item.get("value") if "value" in item else item.get("v")
    elif isinstance(item, Sequence) and not isinstance(item, (str, bytes)):
        seq = [convert_edn(elem) for elem in item]
        if not seq:
            return None
        op_str = str(seq[0]).lower().lstrip(":")
        key = seq[1] if len(seq) > 1 else None
        value = seq[2] if len(seq) > 2 else None
    else:
        return None

    if op_str in {"r", "read"}:
        return Operation(op_type="r", key=key, value=value)
    if op_str in {"w", "write"}:
        return Operation(op_type="w", key=key, value=value)
    return None


def extract_transactions(events: Sequence[Mapping[str, Any]]) -> List[Transaction]:
    transactions: List[Transaction] = []
    counter = 0
    for event in events:
        event_type = str(event.get("type", "")).lower().lstrip(":")
        if event_type != "ok":
            continue
        raw_value = event.get("value")
        if not looks_like_txn(raw_value):
            continue
        event_f = event.get("f")
        if event_f is not None:
            f_str = str(event_f).lower().lstrip(":")
            if f_str != "txn":
                continue
        ops: List[Operation] = []
        for op_item in raw_value:
            normalized = normalize_operation(op_item)
            if normalized:
                ops.append(normalized)
        if not ops:
            continue
        meta = event.get("meta") if isinstance(event.get("meta"), Mapping) else {}
        raw_txn_id = event.get("txn") or event.get("txn_id") or meta.get("txn_id")
        txn_id = str(raw_txn_id) if raw_txn_id is not None else f"txn_{counter + 1}"
        counter += 1
        transactions.append(Transaction(txn_id=txn_id, ops=ops, index=len(transactions)))
    return transactions


def build_graph_and_candidates(
    transactions: Sequence[Transaction],
) -> Tuple[nx.DiGraph, List[EdgeCandidate]]:
    graph = nx.DiGraph()
    for txn in transactions:
        graph.add_node(txn.txn_id, index=txn.index)

    writes_by_key_value: Dict[Any, Dict[Any, List[str]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for txn in transactions:
        for op in txn.ops:
            if op.op_type == "w":
                writes_by_key_value[op.key][op.value].append(txn.txn_id)

    last_writer_by_key: Dict[Any, str] = {}
    pending_reads: Dict[Any, List[ReadRecord]] = defaultdict(list)
    candidates: List[EdgeCandidate] = []

    for txn in transactions:
        for op in txn.ops:
            if op.op_type == "r":
                if op.value is not None:
                    for writer in writes_by_key_value.get(op.key, {}).get(op.value, []):
                        if writer != txn.txn_id:
                            graph.add_edge(writer, txn.txn_id)
                pending_reads[op.key].append(ReadRecord(txn_id=txn.txn_id, value=op.value))
            elif op.op_type == "w":
                last_writer = last_writer_by_key.get(op.key)
                if last_writer and last_writer != txn.txn_id:
                    graph.add_edge(last_writer, txn.txn_id)
                for read_rec in pending_reads.get(op.key, []):
                    if read_rec.txn_id == txn.txn_id:
                        continue
                    if read_rec.value is not None and read_rec.value == op.value:
                        continue
                    candidates.append(
                        EdgeCandidate(
                            reader=read_rec.txn_id,
                            writer=txn.txn_id,
                            key=op.key,
                            read_value=read_rec.value,
                            write_value=op.value,
                        )
                    )
                last_writer_by_key[op.key] = txn.txn_id
    return graph, candidates


def dedupe_candidates(candidates: Iterable[EdgeCandidate]) -> List[EdgeCandidate]:
    seen: set[Tuple[str, str, Any]] = set()
    unique: List[EdgeCandidate] = []
    for candidate in candidates:
        key = (candidate.reader, candidate.writer, candidate.key)
        if key in seen:
            continue
        seen.add(key)
        unique.append(candidate)
    return unique


def pick_best_candidate(
    graph: nx.DiGraph, candidates: Iterable[EdgeCandidate]
) -> Optional[EdgeCandidate]:
    """
    Finds the edge to ADD.
    Priority 1: An edge that creates a cycle (An actual anomaly).
    Priority 2: An edge that connects nodes with the shortest existing path (The 'Almost' anomaly).
    """
    
    best_cycle_candidate: Optional[EdgeCandidate] = None
    min_cycle_len = float('inf')

    best_suspicion_candidate: Optional[EdgeCandidate] = None
    
    first_candidate = None

    for i, candidate in enumerate(candidates):
        if candidate.reader == candidate.writer:
            continue
        
        if first_candidate is None:
            first_candidate = candidate

        try:
            path_len = nx.shortest_path_length(graph, candidate.writer, candidate.reader)
            
            # CYCLE FOUND
            cycle_len = path_len + 1
            if cycle_len < min_cycle_len:
                min_cycle_len = cycle_len
                best_cycle_candidate = candidate
                
        except (nx.NetworkXNoPath, nx.NodeNotFound):
            if best_suspicion_candidate is None:
                best_suspicion_candidate = candidate
            continue

    # DECISION LOGIC
    if best_cycle_candidate:
        print(f"[ANALYSIS] SMOKING GUN: Found potential cycle (len {min_cycle_len}) on key {best_cycle_candidate.key}")
        return best_cycle_candidate

    if best_suspicion_candidate:
        print(f"[ANALYSIS] SUSPICION: No cycles yet. Targeting key {best_suspicion_candidate.key} to force contention.")
        return best_suspicion_candidate

    if first_candidate:
        print(f"[ANALYSIS] FALLBACK: Picking first available RW candidate on key {first_candidate.key}")
        return first_candidate

    return None


def normalize_target_key(key: Any) -> Any:
    if isinstance(key, str) and key.isdigit():
        try:
            return int(key)
        except ValueError:
            return key
    return key


def main() -> None:
    history_path = Path("history.edn")
    output_path = Path("target_profile.json")

    transactions = extract_transactions(load_history(history_path))
    graph, candidates = build_graph_and_candidates(transactions)
    best = pick_best_candidate(graph, dedupe_candidates(candidates))

    if best is None:
        output_path.write_text(json.dumps({}, indent=2) + "\n", encoding="utf-8")
        return

    payload = {
        "target_key": normalize_target_key(best.key),
        "strategy": "hotspot",
        "weight": 0.9,
    }
    output_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
