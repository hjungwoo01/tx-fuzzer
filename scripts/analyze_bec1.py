"""
analyze_bec1.py
Implements BEC-1: Finds the SINGLE best RW edge to force a cycle.
Outputs: bec1_target.json
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

try:
    import networkx as nx
    from edn_format import Keyword, Symbol, loads
except ImportError as exc:
    raise SystemExit("[ERROR] Missing dependency. pip install networkx edn_format") from exc

try:
    from edn_format import Keyword, Symbol, loads
except ImportError as exc:
    raise SystemExit(
        "[ERROR] Missing dependency edn_format. Install with `pip install edn_format`."
    ) from exc

@dataclass(frozen=True)
class Operation:
    op_type: str # "r" or "w"
    key: Any
    value: Any

@dataclass
class Transaction:
    txn_id: str
    ops: List[Operation]
    index: int
    txn_name: Optional[str] = None 

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


def _summarize_txn_ops(txn: Transaction) -> Dict[str, Any]:
    """
    Compute basic per-transaction patterns to help classify anomaly types.

    We stay intentionally generic here and only look at Elle-level ops
    (reads/writes + keys), not SQL strings, so this works for both the
    Lost Update (G1) and Write Skew (G2) workloads.
    """
    reads: List[Tuple[int, Operation]] = []
    writes: List[Tuple[int, Operation]] = []
    for idx, op in enumerate(txn.ops):
        if op.op_type == "r":
            reads.append((idx, op))
        elif op.op_type == "w":
            writes.append((idx, op))

    read_keys = [op.key for _, op in reads]
    write_keys = [op.key for _, op in writes]
    read_set = set(read_keys)
    write_set = set(write_keys)

    # Did this transaction perform SELECT ... then UPDATE ... on the *same* key?
    read_then_write_same_key = False
    for r_idx, r in reads:
        for w_idx, w in writes:
            if r.key == w.key and r_idx < w_idx:
                read_then_write_same_key = True
                break
        if read_then_write_same_key:
            break

    # Classic write-skew shape: reads and writes are on disjoint key-sets.
    disjoint_read_write = bool(read_set) and bool(write_set) and read_set.isdisjoint(write_set)

    return {
        "read_keys": read_set,
        "write_keys": write_set,
        "read_then_write_same_key": read_then_write_same_key,
        "disjoint_read_write": disjoint_read_write,
    }


def classify_cycle_pattern(
    transactions: Sequence[Transaction],
    best: EdgeCandidate,
    has_cycle: bool,
) -> Dict[str, Any]:
    """
    Very lightweight heuristic classifier for the anomaly type behind the
    selected BEC-1 edge.

    - Case A (Lost Update / G1):
        If either side of the edge does SELECT (read) *then* UPDATE (write)
        on the *same* key as the RW edge, we tag this as a Lost Update style
        cycle.

    - Case B (Write Skew / G2):
        If both sides READ key A and WRITE key B where A != B, we tag this
        as a Write Skew pattern.
    """
    if not has_cycle or best is None:
        return {"kind": "none", "reason": "no_cycle"}

    by_id: Dict[str, Transaction] = {t.txn_id: t for t in transactions}
    reader_txn = by_id.get(best.reader)
    writer_txn = by_id.get(best.writer)
    if reader_txn is None or writer_txn is None:
        return {"kind": "unknown", "reason": "missing_txn"}

    r_summary = _summarize_txn_ops(reader_txn)
    w_summary = _summarize_txn_ops(writer_txn)

    key = best.key

    # --- Case A: Lost Update (G1) ---
    lost_update = False
    for summary in (r_summary, w_summary):
        if (
            key in summary["read_keys"]
            and key in summary["write_keys"]
            and summary["read_then_write_same_key"]
        ):
            lost_update = True
            break

    if lost_update:
        return {
            "kind": "lost_update",
            "reason": "select_then_update_same_key",
        }

    # --- Case B: Write Skew (G2) ---
    # Both txns read a keyset that is disjoint from what they write.
    if r_summary["disjoint_read_write"] and w_summary["disjoint_read_write"]:
        return {
            "kind": "write_skew",
            "reason": "read_key_a_write_key_b_disjoint",
        }

    return {"kind": "unknown", "reason": "pattern_not_matched"}

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
    try:
        raw_text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        print(f"Error: {path} not found.")
        return []
        
    stripped = raw_text.strip()
    items: List[Any] = []
    if stripped.startswith("["):
        parsed = loads(raw_text)
        items = list(parsed if isinstance(parsed, Sequence) else [parsed])
    else:
        for raw_line in raw_text.splitlines():
            line = raw_line.strip()
            if not line or line.startswith(";"): continue
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
        
        txn_name = meta.get("txn_name") or meta.get("txn-name")
        if txn_name is not None:
            txn_name = str(txn_name)
        counter += 1
        transactions.append(Transaction(txn_id=txn_id, ops=ops, index=len(transactions), txn_name=txn_name))
    return transactions


def prune_history(transactions):
    """
    Optimization: Discard transactions that ONLY touch keys 
    which never appear again in the entire history.
    """
    key_counts = {}
    
    # Pass 1: Count key usage across all operations in all transactions
    for txn in transactions:
        # Check if the transaction has an 'ops' attribute (standard Jepsen model)
        if hasattr(txn, "ops"):
            for op in txn.ops:
                # Ensure op has a key and it's not None
                if hasattr(op, "key") and op.key is not None:
                    key_counts[op.key] = key_counts.get(op.key, 0) + 1
                    
    # Pass 2: Keep transactions that touch at least one "active" key
    relevant_txns = []
    for txn in transactions:
        is_relevant = False
        
        # If any operation in this transaction touches a popular key, keep the txn
        if hasattr(txn, "ops"):
            for op in txn.ops:
                if hasattr(op, "key") and op.key is not None:
                    if key_counts.get(op.key, 0) > 1:
                        is_relevant = True
                        break
        
        if is_relevant:
            relevant_txns.append(txn)
            
    print(f"[BEC-1] Pruning: Reduced {len(transactions)} -> {len(relevant_txns)} transactions (removed single-touch keys)")
    return relevant_txns

def build_graph_and_candidates(
    transactions: Sequence[Transaction]) -> Tuple[nx.DiGraph, List[EdgeCandidate]]:
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
                # Add WR Edges (Writer -> Reader)
                if op.value is not None:
                    for writer in writes_by_key_value.get(op.key, {}).get(op.value, []):
                        if writer != txn.txn_id:
                            graph.add_edge(writer, txn.txn_id)
                pending_reads[op.key].append(ReadRecord(txn_id=txn.txn_id, value=op.value))
            
            elif op.op_type == "w":
                # Add WW Edges (Last Writer -> Current Writer)
                last_writer = last_writer_by_key.get(op.key)
                if last_writer and last_writer != txn.txn_id:
                    graph.add_edge(last_writer, txn.txn_id)
                
                # Identify RW Candidates (Past Reader -> Current Writer)
                for read_rec in pending_reads.get(op.key, []):
                    if read_rec.txn_id == txn.txn_id:
                        continue
                    if read_rec.value is not None and read_rec.value == op.value:
                        continue
                    
                    candidates.append(EdgeCandidate(
                        reader=read_rec.txn_id,
                        writer=txn.txn_id,
                        key=op.key,
                        read_value=read_rec.value,
                        write_value=op.value
                    ))
                last_writer_by_key[op.key] = txn.txn_id
                
    return graph, candidates

def pick_best_bec1(
    graph: nx.DiGraph, candidates: Iterable[EdgeCandidate]
) -> Tuple[Optional[EdgeCandidate], bool]:
    best_cand = None
    min_cycle_len = float('inf')
    
    best_suspicion = None
    min_suspicion_delta = float('inf')  # SMALLEST gap

    candidate_list = list(candidates)
    total = len(candidate_list)
    
    if total == 0:
        return None
    
    print(f"[BEC-1] Evaluating {total} candidate edges...", file=sys.stderr)
    
    if len(graph.nodes) <= 1000:
        max_candidates = 200
    else:
        max_candidates = 50
    if total > max_candidates:
        print(f"[BEC-1] Optimization: Checking top {max_candidates} of {total} candidates to prevent timeout.", file=sys.stderr)
        candidate_list = candidate_list[:max_candidates]
    

    use_precomputed = len(graph.nodes) <= 1000
    precomputed_paths = {}
    
    if use_precomputed:
        print(f"[BEC-1] Precomputing shortest paths (graph has {len(graph.nodes)} nodes)...", file=sys.stderr)
        try:
            precomputed_paths = dict(nx.all_pairs_shortest_path_length(graph))
            print(f"[BEC-1] Precomputation complete", file=sys.stderr)
        except Exception as e:
            print(f"[BEC-1] Precomputation failed, falling back to on-demand: {e}", file=sys.stderr)
            use_precomputed = False
    
    for i, cand in enumerate(candidate_list):
        if cand.reader == cand.writer: continue
        
        if len(candidate_list) > 100 and i % 50 == 0:
            print(f"[BEC-1] Progress: {i}/{len(candidate_list)} candidates evaluated...", file=sys.stderr)
        
        try:
            if use_precomputed:
                writer_paths = precomputed_paths.get(cand.writer, {})
                path_len = writer_paths.get(cand.reader, -1)
                if path_len == -1:
                    raise nx.NetworkXNoPath
            else:
                path_len = nx.shortest_path_length(graph, cand.writer, cand.reader)
            
            cycle_len = path_len + 1
            if cycle_len < min_cycle_len:
                min_cycle_len = cycle_len
                best_cand = cand
                if cycle_len <= 3: 
                    print(f"[BEC-1] Found short cycle (len {min_cycle_len}), stopping early", file=sys.stderr)
                    break

        except (nx.NetworkXNoPath, nx.NodeNotFound):
            try:
                reader_idx = graph.nodes[cand.reader]['index']
                writer_idx = graph.nodes[cand.writer]['index']
                
                delta = abs(writer_idx - reader_idx)
                
                if delta < min_suspicion_delta:
                    min_suspicion_delta = delta
                    best_suspicion = cand
            except KeyError:
                continue

    if best_cand:
        print(f"[BEC-1] Found Cycle (len {min_cycle_len}) on Key {best_cand.key}")
        return best_cand, True
        
    if best_suspicion:
        print(f"[BEC-1] No Cycle. Suspicion on Key {best_suspicion.key} (Gap: {min_suspicion_delta} ops)")
        return best_suspicion, False
        
    return None, False

def normalize_key(key: Any) -> Any:
    if isinstance(key, str) and key.isdigit(): return int(key)
    return key

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="BEC-1 analysis: Find the single best RW edge to force a cycle")
    parser.add_argument(
        "--history",
        type=Path,
        default=Path("./history.edn"),
        help="Path to history EDN file (default: ./history.edn)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("bec1_target.json"),
        help="Output JSON path (default: bec1_target.json)",
    )
    parser.add_argument(
        "--max-transactions",
        type=int,
        default=None,
        help="Limit number of transactions to analyze (for large histories)",
    )
    args = parser.parse_args()
    
    history_path = args.history
    output_path = args.out

    print(f"[BEC-1] Loading history from {history_path}...", file=sys.stderr)
    transactions = extract_transactions(load_history(history_path))
    transactions = prune_history(transactions)
    original_count = len(transactions)
    print(f"[BEC-1] Extracted {original_count} transactions", file=sys.stderr)
    
    # Limit transactions if specified or if too many
    if args.max_transactions and len(transactions) > args.max_transactions:
        print(f"[BEC-1] Limiting to {args.max_transactions} transactions (from {original_count})", file=sys.stderr)
        transactions = transactions[:args.max_transactions]
    elif len(transactions) > 20000:
        # Auto-limit very large histories
        limit = 20000
        print(f"[BEC-1] Large history detected ({original_count} transactions), limiting to {limit} for performance", file=sys.stderr)
        transactions = transactions[:limit]
    
    print(f"[BEC-1] Building dependency graph...", file=sys.stderr)
    graph, candidates = build_graph_and_candidates(transactions)
    print(f"[BEC-1] Graph has {len(graph.nodes)} nodes, {len(graph.edges)} edges", file=sys.stderr)
    print(f"[BEC-1] Found {len(candidates)} candidate edges", file=sys.stderr)
    
    # Dedupe candidates
    unique_candidates = { (c.reader, c.writer, c.key): c for c in candidates }.values()
    unique_count = len(list(unique_candidates))
    if unique_count < len(candidates):
        print(f"[BEC-1] Deduplicated to {unique_count} unique candidates", file=sys.stderr)
    
    best, has_cycle = pick_best_bec1(graph, unique_candidates)
    
    if best:
        # Map txn_id -> workload template name for reader/writer
        txn_id_to_name: Dict[str, str] = {}
        for t in transactions:
            if t.txn_name:
                txn_id_to_name[t.txn_id] = t.txn_name
        p1_name = txn_id_to_name.get(best.reader) or best.reader
        p2_name = txn_id_to_name.get(best.writer) or best.writer

        payload: Dict[str, Any] = {
            "target_key": normalize_key(best.key),
            "p1_name": p1_name,
            "p2_name": p2_name,
            "strategy": "interleave",
        }

        # --- Patch Recommender: classify anomaly + suggest a patch ---
        classification = classify_cycle_pattern(transactions, best, has_cycle)
        red = "\033[91m"
        reset = "\033[0m"

        if has_cycle and classification.get("kind") == "lost_update":
            msg = (
                "Critical G1 Vulnerability detected. "
                "PATCH: Change your SQL to `SELECT ... FOR UPDATE` to lock rows during the read."
            )
            print(f"{red}[BEC-1][PATCH RECOMMENDER] {msg}{reset}", file=sys.stderr)
            payload["cycle_type"] = "G1_lost_update"
            payload["diagnosis"] = "Critical G1 Lost Update cycle detected."
            payload["patch_recommendation"] = (
                "Change your SQL to `SELECT ... FOR UPDATE` to lock rows during the read."
            )
        elif has_cycle and classification.get("kind") == "write_skew":
            msg = (
                "G2 Write Skew detected. "
                "PATCH: Switch isolation level to `SERIALIZABLE` or use predicate locking."
            )
            print(f"{red}[BEC-1][PATCH RECOMMENDER] {msg}{reset}", file=sys.stderr)
            payload["cycle_type"] = "G2_write_skew"
            payload["diagnosis"] = "G2 Write Skew cycle detected."
            payload["patch_recommendation"] = (
                "Switch isolation level to `SERIALIZABLE` or use predicate locking."
            )

        if not txn_id_to_name.get(best.reader) or not txn_id_to_name.get(best.writer):
            print(
                "[BEC-1] WARNING: History missing txn_name in meta for some actors; using txn_id. "
                "Ensure runner records :meta {:txn_name \"...\"}.",
                file=sys.stderr,
            )
    else:
        payload = {}

    output_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

if __name__ == "__main__":
    main()