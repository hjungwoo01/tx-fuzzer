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
        counter += 1
        transactions.append(Transaction(txn_id=txn_id, ops=ops, index=len(transactions)))
    return transactions

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

def pick_best_bec1(graph: nx.DiGraph, candidates: Iterable[EdgeCandidate]) -> Optional[EdgeCandidate]:
    best_cand = None
    min_cycle_len = float('inf')
    
    best_suspicion = None
    min_suspicion_delta = float('inf')  # SMALLEST gap

    candidate_list = list(candidates)
    total = len(candidate_list)
    
    if total == 0:
        return None
    
    print(f"[BEC-1] Evaluating {total} candidate edges...", file=sys.stderr)
    
    # Limit candidates to 200
    max_candidates = 200
    if total > max_candidates:
        print(f"[BEC-1] Optimization: Checking top {max_candidates} of {total} candidates to prevent timeout.", file=sys.stderr)
        candidate_list = candidate_list[:max_candidates]
    
    use_precomputed = len(graph.nodes) < 1000
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
        return best_cand
        
    if best_suspicion:
        print(f"[BEC-1] No Cycle. Suspicion on Key {best_suspicion.key} (Gap: {min_suspicion_delta} ops)")
        return best_suspicion
        
    return None

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
    original_count = len(transactions)
    print(f"[BEC-1] Extracted {original_count} transactions", file=sys.stderr)
    
    # Limit transactions if specified or if too many
    if args.max_transactions and len(transactions) > args.max_transactions:
        print(f"[BEC-1] Limiting to {args.max_transactions} transactions (from {original_count})", file=sys.stderr)
        transactions = transactions[:args.max_transactions]
    elif len(transactions) > 5000:
        # Auto-limit very large histories
        limit = 5000
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
    
    best = pick_best_bec1(graph, unique_candidates)
    
    if best:
        payload = {
            "target_key": normalize_key(best.key),
            "strategy": "bec-1",
            "weight": 0.9
        }
    else:
        payload = {}

    output_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

if __name__ == "__main__":
    main()