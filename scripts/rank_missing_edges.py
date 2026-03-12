"""
Rank candidate missing edges that would most effectively create cycles (anomalies)
using multiple approximation heuristics:

- Bounded simple-path counts (k-edge cycle boost)
- Length-2 path (triad) frequency
- Sampled cycle hits on short simple cycles
- Node betweenness centrality as a flow/spectral proxy
- Topological gap across the condensation DAG (feedback-arc style)

Usage:
    python scripts/rank_missing_edges.py history.edn --top-k 10
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import networkx as nx
import yaml

try:
    from scripts.analyze_history import (
        EdgeDetail,
        build_dependency_graph,
        extract_transactions,
        load_history,
    )
except ImportError:
    from analyze_history import (  # type: ignore
        EdgeDetail,
        build_dependency_graph,
        extract_transactions,
        load_history,
    )


@dataclass
class CandidateEdge:
    add_from: str  # edge to add: add_from -> add_to
    add_to: str
    path_count: int
    triad_count: int
    cycle_hits: int
    centrality: float
    topo_gap: int
    score: float
    key_hints: List[Any]
    shortest_path_len: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rank missing edges that would most effectively create dependency cycles."
    )
    parser.add_argument("history", type=Path, help="Path to Elle history.edn")
    parser.add_argument(
        "--max-path-len",
        type=int,
        default=5,
        help="Maximum path length to consider when counting simple paths (cycle boost depth).",
    )
    parser.add_argument(
        "--max-candidates",
        type=int,
        default=2000,
        help="Maximum number of candidate edges to score (truncated by path search).",
    )
    parser.add_argument(
        "--cycle-samples",
        type=int,
        default=200,
        help="Maximum number of short simple cycles to sample for the cycle-hit heuristic.",
    )
    parser.add_argument(
        "--cycle-max-len",
        type=int,
        default=7,
        help="Maximum cycle length to include in the sampled set.",
    )
    parser.add_argument(
        "--centrality-samples",
        type=int,
        default=40,
        help="Number of nodes to sample for approximate betweenness centrality (0 disables sampling and uses full betweenness).",
    )
    parser.add_argument(
        "--top-k",
        type=int,
        default=10,
        help="How many top-ranked candidate edges to print.",
    )
    parser.add_argument(
        "--json-output",
        type=Path,
        help="Optional path to write the ranked candidates as JSON.",
    )
    parser.add_argument(
        "--base-workload",
        type=Path,
        help="Optional base workload YAML to mutate toward the top candidate edge.",
    )
    parser.add_argument(
        "--output-workload",
        type=Path,
        help="Where to write the mutated workload (requires --base-workload).",
    )
    parser.add_argument(
        "--output-replay",
        type=Path,
        help="Optional replay YAML focusing on the top candidate when templates are known.",
    )
    parser.add_argument(
        "--replay-sleep-ms",
        type=int,
        default=5,
        help="Sleep inserted between replay steps/commits for finer ordering (0 to disable).",
    )
    parser.add_argument(
        "--preserve-graph",
        action="store_true",
        help="Bias key choices only in edge templates to keep other edges stable.",
    )
    parser.add_argument(
        "--key-bias",
        type=int,
        default=3,
        help="Bias multiplier for choice lists when --preserve-graph is set.",
    )
    parser.add_argument(
        "--replay-background-clients",
        type=int,
        default=0,
        help="If >0, include background workload clients in the replay config.",
    )
    return parser.parse_args()


def bounded_simple_paths_count(
    graph: nx.DiGraph,
    src: str,
    dst: str,
    max_len: int,
) -> int:
    """Count simple paths from src to dst with length <= max_len (intractable at full depth; bounded here)."""
    if src == dst or max_len <= 0:
        return 0
    count = 0
    stack: List[Tuple[str, int, Set[str]]] = [(src, 0, {src})]
    while stack:
        node, depth, visited = stack.pop()
        if depth >= max_len:
            continue
        for succ in graph.successors(node):
            if succ in visited:
                continue
            if succ == dst:
                count += 1
                continue
            if depth + 1 < max_len:
                stack.append((succ, depth + 1, visited | {succ}))
    return count


def triad_count(graph: nx.DiGraph, src: str, dst: str) -> int:
    """Count length-2 paths src->mid->dst (classic near-cycle heuristic)."""
    count = 0
    for mid in graph.successors(src):
        if graph.has_edge(mid, dst):
            count += 1
    return count


def sample_cycles(
    graph: nx.DiGraph,
    max_cycles: int,
    max_len: int,
) -> List[List[str]]:
    """Sample short simple cycles to estimate how often nodes co-occur on cycles."""
    cycles: List[List[str]] = []
    for cycle in nx.simple_cycles(graph):
        if len(cycle) > max_len:
            continue
        cycles.append(cycle)
        if len(cycles) >= max_cycles:
            break
    return cycles


def cycle_hits_for_candidate(
    cycles: Sequence[Sequence[str]],
    src: str,
    dst: str,
) -> int:
    """
    Count sampled cycles where dst is reachable from src following the cycle's direction.
    Adding dst->src would create an additional cycle alongside that path.
    """
    hits = 0
    for cycle in cycles:
        if src not in cycle or dst not in cycle:
            continue
        idx_src = cycle.index(src)
        # Walk forward through the directed cycle until we return or hit dst
        steps = 0
        n = len(cycle)
        cur = idx_src
        while steps < n:
            nxt = (cur + 1) % n
            if cycle[nxt] == dst:
                hits += 1
                break
            cur = nxt
            steps += 1
    return hits


def key_hints_from_triad(
    graph: nx.DiGraph,
    src: str,
    dst: str,
) -> List[Any]:
    """Collect keys from length-2 paths to give context for candidate edge targeting."""
    hints: List[Any] = []
    for mid in graph.successors(src):
        if not graph.has_edge(mid, dst):
            continue
        rels_src_mid = graph[src][mid].get("relationships", [])
        rels_mid_dst = graph[mid][dst].get("relationships", [])
        for rel in (*rels_src_mid, *rels_mid_dst):
            if isinstance(rel, EdgeDetail) and rel.key is not None:
                hints.append(rel.key)
    # preserve order, dedupe
    seen = set()
    deduped = []
    for k in hints:
        if k in seen:
            continue
        seen.add(k)
        deduped.append(k)
    return deduped


def topological_gap(graph: nx.DiGraph, src: str, dst: str) -> int:
    """
    Distance between SCCs in the condensation DAG: how far back would the missing edge jump?
    Larger gaps mean a stronger feedback-arc effect.
    """
    if src == dst:
        return 0
    cond = nx.condensation(graph)
    comp_map = cond.graph["mapping"]
    topo_order = {node: idx for idx, node in enumerate(nx.topological_sort(cond))}
    c_src = comp_map[src]
    c_dst = comp_map[dst]
    if c_src == c_dst:
        return 0
    return max(0, topo_order[c_src] - topo_order[c_dst])


def betweenness_centrality_map(graph: nx.DiGraph, samples: int) -> Dict[str, float]:
    """Approximate betweenness to use as a flow/spectral proxy."""
    n = graph.number_of_nodes()
    if n == 0:
        return {}
    if samples <= 0 or samples >= n:
        return nx.betweenness_centrality(graph, normalized=True)
    # sample a subset of nodes for approximation
    import random

    random.seed(42)
    subset = random.sample(list(graph.nodes), k=min(samples, n))
    return nx.betweenness_centrality(graph, k=subset, normalized=True, seed=42)


def generate_candidates(
    graph: nx.DiGraph,
    max_path_len: int,
    max_candidates: int,
) -> List[Tuple[str, str, int]]:
    """
    Enumerate candidate edges as (src, dst, shortest_path_len) where src->...->dst exists,
    dst->src does not, and path length <= max_path_len.
    """
    candidates: List[Tuple[str, str, int]] = []
    for src in graph.nodes:
        lengths = nx.single_source_shortest_path_length(graph, src, cutoff=max_path_len)
        for dst, dist in lengths.items():
            if src == dst:
                continue
            if graph.has_edge(dst, src):
                continue
            candidates.append((src, dst, dist))
            if len(candidates) >= max_candidates:
                return candidates
    return candidates


def score_candidates(
    graph: nx.DiGraph,
    candidates: Sequence[Tuple[str, str, int]],
    cycles: Sequence[Sequence[str]],
    centrality: Dict[str, float],
    max_path_len: int,
) -> List[CandidateEdge]:
    scored: List[CandidateEdge] = []
    for src, dst, shortest in candidates:
        pcount = bounded_simple_paths_count(graph, src, dst, max_path_len)
        tcount = triad_count(graph, src, dst)
        chits = cycle_hits_for_candidate(cycles, src, dst)
        cent = 0.5 * (centrality.get(src, 0.0) + centrality.get(dst, 0.0))
        gap = topological_gap(graph, src, dst)
        hints = key_hints_from_triad(graph, src, dst)
        score = (
            2.0 * pcount
            + 1.5 * tcount
            + 2.5 * chits
            + 1.0 * cent
            + 0.5 * gap
        )
        scored.append(
            CandidateEdge(
                add_from=dst,
                add_to=src,
                path_count=pcount,
                triad_count=tcount,
                cycle_hits=chits,
                centrality=cent,
                topo_gap=gap,
                score=score,
                key_hints=hints,
                shortest_path_len=shortest,
            )
        )
    scored.sort(key=lambda c: c.score, reverse=True)
    return scored


# Workload mutation helpers


def _ensure_list(obj: Any) -> List[Any]:
    if isinstance(obj, Iterable) and not isinstance(obj, (str, bytes)):
        return list(obj)
    return [obj]


def _coerce_keys(sample: List[Any], hot_keys: List[Any]) -> List[Any]:
    if not hot_keys:
        return []
    if sample:
        exemplar = sample[0]
        if isinstance(exemplar, int):
            coerced: List[Any] = []
            for key in hot_keys:
                try:
                    coerced.append(int(key))
                except (TypeError, ValueError):
                    coerced.append(key)
            return coerced
        if isinstance(exemplar, str):
            return [str(key) for key in hot_keys]
    return list(hot_keys)


def _bias_choice_list(original: List[Any], hot_keys: List[Any], bias: int) -> List[Any]:
    if not hot_keys:
        return original
    bias = max(1, bias)
    coerced = _coerce_keys(original, hot_keys)
    if not coerced:
        return original
    max_len = 200
    biased: List[Any] = []
    for _ in range(bias):
        biased.extend(coerced)
    biased.extend(original)
    if len(biased) <= max_len:
        return biased
    head_room = max(0, max_len - len(original))
    if head_room == 0:
        return original
    trimmed: List[Any] = []
    for i in range(head_room):
        trimmed.append(coerced[i % len(coerced)])
    trimmed.extend(original)
    return trimmed


def _mutate_args(args: List[Any], hot_keys: List[Any], mode: str = "strict", bias: int = 3) -> None:
    """Restrict or bias choice lists/literal args to focus keys."""
    if not hot_keys:
        return
    for idx, arg in enumerate(args):
        if isinstance(arg, dict) and "choice" in arg:
            choice = arg["choice"]
            if isinstance(choice, dict) and "from" in choice:
                original = _ensure_list(choice.get("from", []))
                if mode == "bias":
                    choice["from"] = _bias_choice_list(original, hot_keys, bias)
                else:
                    choice["from"] = _coerce_keys(original, hot_keys)
        elif mode == "strict" and isinstance(arg, (int, str)) and len(hot_keys) == 1:
            coerced = _coerce_keys([arg], hot_keys)
            if coerced:
                args[idx] = coerced[0]


def _iter_queries(txn: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
    queries = txn.get("queries", [])
    if isinstance(queries, list):
        for query in queries:
            if isinstance(query, dict):
                yield query
    for step in txn.get("steps", []):
        if not isinstance(step, dict):
            continue
        nested = step.get("queries")
        if isinstance(nested, list):
            for query in nested:
                if isinstance(query, dict):
                    yield query
            continue
        if step.get("sql"):
            yield step


def _retarget_transactions(
    transactions: List[Dict[str, Any]],
    hot_keys: List[Any],
    target_templates: Optional[Set[str]] = None,
    mode: str = "strict",
    bias: int = 3,
) -> None:
    for txn in transactions:
        name = txn.get("name")
        if target_templates and name not in target_templates:
            continue
        for query in _iter_queries(txn):
            args = query.get("args", [])
            if isinstance(args, list):
                _mutate_args(args, hot_keys, mode=mode, bias=bias)


def _boost_mix(mix: List[Dict[str, Any]], favored: Set[str], boost: float = 2.0) -> None:
    if not mix:
        return
    weights = []
    for item in mix:
        w = float(item.get("weight", 1.0))
        if item.get("txn") in favored:
            w *= boost
        weights.append(max(w, 0.01))
    total = sum(weights) or 1.0
    for item, w in zip(mix, weights, strict=False):
        item["weight"] = round(w / total, 4)


def build_replay_config_for_edge(
    base_cfg: Dict[str, Any],
    best: CandidateEdge,
    template_map: Dict[str, Optional[str]],
    sleep_ms: int,
    background_clients: int = 0,
    key_mode: str = "strict",
    key_bias: int = 3,
) -> Optional[Dict[str, Any]]:
    start_tmpl = template_map.get(best.add_from)
    end_tmpl = template_map.get(best.add_to)
    if not start_tmpl or not end_tmpl:
        return None

    # Look up templates to count and target steps
    templates = {
        txn.get("name"): txn for txn in base_cfg.get("workload", {}).get("transactions", [])
    }

    def _steps_for(name: str) -> List[Dict[str, Any]]:
        tmpl = templates.get(name, {})
        if isinstance(tmpl, dict):
            if isinstance(tmpl.get("steps"), list):
                return tmpl["steps"]
            if isinstance(tmpl.get("queries"), list):
                # Normalize queries to steps
                return [{"sql": q.get("sql"), "args": q.get("args", []), "elle": q.get("elle"), "sleep": q.get("sleep")} for q in tmpl["queries"]]
        return []

    def _len_steps(name: str) -> int:
        return len(_steps_for(name))

    def _first_idx(name: str, kind: str, key_hint: Optional[Any]) -> Optional[int]:
        steps = _steps_for(name)
        for i, step in enumerate(steps, 1):
            elle = step.get("elle")
            if not isinstance(elle, dict):
                continue
            f = str(elle.get("f", "")).lower()
            if kind == "write" and f not in {":write", ":w", ":append", ":inc"}:
                continue
            if kind == "read" and f not in {":read", ":r"}:
                continue
            if key_hint is not None:
                key = elle.get("key")
                if key is not None and str(key) not in {str(key_hint), "$1", "$2", "$3"}:
                    continue
            return i
        return None

    focus_key = best.key_hints[0] if best.key_hints else None
    start_steps = _len_steps(start_tmpl)
    end_steps = _len_steps(end_tmpl)

    start_target = _first_idx(start_tmpl, "write", focus_key) or start_steps
    end_target = _first_idx(end_tmpl, "read", focus_key) or end_steps

    sleep_ms = max(0, sleep_ms)

    def maybe_sleep(schedule: List[Dict[str, Any]]) -> None:
        if sleep_ms > 0:
            schedule.append({"action": "sleep", "sleep": f"{sleep_ms}ms"})

    schedule: List[Dict[str, Any]] = []
    executed = {"from": 0, "to": 0}

    def add_step(alias: str, target: int, total: int) -> None:
        delta = target - executed[alias]
        if delta <= 0:
            return
        schedule.append({"alias": alias, "action": "step", "steps": delta})
        executed[alias] += delta
        maybe_sleep(schedule)

    # Run writer (from) to its key write, then reader (to) to its key read
    add_step("from", start_target, start_steps)
    add_step("to", end_target, end_steps)
    # Drain remaining steps
    add_step("from", start_steps, start_steps)
    add_step("to", end_steps, end_steps)

    # Commit in order: writer then reader
    schedule.append({"alias": "from", "action": "commit"})
    maybe_sleep(schedule)
    schedule.append({"alias": "to", "action": "commit"})

    replay = {
        "replay": {
            "enabled": True,
            "transactions": [
                {"alias": "from", "txn": start_tmpl},
                {"alias": "to", "txn": end_tmpl},
            ],
            "schedule": schedule,
        }
    }
    if background_clients > 0:
        replay["replay"]["background_clients"] = background_clients
        replay["replay"]["process_offset"] = background_clients

    # Retarget keys inside the replay workload as well.
    focus_keys = best.key_hints[:1] if best.key_hints else []
    replay_workload = yaml.safe_load(yaml.safe_dump(base_cfg))
    if focus_keys:
        _retarget_transactions(
            replay_workload.get("workload", {}).get("transactions", []),
            focus_keys,
            target_templates={start_tmpl, end_tmpl},
            mode=key_mode,
            bias=key_bias,
        )
    replay.update(replay_workload)
    return replay


def build_targeted_workload(
    base_path: Path,
    best: CandidateEdge,
    transactions: Sequence[Any],
    sleep_ms: int = 0,
    preserve_graph: bool = False,
    key_bias: int = 3,
    replay_background_clients: int = 0,
) -> Tuple[Dict[str, Any], Optional[Dict[str, Any]]]:
    cfg = yaml.safe_load(base_path.read_text(encoding="utf-8"))
    workload = cfg.get("workload", {})
    txns = workload.get("transactions", [])
    mix = workload.get("mix", [])
    if preserve_graph and replay_background_clients <= 0:
        sched = cfg.get("scheduling", {})
        replay_background_clients = int(sched.get("clients", 1))

    # Build txn_id -> template map from history metadata
    template_map: Dict[str, Optional[str]] = {}
    for txn in transactions:
        txn_id = getattr(txn, "txn_id", None)
        template_map[txn_id] = getattr(txn, "template", None)

    favored_templates = {
        t for t in (template_map.get(best.add_from), template_map.get(best.add_to)) if t
    }
    focus_keys = best.key_hints[:2] if best.key_hints else []
    mode = "bias" if preserve_graph else "strict"
    target_templates = favored_templates if preserve_graph and favored_templates else None
    if focus_keys:
        _retarget_transactions(
            txns,
            focus_keys,
            target_templates=target_templates,
            mode=mode,
            bias=key_bias,
        )
    if favored_templates:
        boost = 1.4 if preserve_graph else 3.0
        _boost_mix(mix, favored_templates, boost=boost)
    cfg["workload"]["transactions"] = txns
    cfg["workload"]["mix"] = mix

    replay_cfg = build_replay_config_for_edge(
        cfg,
        best,
        template_map,
        sleep_ms=sleep_ms,
        background_clients=replay_background_clients,
        key_mode=mode,
        key_bias=key_bias,
    )
    return cfg, replay_cfg


def print_results(results: Sequence[CandidateEdge], top_k: int) -> None:
    print(f"[INFO] Ranked missing edges (top {top_k}):")
    for rank, cand in enumerate(results[:top_k], 1):
        key_part = f" keys={cand.key_hints}" if cand.key_hints else ""
        print(
            f"{rank:02d}. add {cand.add_from} -> {cand.add_to} "
            f"[score={cand.score:.2f}; paths={cand.path_count}; triads={cand.triad_count}; "
            f"cycles={cand.cycle_hits}; centrality={cand.centrality:.4f}; gap={cand.topo_gap}; "
            f"shortest={cand.shortest_path_len}{key_part}]"
        )


def main() -> int:
    args = parse_args()
    if not args.history.exists():
        print(f"[ERROR] History file not found: {args.history}")
        return 1

    events = load_history(args.history)
    transactions = extract_transactions(events)
    graph, _, _ = build_dependency_graph(transactions)

    cycles = sample_cycles(
        graph,
        max_cycles=max(args.cycle_samples, 0),
        max_len=max(args.cycle_max_len, 2),
    )
    centrality = betweenness_centrality_map(graph, args.centrality_samples)
    candidates = generate_candidates(
        graph,
        max_path_len=max(args.max_path_len, 1),
        max_candidates=max(args.max_candidates, 1),
    )
    results = score_candidates(
        graph,
        candidates,
        cycles,
        centrality,
        max_path_len=max(args.max_path_len, 1),
    )
    print_results(results, top_k=max(args.top_k, 1))

    if args.json_output:
        payload = [asdict(c) for c in results]
        args.json_output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"[INFO] Wrote ranked candidates to {args.json_output}")

    if args.base_workload and args.output_workload:
        best = results[0] if results else None
        if best is None:
            print("[WARN] No candidates to drive workload mutation.")
        else:
            mutated, replay = build_targeted_workload(
                args.base_workload,
                best,
                transactions,
                sleep_ms=args.replay_sleep_ms,
                preserve_graph=args.preserve_graph,
                key_bias=max(args.key_bias, 1),
                replay_background_clients=max(args.replay_background_clients, 0),
            )
            args.output_workload.parent.mkdir(parents=True, exist_ok=True)
            args.output_workload.write_text(
                yaml.safe_dump(mutated, sort_keys=False),
                encoding="utf-8",
            )
            print(f"[INFO] Wrote targeted workload to {args.output_workload} for edge {best.add_from}->{best.add_to}")
            if replay and args.output_replay:
                args.output_replay.parent.mkdir(parents=True, exist_ok=True)
                args.output_replay.write_text(
                    yaml.safe_dump(replay, sort_keys=False),
                    encoding="utf-8",
                )
                print(f"[INFO] Wrote deterministic replay to {args.output_replay}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
