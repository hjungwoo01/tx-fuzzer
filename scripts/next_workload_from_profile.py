"""
Generate a next-step workload.

Creates "hot" copies of retargetable transactions and adjusts mix weights so
approximately target_weight of transactions hit the target key.
"""

from __future__ import annotations

import argparse
import copy
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

try:
    import yaml
except ImportError as exc:
    raise SystemExit("[ERROR] Missing dependency pyyaml. Install with `pip install pyyaml`.") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apply target_profile.json to a base workload YAML."
    )
    parser.add_argument(
        "--base",
        type=Path,
        default=Path("workloads/next.yaml"),
        help="Base workload YAML to retarget.",
    )
    parser.add_argument(
        "--profile",
        type=Path,
        default=Path("beck_target.json"),
        help="Path to target_profile.json.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("workloads/next.yaml"),
        help="Output workload YAML path.",
    )
    return parser.parse_args()


def load_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def save_yaml(data: Dict[str, Any], path: Path) -> None:
    with path.open("w", encoding="utf-8") as fh:
        yaml.safe_dump(data, fh, sort_keys=False, default_flow_style=False)


def load_profile(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    raw = path.read_text(encoding="utf-8").strip()
    if not raw or raw == "null":
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def parse_target_key(value: Any) -> Any:
    if isinstance(value, str) and value.isdigit():
        try:
            return int(value)
        except ValueError:
            return value
    return value


def parse_weight(value: Any, default: float = 0.9) -> float:
    try:
        weight = float(value)
    except (TypeError, ValueError):
        return default
    return max(0.0, min(1.0, weight))


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


def txn_has_key(txn: Dict[str, Any]) -> bool:
    for query in _iter_queries(txn):
        elle = query.get("elle")
        if isinstance(elle, dict) and "key" in elle:
            return True
    return False


def parse_arg_index(key_spec: Any) -> Optional[int]:
    if not isinstance(key_spec, str):
        return None
    if not key_spec.startswith("$"):
        return None
    try:
        return int(key_spec[1:])
    except ValueError:
        return None


def coerce_key(target: Any, exemplar: Any) -> Any:
    if isinstance(exemplar, str):
        return str(target)
    if isinstance(exemplar, int):
        try:
            return int(target)
        except (TypeError, ValueError):
            return target
    return target


def retarget_query(query: Dict[str, Any], target_key: Any) -> bool:
    elle = query.get("elle")
    if not isinstance(elle, dict) or "key" not in elle:
        return False

    key_spec = elle.get("key")
    
    # Read-Only Pivot strategy: Only retarget queries where key == "$1"
    # This ensures we only target the shared dependency (pivot) and preserve
    # randomness of $2 to avoid lock contention
    if key_spec != "$1":
        return False
    args = query.get("args", [])
    if not isinstance(args, list):
        args = []

    arg_index = parse_arg_index(key_spec)
    if arg_index is not None:
        if 1 <= arg_index <= len(args):
            exemplar = args[arg_index - 1]
            args[arg_index - 1] = coerce_key(target_key, exemplar)
            query["args"] = args
        return True

    old_key = key_spec
    elle["key"] = coerce_key(target_key, old_key)

    if args:
        new_args: List[Any] = []
        for arg in args:
            if arg == old_key:
                new_args.append(coerce_key(target_key, old_key))
            else:
                new_args.append(arg)
        query["args"] = new_args

    sql = query.get("sql")
    if isinstance(sql, str) and old_key is not None:
        old_str = str(old_key)
        if old_str.isdigit():
            new_str = str(coerce_key(target_key, old_key))
            pattern = rf"(\bk\s*=\s*){re.escape(old_str)}\b"
            if re.search(pattern, sql):
                query["sql"] = re.sub(pattern, lambda m: f"{m.group(1)}{new_str}", sql)
    return True


def retarget_transaction(txn: Dict[str, Any], target_key: Any) -> bool:
    changed = False
    for query in _iter_queries(txn):
        changed |= retarget_query(query, target_key)
    return changed


def unique_name(base: str, existing: set[str]) -> str:
    name = base
    counter = 1
    while name in existing:
        counter += 1
        name = f"{base}_{counter}"
    existing.add(name)
    return name


def build_mix(
    originals: List[Dict[str, Any]],
    retargetable_names: List[str],
    hot_names: Dict[str, str],
    existing_mix: Optional[List[Dict[str, Any]]],
    target_weight: float,
) -> List[Dict[str, Any]]:
    weights: Dict[str, float] = {}
    if isinstance(existing_mix, list):
        for item in existing_mix:
            if not isinstance(item, dict):
                continue
            name = item.get("txn")
            if not name:
                continue
            try:
                weights[name] = float(item.get("weight", 1.0))
            except (TypeError, ValueError):
                weights[name] = 1.0

    original_names = [txn.get("name") for txn in originals if txn.get("name")]
    for name in original_names:
        if name not in weights:
            weights[name] = 1.0

    sum_all = sum(weights.get(name, 0.0) for name in original_names)
    sum_retargetable = sum(weights.get(name, 0.0) for name in retargetable_names)

    if sum_all <= 0:
        sum_all = float(len(original_names) or 1)
        weights = {name: 1.0 for name in original_names}

    if sum_retargetable <= 0 or target_weight <= 0:
        return normalize_mix(
            [{"txn": name, "weight": weights[name]} for name in original_names]
        )

    hot_total = target_weight
    cold_total = 1.0 - hot_total

    mix: List[Dict[str, Any]] = []
    for name in retargetable_names:
        hot_name = hot_names.get(name)
        if not hot_name:
            continue
        hot_weight = hot_total * (weights[name] / sum_retargetable)
        mix.append({"txn": hot_name, "weight": hot_weight})

    for name in original_names:
        cold_weight = cold_total * (weights[name] / sum_all)
        mix.append({"txn": name, "weight": cold_weight})

    return normalize_mix(mix)


def normalize_mix(mix: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    total = sum(float(item.get("weight", 0.0)) for item in mix) or 1.0
    normalized: List[Dict[str, Any]] = []
    for item in mix:
        name = item.get("txn")
        if not name:
            continue
        weight = float(item.get("weight", 0.0)) / total
        normalized.append({"txn": name, "weight": round(weight, 4)})
    return normalized


def main() -> None:
    args = parse_args()
    cfg = load_yaml(args.base)
    profile = load_profile(args.profile)
    
    # Handle both BEC-1 format (target_key) and BEC-k format (targets array)
    strategy = profile.get("strategy", "").lower()
    target_key = None
    target_weight = 0.9
    
    if strategy == "bec-k" or "targets" in profile:
        # BEC-k format: use the first target
        targets = profile.get("targets", [])
        if targets and isinstance(targets, list) and len(targets) > 0:
            first_target = targets[0]
            if isinstance(first_target, dict):
                target_key = parse_target_key(first_target.get("key"))
                target_weight = parse_weight(first_target.get("weight", 0.6))
            else:
                target_key = parse_target_key(first_target)
    else:
        # BEC-1 format: single target_key
        target_key = parse_target_key(profile.get("target_key"))
        target_weight = parse_weight(profile.get("weight", 0.9))

    if target_key is None:
        save_yaml(cfg, args.out)
        return

    workload = cfg.setdefault("workload", {})
    transactions = workload.get("transactions", [])
    if not isinstance(transactions, list):
        save_yaml(cfg, args.out)
        return

    originals = list(transactions)
    existing_names = {txn.get("name") for txn in originals if txn.get("name")}
    retargetable_names: List[str] = []
    hot_names: Dict[str, str] = {}

    new_transactions = list(originals)
    for txn in originals:
        name = txn.get("name")
        if not name or not txn_has_key(txn):
            continue
        retargetable_names.append(name)
        hot_txn = copy.deepcopy(txn)
        hot_name = unique_name(f"hot_{name}", existing_names)
        hot_txn["name"] = hot_name
        retarget_transaction(hot_txn, target_key)
        hot_names[name] = hot_name
        new_transactions.append(hot_txn)

    workload["transactions"] = new_transactions
    workload["mix"] = build_mix(
        originals,
        retargetable_names,
        hot_names,
        workload.get("mix"),
        target_weight,
    )

    save_yaml(cfg, args.out)


if __name__ == "__main__":
    main()
