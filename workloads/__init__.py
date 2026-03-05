"""
Workload registry — central catalog of all evaluation workloads.

Each workload module must expose:
    setup_sql()                                       -> list[str]
    seed_trace(clients, isolation, variant, seed)      -> list[dict]
    oracle(execution_history, final_state, **kwargs)   -> OracleResult
    WORKLOAD_META: dict  with name, description, invariants, isolation_levels, etc.
"""

from __future__ import annotations

import importlib
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class OracleResult:
    """Outcome of an oracle / invariant check."""
    ok: bool                      # True → no violation
    bug_type: Optional[str] = None  # e.g. "write_skew", "lost_update", "ssi_conflict_out"
    details: str = ""
    metrics: Dict[str, Any] = field(default_factory=dict)


# ── module catalogue ────────────────────────────────────────────────────────
_WORKLOAD_MODULES = {
    "update_conflict_out":  "workloads.update_conflict_out",
    "doctors_write_skew":   "workloads.doctors_write_skew",
    "bank_lost_update":     "workloads.bank_lost_update",
    "inventory_oversell":   "workloads.inventory_oversell",
    "append_upsert":        "workloads.append_upsert",
    "job_queue_double_claim": "workloads.job_queue_double_claim",
    "bank_overdraft":       "workloads.bank_overdraft",
}


def list_workloads() -> List[str]:
    return list(_WORKLOAD_MODULES.keys())


def load_workload(name: str):
    """Import and return the workload module."""
    if name not in _WORKLOAD_MODULES:
        raise ValueError(f"Unknown workload: {name}. Available: {list_workloads()}")
    return importlib.import_module(_WORKLOAD_MODULES[name])


def get_meta(name: str) -> Dict[str, Any]:
    mod = load_workload(name)
    return getattr(mod, "WORKLOAD_META", {"name": name})
