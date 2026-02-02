"""
Workload generator utilities for BEC-1 benchmarks.
"""
from __future__ import annotations

from typing import Any, Dict, Optional

DEFAULT_DSN = "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"


def generate_write_skew_workload(
    keys: int,
    limit: Optional[int] = None,
    dsn: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Generate a workload config that targets Write Skew (G2) anomalies.

    Pattern: each transaction reads one key (K1) and writes a *different* key (K2):
    - SELECT v FROM kv WHERE k = $1  (elle: :read, key: $1)
    - UPDATE kv SET v = v + 1 WHERE k = $2  (elle: :write, key: $2)

    The mix is 100% "cross-over" transactions to maximize collision probability
    (concurrent T1: read A write B, T2: read B write A).

    Args:
        keys: Keyspace size; both $1 and $2 are chosen from [1, keys].
        limit: Optional transaction limit for exploration (e.g. BEC-1 scout).
        dsn: Optional Postgres DSN; defaults to DEFAULT_DSN.

    Returns:
        Full workload YAML as a dict (database, init_schema, consistency_model,
        workload, scheduling).
    """
    if keys < 2:
        keys = 2
    cfg: Dict[str, Any] = {
        "database": {
            "kind": "postgres",
            "dsn": dsn or DEFAULT_DSN,
        },
        "init_schema": True,
        "consistency_model": "serializable",
        "workload": {
            "datatype": "register",
            "transactions": [
                {
                    "name": "cross_over",
                    "isolation": "READ COMMITTED",
                    "timeout": "2s",
                    "steps": [
                        {
                            "sql": "SELECT v FROM kv WHERE k = $1",
                            "args": [{"rand_int": {"min": 1, "max": keys}}],
                            "elle": {"f": ":read", "key": "$1", "value_from_result_col": 0},
                        },
                        {
                            "sql": "UPDATE kv SET v = v + 1 WHERE k = $2",
                            "args": [{"rand_int": {"min": 1, "max": keys}}],
                            "elle": {"f": ":write", "key": "$2"},
                        },
                    ],
                },
            ],
            "mix": [{"txn": "cross_over", "weight": 1.0}],
        },
        "scheduling": {
            "duration": "30s",
            "clients": 4,
            "barrier_every": 0,
            "random_kill_pct": 0,
            "jitter_ms_min": 0,
            "jitter_ms_max": 0,
        },
    }
    if limit is not None:
        cfg["scheduling"]["limit"] = limit
    return cfg
