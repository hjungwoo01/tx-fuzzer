from __future__ import annotations

"""
Litmus test for P3: Phantom Read.

Scenario:
    1. Setup: Ensure there are two rows where v = 10 (k=1, k=2).
    2. T1: SELECT count(*) FROM kv WHERE v = 10. (e.g., returns 2)
    3. T2: INSERT INTO kv (k, v) VALUES (999, 10); COMMIT.
    4. T1: Re-run SELECT count(*) FROM kv WHERE v = 10.

Check:
    - If T1 sees the new count (> original) inside the same transaction
      -> FAIL (phantom read observed).
    - If T1 still sees the original count or is aborted -> PASS.
"""

import sys
from pathlib import Path
from typing import TYPE_CHECKING

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

if TYPE_CHECKING:
    from adapters import DatabaseAdapter

GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"


def run_test(adapter: "DatabaseAdapter") -> int:
    """Run the P3 Phantom Read litmus test using the given database adapter."""
    anomaly_name = "P3: Phantom Read"

    if type(adapter).__name__ == "RedisAdapter":
        print(f"[{anomaly_name}] Phantom read test not supported for Redis (no count/predicate).")
        return 0

    adapter.setup_schema(key_count=2)

    adapter.cur1.execute("BEGIN")
    adapter.cur1.execute("UPDATE kv SET v = 10 WHERE k IN (1, 2)")
    adapter.conn1.commit()

    adapter.cur1.execute("BEGIN")
    adapter.cur2.execute("BEGIN")

    adapter.cur1.execute("SELECT count(*) FROM kv WHERE v = 10")
    row = adapter.cur1.fetchone()
    initial_count = int(row[0]) if row is not None else 0

    try:
        adapter.cur2.execute("INSERT INTO kv (k, v) VALUES (999, 10)")
        adapter.conn2.commit()
    except Exception:
        adapter.conn2.rollback()
        print(
            f"[{anomaly_name}] {GREEN}PASS (Safe){RESET} - "
            "T2 was aborted while inserting; phantom prevented.",
        )
        adapter.conn1.rollback()
        return 0

    saw_abort = False
    final_count = initial_count
    try:
        adapter.cur1.execute("SELECT count(*) FROM kv WHERE v = 10")
        row2 = adapter.cur1.fetchone()
        final_count = int(row2[0]) if row2 is not None else 0
        adapter.conn1.commit()
    except Exception:
        saw_abort = True
        adapter.conn1.rollback()

    if saw_abort:
        print(
            f"[{anomaly_name}] {GREEN}PASS (Safe){RESET} - "
            "T1 was aborted during repeat count; phantom prevented.",
        )
        return 0

    if final_count > initial_count:
        print(
            f"[{anomaly_name}] {RED}FAIL (Vulnerable){RESET} - "
            f"T1 saw count grow from {initial_count} to {final_count}; phantom read observed.",
        )
        return 1

    print(
        f"[{anomaly_name}] {GREEN}PASS (Safe){RESET} - "
        f"T1 saw stable count {initial_count} -> {final_count}; no phantom read.",
    )
    return 0


def main() -> int:
    """Standalone entry point (PostgreSQL default DSN)."""
    import argparse
    from adapters import PostgresAdapter

    parser = argparse.ArgumentParser(description="Litmus test for P3: Phantom Read.")
    parser.add_argument(
        "--dsn",
        default="postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable",
        help="PostgreSQL DSN.",
    )
    parser.add_argument(
        "--isolation",
        default="READ COMMITTED",
        help="Transaction isolation level (READ COMMITTED, REPEATABLE READ, SERIALIZABLE).",
    )
    args = parser.parse_args()
    adapter = PostgresAdapter(args.dsn, isolation_level=args.isolation)
    adapter.connect()
    try:
        return run_test(adapter)
    finally:
        adapter.teardown()


if __name__ == "__main__":
    sys.exit(main())
