from __future__ import annotations

"""
Litmus test for G2: Write Skew (cross-over pattern).

Scenario:
    1. T1 reads Key 1 (Value 0).
    2. T2 reads Key 2 (Value 0).
    3. T1 updates Key 2 to 100 and COMMITS.
    4. T2 updates Key 1 to 100 and COMMITS.

Check:
    - If both transactions commit and both keys end up 100 -> FAIL (write skew occurred).
    - If one transaction is blocked/aborted (serialization/lock failure) -> PASS.
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


def run_one(adapter: "DatabaseAdapter") -> bool:
    """
    Run one G2 Write Skew scenario. No output.
    Returns True if write skew was observed (both txns committed, both keys 100), False otherwise.
    """
    adapter.setup_schema(key_count=2)

    adapter.cur1.execute("BEGIN")
    adapter.cur2.execute("BEGIN")

    adapter.cur1.execute("SELECT v FROM kv WHERE k = 1")
    adapter.cur1.fetchone()

    adapter.cur2.execute("SELECT v FROM kv WHERE k = 2")
    adapter.cur2.fetchone()

    t1_committed = False
    t2_committed = False

    try:
        adapter.cur1.execute("UPDATE kv SET v = 100 WHERE k = 2")
        adapter.conn1.commit()
        t1_committed = True
    except Exception:
        adapter.conn1.rollback()

    try:
        adapter.cur2.execute("UPDATE kv SET v = 100 WHERE k = 1")
        adapter.conn2.commit()
        t2_committed = True
    except Exception:
        adapter.conn2.rollback()

    with adapter.conn1.cursor() as c:
        c.execute("SELECT v FROM kv WHERE k = 1")
        r1 = c.fetchone()
        c.execute("SELECT v FROM kv WHERE k = 2")
        r2 = c.fetchone()
    v1 = r1[0] if r1 is not None else None
    v2 = r2[0] if r2 is not None else None

    return bool(t1_committed and t2_committed and v1 == 100 and v2 == 100)


def run_test(adapter: "DatabaseAdapter") -> int:
    """Run the G2 Write Skew litmus test using the given database adapter (single run, with report)."""
    anomaly_name = "G2: Write Skew"
    anomaly = run_one(adapter)

    if anomaly:
        print(f"[{anomaly_name}] {RED}FAIL (Vulnerable){RESET} - Write Skew Detected (both keys are 100).")
        return 1

    print(
        f"[{anomaly_name}] {GREEN}PASS (Safe){RESET} - "
        "One or both transactions were aborted; write skew prevented."
    )
    return 0


def main() -> int:
    """Standalone entry point (PostgreSQL default DSN)."""
    import argparse
    from adapters import PostgresAdapter

    parser = argparse.ArgumentParser(description="Litmus test for G2: Write Skew.")
    parser.add_argument(
        "--dsn",
        default="postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable",
        help="PostgreSQL DSN.",
    )
    args = parser.parse_args()
    adapter = PostgresAdapter(args.dsn)
    adapter.connect()
    try:
        return run_test(adapter)
    finally:
        adapter.teardown()


if __name__ == "__main__":
    sys.exit(main())
