from __future__ import annotations

"""
Litmus test for G0: Dirty Read.

Scenario:
    1. T1: Updates Key 1 to 99 but does NOT commit (keeps transaction open).
    2. T2: Reads Key 1.
    3. T1: Rolls back (or commits; we use ROLLBACK for a stricter test).

Check:
    - If T2 sees 99 -> FAIL (dirty read observed).
    - If T2 sees the old value (e.g., 0) or is aborted/blocked
      with SerializationFailure -> PASS.
"""

import argparse
import sys
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from litmus_utils import LitmusTester

def run_test(dsn: str, isolation: str) -> int:
    tester = LitmusTester(dsn, isolation_level=isolation)
    anomaly_name = "G0: Dirty Read"

    try:
        tester.setup_db()

        # Start both transactions
        tester.cur1.execute("BEGIN")
        tester.cur2.execute("BEGIN")

        # 1. T1: Update Key 1 to 99 but DO NOT commit
        tester.cur1.execute("UPDATE kv SET v = 99 WHERE k = 1")

        # 2. T2: Attempt to read Key 1
        saw_serialization_failure = False
        read_value = None
        try:
            tester.cur2.execute("SELECT v FROM kv WHERE k = 1")
            row = tester.cur2.fetchone()
            read_value = row[0] if row is not None else None
            tester.conn2.commit()
        except psycopg2.errors.SerializationFailure:
            saw_serialization_failure = True
            tester.conn2.rollback()

        # 3. T1: Roll back its uncommitted change
        tester.conn1.rollback()

        if saw_serialization_failure:
            tester.report(
                anomaly_name,
                True,
                "T2 was aborted with SerializationFailure while reading; dirty read prevented.",
            )
            return 0

        if read_value == 99:
            tester.report(
                anomaly_name,
                False,
                "T2 read the uncommitted value 99 from T1; dirty read observed.",
            )
            return 1

        tester.report(
            anomaly_name,
            True,
            f"T2 saw value {read_value!r} (not the uncommitted 99); no dirty read observed.",
        )
        return 0

    finally:
        tester.teardown()


def main() -> int:
    parser = argparse.ArgumentParser(description="Litmus test for G0: Dirty Read.")
    parser.add_argument(
        "--dsn",
        default="postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable",
        help=(
            "PostgreSQL DSN. "
            "Defaults to postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
        ),
    )
    parser.add_argument(
        "--isolation",
        default="READ COMMITTED",
        help='Transaction isolation level (READ COMMITTED, REPEATABLE READ, SERIALIZABLE).',
    )
    args = parser.parse_args()
    return run_test(args.dsn, args.isolation)


if __name__ == "__main__":
    sys.exit(main())

