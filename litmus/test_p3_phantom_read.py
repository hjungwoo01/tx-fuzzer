from __future__ import annotations

"""
Litmus test for P3: Phantom Read.

Scenario:
    1. Setup: Ensure there are two rows where v = 10.
    2. T1: SELECT count(*) FROM kv WHERE v = 10. (e.g., returns 2)
    3. T2: INSERT INTO kv (k, v) VALUES (999, 10); COMMIT.
    4. T1: Re-run SELECT count(*) FROM kv WHERE v = 10.

Check:
    - If T1 sees the new count (> original) inside the same transaction
      -> FAIL (phantom read observed).
    - If T1 still sees the original count or is aborted
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
    anomaly_name = "P3: Phantom Read"

    try:
        tester.setup_db()

        # Setup: ensure two rows where v = 10
        tester.cur1.execute("BEGIN")
        tester.cur1.execute("UPDATE kv SET v = 10 WHERE k IN (1, 2)")
        tester.conn1.commit()

        tester.cur1.execute("BEGIN")
        tester.cur2.execute("BEGIN")

        # 1. T1: initial count
        tester.cur1.execute("SELECT count(*) FROM kv WHERE v = 10")
        row = tester.cur1.fetchone()
        initial_count = int(row[0]) if row is not None else 0

        # 2. T2: insert a new row with v = 10 and commit
        try:
            tester.cur2.execute("INSERT INTO kv (k, v) VALUES (999, 10)")
            tester.conn2.commit()
        except psycopg2.errors.SerializationFailure:
            tester.conn2.rollback()
            tester.report(
                anomaly_name,
                True,
                "T2 was aborted with SerializationFailure while inserting; phantom prevented.",
            )
            tester.conn1.rollback()
            return 0

        # 3. T1: re-run the count in the same transaction
        saw_serialization_failure = False
        final_count = initial_count
        try:
            tester.cur1.execute("SELECT count(*) FROM kv WHERE v = 10")
            row2 = tester.cur1.fetchone()
            final_count = int(row2[0]) if row2 is not None else 0
            tester.conn1.commit()
        except psycopg2.errors.SerializationFailure:
            saw_serialization_failure = True
            tester.conn1.rollback()

        if saw_serialization_failure:
            tester.report(
                anomaly_name,
                True,
                "T1 was aborted with SerializationFailure during repeat count; phantom prevented.",
            )
            return 0

        if final_count > initial_count:
            tester.report(
                anomaly_name,
                False,
                f"T1 saw count grow from {initial_count} to {final_count} within one transaction; phantom read observed.",
            )
            return 1

        tester.report(
            anomaly_name,
            True,
            f"T1 saw stable count {initial_count} -> {final_count}; no phantom read observed.",
        )
        return 0

    finally:
        tester.teardown()


def main() -> int:
    parser = argparse.ArgumentParser(description="Litmus test for P3: Phantom Read.")
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

