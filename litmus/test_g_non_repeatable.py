from __future__ import annotations

"""
Litmus test for Non-Repeatable Read.

Scenario:
    1. T1 reads Key 1 (Value 0).
    2. T2 updates Key 1 to 50 and COMMITS.
    3. T1 reads Key 1 again.

Check:
    - If T1 sees 50 on the second read -> FAIL (read was not repeatable).
    - If T1 continues to see 0 or is aborted with SerializationFailure -> PASS.
"""

import argparse
import sys
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from litmus_utils import LitmusTester


def run_test(dsn: str) -> int:
    tester = LitmusTester(dsn)
    anomaly_name = "G?: Non-Repeatable Read"

    try:
        tester.setup_db()

        # Start both transactions
        tester.cur1.execute("BEGIN")
        tester.cur2.execute("BEGIN")

        # 1. T1 reads Key 1 (Value 0)
        tester.cur1.execute("SELECT v FROM kv WHERE k = 1")
        row1 = tester.cur1.fetchone()
        first_v = row1[0] if row1 is not None else None

        # T1 keeps its transaction open, no commit yet

        # 2. T2 updates Key 1 to 50 and COMMITS
        tester.cur2.execute("UPDATE kv SET v = 50 WHERE k = 1")
        tester.conn2.commit()

        # 3. T1 reads Key 1 again
        second_v = None
        saw_serialization_failure = False
        try:
            tester.cur1.execute("SELECT v FROM kv WHERE k = 1")
            row2 = tester.cur1.fetchone()
            second_v = row2[0] if row2 is not None else None
            tester.conn1.commit()
        except psycopg2.errors.SerializationFailure:
            saw_serialization_failure = True
            tester.conn1.rollback()

        if saw_serialization_failure:
            tester.report(
                anomaly_name,
                True,
                "T1 was aborted with SerializationFailure during repeat read; non-repeatable read prevented.",
            )
            return 0

        if first_v == 0 and second_v == 50:
            tester.report(
                anomaly_name,
                False,
                "T1 observed 0 then 50 within a single transaction; non-repeatable read observed.",
            )
            return 1

        tester.report(
            anomaly_name,
            True,
            f"T1 saw values {first_v!r} then {second_v!r}; no non-repeatable read observed.",
        )
        return 0

    finally:
        tester.teardown()


def main() -> int:
    parser = argparse.ArgumentParser(description="Litmus test for Non-Repeatable Read.")
    parser.add_argument(
        "--dsn",
        default="postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable",
        help=(
            "PostgreSQL DSN. "
            "Defaults to postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"
        ),
    )
    args = parser.parse_args()
    return run_test(args.dsn)


if __name__ == "__main__":
    sys.exit(main())

