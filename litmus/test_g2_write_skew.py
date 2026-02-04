from __future__ import annotations

"""
Litmus test for G2: Write Skew (cross-over pattern).

Scenario:
    1. T1 reads Key 1 (Value 0).
    2. T2 reads Key 2 (Value 0).
    3. T1 updates Key 2 to 100 and COMMITS.
    4. T2 updates Key 1 to 100 and COMMITS.

Check:
    - If both transactions commit successfully -> FAIL (write skew occurred).
    - If one transaction is blocked/aborted with SerializationFailure -> PASS.
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
    anomaly_name = "G2: Write Skew"

    try:
        tester.setup_db()

        tester.cur1.execute("BEGIN")
        tester.cur2.execute("BEGIN")

        # 1. T1 reads Key 1
        tester.cur1.execute("SELECT v FROM kv WHERE k = 1")
        _ = tester.cur1.fetchone()

        # 2. T2 reads Key 2
        tester.cur2.execute("SELECT v FROM kv WHERE k = 2")
        _ = tester.cur2.fetchone()

        t1_committed = False
        t2_committed = False

        # 3. T1 updates Key 2 to 100 and attempts to COMMIT
        try:
            tester.cur1.execute("UPDATE kv SET v = 100 WHERE k = 2")
            tester.conn1.commit()
            t1_committed = True
        except psycopg2.errors.SerializationFailure:
            tester.conn1.rollback()

        # 4. T2 updates Key 1 to 100 and attempts to COMMIT
        try:
            tester.cur2.execute("UPDATE kv SET v = 100 WHERE k = 1")
            tester.conn2.commit()
            t2_committed = True
        except psycopg2.errors.SerializationFailure:
            tester.conn2.rollback()

        if t1_committed and t2_committed:
            tester.report(
                anomaly_name,
                False,
                "Both T1 and T2 committed cross-over writes (write skew observed).",
            )
            return 1

        if not t1_committed and not t2_committed:
            tester.report(
                anomaly_name,
                True,
                "Both transactions were aborted with SerializationFailure; write skew prevented.",
            )
        elif not t1_committed:
            tester.report(
                anomaly_name,
                True,
                "T1 was aborted with SerializationFailure; only T2 committed.",
            )
        else:
            tester.report(
                anomaly_name,
                True,
                "T2 was aborted with SerializationFailure; only T1 committed.",
            )

        return 0

    finally:
        tester.teardown()


def main() -> int:
    parser = argparse.ArgumentParser(description="Litmus test for G2: Write Skew.")
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
