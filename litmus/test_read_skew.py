from __future__ import annotations

"""
Litmus test for Read Skew (inconsistent snapshot).

Scenario:
    1. Setup: Key 1 = 50, Key 2 = 50 (total = 100).
    2. T1: Reads Key 1 (gets 50).
    3. T2: Transfers 10 from Key 1 to Key 2:
           - UPDATE k=1 to 40
           - UPDATE k=2 to 60
           COMMIT.
    4. T1: Reads Key 2.

Check:
    - T1 computes total = read1 + read2.
    - If total == 110 (50 + 60) -> FAIL (read skew observed).
    - If total == 100 (50 + 50) or T1/T2 is aborted
      with SerializationFailure -> PASS.
"""

import argparse
import sys
from pathlib import Path

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from litmus_utils import LitmusTester  # type: ignore  # noqa: E402


def run_test(dsn: str, isolation: str) -> int:
    tester = LitmusTester(dsn, isolation_level=isolation)
    anomaly_name = "Read Skew"

    try:
        tester.setup_db()

        # Setup: k1 = 50, k2 = 50.
        tester.cur1.execute("BEGIN")
        tester.cur1.execute("UPDATE kv SET v = 50 WHERE k IN (1, 2)")
        tester.conn1.commit()

        tester.cur1.execute("BEGIN")
        tester.cur2.execute("BEGIN")

        # 1. T1: Reads Key 1 (expected 50)
        tester.cur1.execute("SELECT v FROM kv WHERE k = 1")
        row1 = tester.cur1.fetchone()
        v1 = row1[0] if row1 is not None else None

        # 2. T2: Perform transfer 10 from k1 to k2 and commit
        t2_serialization_failure = False
        try:
            tester.cur2.execute("UPDATE kv SET v = 40 WHERE k = 1")
            tester.cur2.execute("UPDATE kv SET v = 60 WHERE k = 2")
            tester.conn2.commit()
        except psycopg2.errors.SerializationFailure:
            t2_serialization_failure = True
            tester.conn2.rollback()

        # 3. T1: Reads Key 2 and tries to commit
        t1_serialization_failure = False
        v2 = None
        try:
            tester.cur1.execute("SELECT v FROM kv WHERE k = 2")
            row2 = tester.cur1.fetchone()
            v2 = row2[0] if row2 is not None else None
            tester.conn1.commit()
        except psycopg2.errors.SerializationFailure:
            t1_serialization_failure = True
            tester.conn1.rollback()

        if t1_serialization_failure or t2_serialization_failure:
            tester.report(
                anomaly_name,
                True,
                "At least one transaction was aborted with SerializationFailure; read skew prevented.",
            )
            return 0

        # case: both transactions committed
        if v1 is None or v2 is None:
            tester.report(
                anomaly_name,
                True,
                f"T1 saw values v1={v1!r}, v2={v2!r}; treating as no read skew.",
            )
            return 0

        total = v1 + v2
        if total == 110:
            tester.report(
                anomaly_name,
                False,
                f"T1 observed inconsistent total {total} (50 + 60); read skew observed.",
            )
            return 1

        tester.report(
            anomaly_name,
            True,
            f"T1 observed total {total}; no read skew observed.",
        )
        return 0

    finally:
        tester.teardown()


def main() -> int:
    parser = argparse.ArgumentParser(description="Litmus test for Read Skew.")
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

