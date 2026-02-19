from __future__ import annotations

"""
Litmus test for G1: Lost Update.

Scenario:
    1. T1 reads Key 1 (Value 0).
    2. T2 updates Key 1 to 10 and COMMITS.
    3. T1 updates Key 1 to 1 (based on stale read) and COMMITS.

Check:
    - If final v=1  -> FAIL (T2 was overwritten / lost).
    - If v=10 or T1 aborts with SerializationFailure -> PASS.

When run directly, writes execution history to logs/g1_lost_update.log.
"""

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from adapters import PostgresAdapter
from litmus_utils import LitmusTester
from logger import HistoryLogger

try:
    import psycopg2
except ImportError:
    psycopg2 = None


def run_test(dsn: str, isolation: str, adapter=None) -> int:
    """
    Run G1 Lost Update. If adapter is None, creates PostgresAdapter with execution logger.
    """
    if adapter is None:
        log_path = ROOT / "logs" / "g1_lost_update.log"
        logger = HistoryLogger(log_path)
        logger.log(
            "CONFIG",
            "INFO",
            f"target=postgres test=lost_update isolation={isolation} dsn={dsn}",
        )
        adapter = PostgresAdapter(dsn, isolation_level=isolation, logger=logger)
    tester = LitmusTester(adapter=adapter)
    anomaly_name = "G1: Lost Update"

    try:
        tester.setup_db()

        tester.cur1.execute("BEGIN")
        tester.cur2.execute("BEGIN")

        # 1. T1 reads Key 1 (Value 0)
        v1 = adapter.read(1, conn_id=1)

        # 2. T2 updates Key 1 to 10 and COMMITS
        adapter.write(1, 10, conn_id=2)
        tester.conn2.commit()

        # 3. T1 updates Key 1 to 1 and attempts to COMMIT
        saw_serialization_failure = False
        try:
            adapter.write(1, 1, conn_id=1)
            tester.conn1.commit()
        except Exception as e:
            ser_fail = getattr(psycopg2.errors, "SerializationFailure", None) if psycopg2 else None
            if ser_fail is not None and type(e) is ser_fail:
                saw_serialization_failure = True
            tester.conn1.rollback()

        # Read Key 1 in a fresh transaction
        tester.conn1.autocommit = True
        with tester.conn1.cursor() as c:
            c.execute("SELECT v FROM kv WHERE k = 1")
            final_row = c.fetchone()
            final_v = final_row[0] if final_row is not None else None

        if saw_serialization_failure:
            tester.report(anomaly_name, True, "T1 was aborted with SerializationFailure; lost update prevented.")
            return 0

        if final_v == 1:
            tester.report(anomaly_name, False, "Final value is 1 (T1 overwrote T2) -> Lost Update observed.")
            return 1

        tester.report(
            anomaly_name,
            True,
            f"Final value is {final_v!r} (T2 not lost); system prevented lost update.",
        )
        return 0

    finally:
        tester.teardown()


def main() -> int:
    parser = argparse.ArgumentParser(description="Litmus test for G1: Lost Update.")
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

