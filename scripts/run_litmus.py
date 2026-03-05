"""
Run a litmus test against a chosen database backend.

Examples:
  python scripts/run_litmus.py --target postgres --test lost_update
  python scripts/run_litmus.py --target mysql --test lost_update
  python scripts/run_litmus.py --target sqlite --test lost_update
  python scripts/run_litmus.py --target redis --test atomicity
"""

from __future__ import annotations

import argparse
import sys
import threading
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from adapters import (
    DatabaseAdapter,
    PostgresAdapter,
    MySQLAdapter,
    SQLiteAdapter,
    RedisAdapter,
)
from litmus import test_g2_write_skew, test_p3_phantom_read


# Default DSNs per target (match docker-compose.yml; override with --dsn)
DEFAULT_DSN = {
    "postgres": "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable",
    "mysql": "mysql://test:test@localhost:3307/test",
    "sqlite": "/tmp/tx_fuzzer_litmus.sqlite",
    "redis": "redis://localhost:6380/0",
}

GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"


def _report(adapter: DatabaseAdapter, anomaly_name: str, safe: bool, message: str) -> None:
    status = f"{GREEN}PASS (Safe){RESET}" if safe else f"{RED}FAIL (Vulnerable){RESET}"
    print(f"[{anomaly_name}] {status} - {message}")


def _is_serialization_or_lock_failure(exc: BaseException, target: str) -> bool:
    if target == "postgres":
        try:
            import psycopg2.errors
            return type(exc) is psycopg2.errors.SerializationFailure
        except Exception:
            return False
    if target == "mysql":
        try:
            import pymysql.err
            return isinstance(exc, (pymysql.err.OperationalError, pymysql.err.InternalError))
        except Exception:
            return False
    return False


def run_lost_update(adapter: DatabaseAdapter, target: str, isolation: str) -> int:
    """
    G1 Lost Update: T1 reads key 1, T2 updates key 1 and commits, T1 updates key 1 and commits.
    Safe if final value is T2's (10) or T1 aborts with serialization/lock failure.
    Uses adapter.read/write when logger is set so execution log shows semantic actions.
    """
    from litmus_utils import LitmusTester
    tester = LitmusTester(adapter=adapter)
    anomaly_name = "G1: Lost Update"
    try:
        tester.setup_db()
        tester.cur1.execute("BEGIN")
        tester.cur2.execute("BEGIN")

        v1 = adapter.read(1, conn_id=1)

        adapter.write(1, 10, conn_id=2)
        tester.conn2.commit()

        saw_abort = False
        try:
            adapter.write(1, 1, conn_id=1)
            tester.conn1.commit()
        except Exception as e:
            if _is_serialization_or_lock_failure(e, target):
                saw_abort = True
            tester.conn1.rollback()

        # Fresh read of key 1
        tester.conn1.autocommit = True
        with tester.conn1.cursor() as c:
            c.execute("SELECT v FROM kv WHERE k = 1")
            final_row = c.fetchone()
            final_v = final_row[0] if final_row is not None else None
        tester.conn1.autocommit = False

        if saw_abort:
            _report(adapter, anomaly_name, True, "T1 was aborted; lost update prevented.")
            return 0
        if final_v == 1:
            _report(adapter, anomaly_name, False, "Final value is 1 (T1 overwrote T2) -> Lost Update observed.")
            return 1
        _report(adapter, anomaly_name, True, f"Final value is {final_v!r}; system prevented lost update.")
        return 0
    finally:
        tester.teardown()
        if getattr(adapter, "_logger", None) is not None and hasattr(adapter._logger, "close"):
            adapter._logger.close()


def run_atomicity(adapter: DatabaseAdapter, target: str, isolation: str) -> int:
    """
    Atomicity: one transaction writes key 1 and key 2; either both visible after commit or neither.
    """
    from litmus_utils import LitmusTester
    tester = LitmusTester(adapter=adapter)
    anomaly_name = "Atomicity"
    try:
        tester.setup_db()
        # Single transaction: set k=1 to 1, k=2 to 2, then commit
        tester.cur1.execute("BEGIN")
        tester.cur1.execute("UPDATE kv SET v = 1 WHERE k = 1")
        tester.cur1.execute("UPDATE kv SET v = 2 WHERE k = 2")
        tester.conn1.commit()

        # Fresh read both keys
        tester.conn1.autocommit = True
        with tester.conn1.cursor() as c:
            c.execute("SELECT v FROM kv WHERE k = 1")
            r1 = c.fetchone()
            c.execute("SELECT v FROM kv WHERE k = 2")
            r2 = c.fetchone()
        v1 = r1[0] if r1 is not None else None
        v2 = r2[0] if r2 is not None else None
        tester.conn1.autocommit = False

        if v1 == 1 and v2 == 2:
            _report(adapter, anomaly_name, True, "Both keys updated atomically (1, 2).")
            return 0
        _report(adapter, anomaly_name, False, f"Expected (1, 2), got ({v1!r}, {v2!r}).")
        return 1
    finally:
        tester.teardown()


TESTS = {
    "lost_update": run_lost_update,
    "atomicity": run_atomicity,
}


def _run_write_skew_stress(
    target: str,
    dsn: str,
    isolation: str,
    iterations: int,
    workers: int,
    adapter: Optional[DatabaseAdapter] = None,
) -> tuple[int, int]:
    """Run write skew scenario many times. Returns (anomaly_count, total_runs)."""
    total_runs = iterations
    if workers <= 1:
        if adapter is None:
            adapter = get_adapter(target, dsn, isolation)
            adapter.connect()
            try:
                anomalies = sum(
                    test_g2_write_skew.run_one(adapter) for _ in range(iterations)
                )
                return (anomalies, iterations)
            finally:
                adapter.teardown()
        anomalies = sum(test_g2_write_skew.run_one(adapter) for _ in range(iterations))
        return (anomalies, iterations)

    # Multi-worker: each thread has its own adapter
    results: list[int] = []
    lock = threading.Lock()

    def worker(runs: int) -> None:
        ad = get_adapter(target, dsn, isolation)
        ad.connect()
        try:
            n = sum(test_g2_write_skew.run_one(ad) for _ in range(runs))
            with lock:
                results.append(n)
        finally:
            ad.teardown()

    chunk = iterations // workers
    remainder = iterations % workers
    threads = []
    for i in range(workers):
        runs = chunk + (1 if i < remainder else 0)
        if runs <= 0:
            continue
        t = threading.Thread(target=worker, args=(runs,))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    return (sum(results), total_runs)


def get_adapter(
    target: str,
    dsn: str,
    isolation: str,
    logger: Optional[object] = None,
) -> DatabaseAdapter:
    target = target.lower()
    if target == "postgres":
        return PostgresAdapter(dsn, isolation_level=isolation, logger=logger)
    if target == "mysql":
        return MySQLAdapter(dsn, isolation_level=isolation, logger=logger)
    if target == "sqlite":
        return SQLiteAdapter(dsn, isolation_level=isolation, logger=logger)
    if target == "redis":
        return RedisAdapter(dsn, isolation_level=isolation, logger=logger)
    raise ValueError(f"Unknown target {target!r}. Use: postgres, mysql, sqlite, redis")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run a litmus test against postgres, mysql, sqlite, or redis.",
    )
    parser.add_argument(
        "--target",
        default="postgres",
        choices=["postgres", "mysql", "sqlite", "redis"],
        help="Database backend to test",
    )
    parser.add_argument(
        "--test",
        default="lost_update",
        choices=list(TESTS) + ["write_skew", "phantom"],
        help="Test to run",
    )
    parser.add_argument(
        "--dsn",
        default=None,
        help="Connection DSN (defaults match docker-compose: mysql 3307, redis 6380). Override if needed.",
    )
    parser.add_argument(
        "--isolation",
        default="READ COMMITTED",
        help="Isolation level for SQL backends (READ COMMITTED, REPEATABLE READ, SERIALIZABLE)",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=1,
        metavar="N",
        help="For write_skew: run the scenario N times (stress/load). Default 1.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        metavar="W",
        help="For write_skew stress: number of concurrent workers. Each has its own connection pair.",
    )
    args = parser.parse_args()
    # Normalize isolation (e.g. SERIALIZATION -> SERIALIZABLE)
    args.isolation = (args.isolation or "").strip().upper().replace("_", " ")
    if args.isolation == "SERIALIZATION":
        args.isolation = "SERIALIZABLE"
    # Write skew (G2): default to REPEATABLE READ only when user didn't set --isolation
    if args.test == "write_skew" and args.isolation == "READ COMMITTED":
        args.isolation = "REPEATABLE READ"
    if args.test == "phantom" and args.target == "redis":
        print("Error: Phantom read test is not supported for Redis (no count/predicate reads).", file=sys.stderr)
        return 2
    dsn = args.dsn or DEFAULT_DSN[args.target]
    # Set up execution history logger for all litmus tests.
    from logger import HistoryLogger
    logs_dir = ROOT / "logs"
    if args.test == "lost_update":
        # Keep the explicit G1 name for compatibility with earlier expectations.
        log_file = "g1_lost_update.log"
    else:
        # Per-test, per-target log file (e.g., write_skew_mysql.log, phantom_postgres.log).
        log_file = f"{args.test}_{args.target}.log"
    logger = HistoryLogger(logs_dir / log_file)
    # Log test configuration (including isolation level) at the top of the trace.
    logger.log(
        "CONFIG",
        "INFO",
        f"target={args.target} test={args.test} isolation={args.isolation} dsn={dsn}",
    )
    adapter = get_adapter(args.target, dsn, args.isolation, logger=logger)
    try:
        adapter.connect()
    except Exception as e:
        raise SystemExit(2) from e
    try:
        adapter.setup_schema()
        if args.test == "write_skew":
            print(f"Running Write Skew (G2) test against {args.target} (isolation: {args.isolation})...")
            if args.iterations <= 1 and args.workers <= 1:
                return test_g2_write_skew.run_test(adapter)
            # Stress/load: run write skew scenario many times, optionally with concurrent workers
            total_anomalies, total_runs = _run_write_skew_stress(
                target=args.target,
                dsn=dsn,
                isolation=args.isolation,
                iterations=args.iterations,
                workers=args.workers,
                adapter=adapter,
            )
            print(
                f"Write Skew (G2) stress: {total_runs} runs, {total_anomalies} anomaly(ies) observed."
            )
            if total_anomalies > 0:
                print(f"{RED}FAIL (Vulnerable){RESET} - Write skew detected under REPEATABLE READ.")
                return 1
            print(f"{GREEN}PASS (Safe){RESET} - No write skew observed in {total_runs} runs.")
            return 0
        if args.test == "phantom":
            print(f"Running Phantom Read (P3) test against {args.target} (isolation: {args.isolation})...")
            return test_p3_phantom_read.run_test(adapter)
        return TESTS[args.test](adapter, args.target, args.isolation)
    finally:
        adapter.teardown()


if __name__ == "__main__":
    sys.exit(main())
