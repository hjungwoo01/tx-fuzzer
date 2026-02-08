from __future__ import annotations

"""
Elle-style append-only workload runner with optional ChaosMonkey nemesis.

This script:
  - Spawns multiple client threads that repeatedly append unique transaction IDs
    to an append-only JSONB list in `kv`.
  - Optionally starts the `ChaosMonkey` from `scripts/nemesis.py` to inject
    pause/kill/disconnect faults into the `txfuzz-pg` container.
  - At the end, reads back the final list and checks for "lost commits":
      any transaction that reported COMMIT OK but whose ID is missing.
"""

import argparse
import sys
import threading
import time
import uuid
from pathlib import Path
from typing import List, Set

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.elle_utils import ELLE_TABLE, ElleTester
from scripts.nemesis import ChaosConfig, ChaosMonkey


def _reset_elle_key(dsn: str, key: int) -> None:
    """Reset the append-only list for `key` to empty so this run starts fresh."""
    conn = psycopg2.connect(dsn)
    try:
        conn.autocommit = True
        with conn.cursor() as c:
            c.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {ELLE_TABLE} (
                    k INT PRIMARY KEY,
                    v JSONB NOT NULL DEFAULT '[]'::jsonb
                )
                """
            )
            c.execute(
                f"""
                INSERT INTO {ELLE_TABLE} (k, v) VALUES (%s, '[]'::jsonb)
                ON CONFLICT (k) DO UPDATE SET v = '[]'::jsonb
                """,
                (key,),
            )
    finally:
        conn.close()


def _client_worker(
    dsn: str,
    isolation: str,
    key: int,
    stop_event: threading.Event,
    committed_ids: List[str],
    failed_ids: List[str],
    other_errors: List[None],
    stats_lock: threading.Lock,
) -> None:
    """Run append loop; reconnect with backoff on connection/operational errors."""
    tester = None
    backoff = 1.0
    max_backoff = 15.0

    while not stop_event.is_set():
        if tester is None:
            try:
                tester = ElleTester(dsn, isolation_level=isolation, quiet=True)
                tester.setup_db()
                backoff = 1.0
            except (psycopg2.OperationalError, psycopg2.InterfaceError):
                with stats_lock:
                    other_errors.append(None)
                try:
                    time.sleep(backoff)
                except Exception:
                    pass
                backoff = min(backoff * 1.5, max_backoff)
                continue

        try:
            txn_id = str(uuid.uuid4())
            ok = tester.append_txn_id(key, txn_id)
            with stats_lock:
                if ok:
                    committed_ids.append(txn_id)
                else:
                    failed_ids.append(txn_id)
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            if tester is not None:
                try:
                    tester.teardown()
                except Exception:
                    pass
                tester = None
            with stats_lock:
                other_errors.append(None)
            try:
                time.sleep(backoff)
            except Exception:
                pass
            backoff = min(backoff * 1.5, max_backoff)
        except Exception:
            if tester is not None:
                try:
                    tester.teardown()
                except Exception:
                    pass
                tester = None
            with stats_lock:
                other_errors.append(None)
            backoff = min(backoff * 1.5, max_backoff)

    if tester is not None:
        try:
            tester.teardown()
        except Exception:
            pass


def _read_final_ids(dsn: str, key: int) -> List[str]:
    conn = psycopg2.connect(dsn)
    try:
        conn.autocommit = True
        from psycopg2.extras import Json

        with conn.cursor() as c:
            c.execute(f"SELECT v FROM {ELLE_TABLE} WHERE k = %s", (key,))
            row = c.fetchone()
            if not row:
                return []
            val = row[0]
            if isinstance(val, list):
                return [str(x) for x in val]
            import json

            try:
                parsed = json.loads(val)
            except Exception:
                return []
            if isinstance(parsed, list):
                return [str(x) for x in parsed]
            return []
    finally:
        conn.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Elle-style append-only workload runner with optional ChaosMonkey.",
    )
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
        help="Transaction isolation level (READ COMMITTED, REPEATABLE READ, SERIALIZABLE).",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=8,
        help="Number of client threads appending to the list (default: 8).",
    )
    parser.add_argument(
        "--key",
        type=int,
        default=1,
        help="Key in `kv` whose JSONB list to append to (default: 1).",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=60.0,
        help="Duration of the run in seconds (default: 60).",
    )
    parser.add_argument(
        "--enable-nemesis",
        action="store_true",
        help="Enable ChaosMonkey nemesis against the txfuzz-pg container.",
    )
    parser.add_argument(
        "--nemesis-faults",
        type=str,
        default="pause,kill,disconnect",
        help="Comma-separated faults for ChaosMonkey: pause, kill, disconnect.",
    )
    parser.add_argument(
        "--nemesis-interval-min",
        type=float,
        default=2.0,
        help="Minimum fault duration / cooldown (seconds).",
    )
    parser.add_argument(
        "--nemesis-interval-max",
        type=float,
        default=10.0,
        help="Maximum fault duration / cooldown (seconds).",
    )

    args = parser.parse_args()

    print(
        f"Starting Elle append-only run: threads={args.threads}, key={args.key}, "
        f"isolation={args.isolation}, duration={args.duration}s, "
        f"nemesis={'on' if args.enable_nemesis else 'off'}",
        file=sys.stderr,
    )

    for attempt in range(5):
        try:
            _reset_elle_key(args.dsn, args.key)
            break
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            if attempt < 4:
                time.sleep(2)
            else:
                print("[elle_runner] WARNING: could not reset kv_elle; list may include data from previous runs.", file=sys.stderr)

    stop_event = threading.Event()

    client_threads: List[threading.Thread] = []
    committed_ids: List[str] = []
    failed_ids: List[str] = []
    other_errors: List[None] = []
    stats_lock = threading.Lock()

    for _ in range(args.threads):
        t = threading.Thread(
            target=_client_worker,
            args=(
                args.dsn,
                args.isolation,
                args.key,
                stop_event,
                committed_ids,
                failed_ids,
                other_errors,
                stats_lock,
            ),
            daemon=True,
        )
        t.start()
        client_threads.append(t)

    nemesis_thread: threading.Thread | None = None
    nemesis_stop = threading.Event()
    if args.enable_nemesis:
        from scripts.nemesis import _parse_faults

        cfg = ChaosConfig(
            faults=_parse_faults(args.nemesis_faults),
            interval_min=args.nemesis_interval_min,
            interval_max=args.nemesis_interval_max,
        )
        monkey = ChaosMonkey(cfg)
        nemesis_thread = threading.Thread(target=monkey.run, args=(nemesis_stop,), daemon=True)
        nemesis_thread.start()

    try:
        time.sleep(args.duration)
    except KeyboardInterrupt:
        print("\n[elle_runner] Interrupted by user; stopping...", file=sys.stderr)

    stop_event.set()
    nemesis_stop.set()

    for t in client_threads:
        t.join(timeout=5.0)
    if nemesis_thread is not None:
        nemesis_thread.join(timeout=5.0)

    committed_set: Set[str] = set(committed_ids)
    observed_ids: List[str] = []
    for attempt in range(5):
        try:
            observed_ids = _read_final_ids(args.dsn, args.key)
            break
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            if attempt < 4:
                time.sleep(2)
    observed_set: Set[str] = set(observed_ids)

    lost_commits = committed_set - observed_set

    print("\n=== Elle Append-Only Summary ===")
    print(f"Threads                 : {args.threads}")
    print(f"Isolation               : {args.isolation}")
    print(f"Duration (s)            : {args.duration}")
    print(f"Total COMMIT OK         : {len(committed_ids)}")
    print(f"Total failed/aborted    : {len(failed_ids)}")
    print(f"Connection/other errors : {len(other_errors)}")
    print(f"Final list length       : {len(observed_ids)}")
    print(f"Lost commit IDs         : {len(lost_commits)}")

    if lost_commits:
        print("Atomic visibility violation detected! Some COMMIT OK txns are missing from the list.")

        sample = list(lost_commits)[:10]
        print(f"Sample missing IDs: {sample}")
    else:
        print("No atomic visibility violations detected (all COMMIT OK txns are present).")

    return 0


if __name__ == "__main__":
    sys.exit(main())

