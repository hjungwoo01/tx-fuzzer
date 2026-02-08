from __future__ import annotations

"""
Mixed-mode runner: inject deterministic anomaly attacks into a noisy background workload.

High-level idea:
  - `AttackInjector` repeatedly runs a specific anomaly pattern (e.g., G1 lost update)
    against a chosen `target_key`, using `LitmusTester`.
  - `BackgroundNoise` creates concurrent load by running random SELECT / UPDATE traffic
    on other keys.

You can control:
  - Which anomaly to inject (`--attack`)
  - How many noise threads to run (`--noise-threads`)
  - How long to run the experiment (`--duration`)
  - Optional staggering of the victim commit within each anomaly (`--stagger`)
"""

import argparse
import random
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import psycopg2

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from litmus_utils import LitmusTester


@dataclass
class AttackStats:
    attempts: int = 0
    successes: int = 0  # "success" = anomaly was actually observed (vulnerable behavior)


class AttackInjector:
    """
    Repeatedly inject a specific anomaly pattern using `LitmusTester`.
    """

    def __init__(
        self,
        dsn: str,
        isolation_level: str,
        anomaly_type: str,
        target_key: int,
        frequency: float,
        stagger_seconds: Optional[float] = None,
    ) -> None:
        self.dsn = dsn
        self.isolation_level = isolation_level
        self.anomaly_type = anomaly_type.lower()
        self.target_key = target_key
        self.frequency = max(frequency, 0.0)
        self.stagger_seconds = stagger_seconds

        self.stats = AttackStats()
        self._tester: Optional[LitmusTester] = None
        self._lock = threading.Lock()

    def _get_tester(self) -> LitmusTester:
        if self._tester is None:
            self._tester = LitmusTester(self.dsn, isolation_level=self.isolation_level)
        return self._tester

    # --- Individual anomaly patterns -------------------------------------------------

    def _attack_g1_lost_update(self) -> bool:
        """
        Lost Update (G1) against `target_key`.

        Returns True if the anomaly is observed (vulnerable behavior).
        """
        tester = self._get_tester()

        # Ensure the target key exists with a reasonable baseline value.
        tester.cur1.execute("BEGIN")
        tester.cur1.execute(
            "INSERT INTO kv (k, v) VALUES (%s, 0) ON CONFLICT (k) DO NOTHING",
            (self.target_key,),
        )
        tester.conn1.commit()

        # Begin both transactions.
        tester.cur1.execute("BEGIN")
        tester.cur2.execute("BEGIN")

        # T1 reads target_key (victim).
        tester.cur1.execute("SELECT v FROM kv WHERE k = %s", (self.target_key,))
        _row = tester.cur1.fetchone()

        # T2 overwrites target_key and commits.
        tester.cur2.execute("UPDATE kv SET v = 10 WHERE k = %s", (self.target_key,))
        tester.conn2.commit()

        # Optional stagger after attacker commit but before victim resumes.
        if self.stagger_seconds and self.stagger_seconds > 0:
            time.sleep(self.stagger_seconds)

        # T1 blindly writes based on stale view and tries to commit.
        saw_serialization_failure = False
        try:
            tester.cur1.execute("UPDATE kv SET v = 1 WHERE k = %s", (self.target_key,))
            tester.conn1.commit()
        except psycopg2.errors.SerializationFailure:
            saw_serialization_failure = True
            tester.conn1.rollback()

        # Final check: read in a fresh transaction.
        tester.conn1.autocommit = True
        with tester.conn1.cursor() as c:
            c.execute("SELECT v FROM kv WHERE k = %s", (self.target_key,))
            row = c.fetchone()
            final_v = row[0] if row is not None else None
        tester.conn1.autocommit = False

        if saw_serialization_failure:
            # Database blocked the lost update.
            return False

        # Anomaly "success" if T1 overwrote T2.
        return final_v == 1

    def _attack_g0_dirty_read(self) -> bool:
        """
        Dirty Read (G0) against `target_key`.

        Returns True if T2 observes T1's uncommitted write.
        """
        tester = self._get_tester()

        # Ensure the target key exists with a baseline value.
        tester.cur1.execute("BEGIN")
        tester.cur1.execute(
            "INSERT INTO kv (k, v) VALUES (%s, 0) ON CONFLICT (k) DO NOTHING",
            (self.target_key,),
        )
        tester.conn1.commit()

        # Start both transactions.
        tester.cur1.execute("BEGIN")
        tester.cur2.execute("BEGIN")

        # T1: uncommitted update.
        tester.cur1.execute("UPDATE kv SET v = 99 WHERE k = %s", (self.target_key,))

        # T2: attempt to read.
        read_value = None
        try:
            tester.cur2.execute("SELECT v FROM kv WHERE k = %s", (self.target_key,))
            row = tester.cur2.fetchone()
            read_value = row[0] if row is not None else None
            tester.conn2.commit()
        except psycopg2.errors.SerializationFailure:
            tester.conn2.rollback()
            # Dirty read prevented.
            tester.conn1.rollback()
            return False

        # T1: roll back its change.
        tester.conn1.rollback()

        # Anomaly "success" if T2 saw the uncommitted value.
        return read_value == 99

    def _attack_g2_write_skew(self) -> bool:
        """
        Write Skew (G2) cross-over pattern using `target_key` and `target_key + 1`.

        Returns True if the anomaly is observed (both commits succeed).
        """
        tester = self._get_tester()
        k1 = self.target_key
        k2 = self.target_key + 1

        # Ensure both keys exist.
        tester.cur1.execute("BEGIN")
        tester.cur1.execute(
            "INSERT INTO kv (k, v) VALUES (%s, 0) ON CONFLICT (k) DO NOTHING",
            (k1,),
        )
        tester.cur1.execute(
            "INSERT INTO kv (k, v) VALUES (%s, 0) ON CONFLICT (k) DO NOTHING",
            (k2,),
        )
        tester.conn1.commit()

        tester.cur1.execute("BEGIN")
        tester.cur2.execute("BEGIN")

        # 1. T1 reads k1.
        tester.cur1.execute("SELECT v FROM kv WHERE k = %s", (k1,))
        _ = tester.cur1.fetchone()

        # 2. T2 reads k2.
        tester.cur2.execute("SELECT v FROM kv WHERE k = %s", (k2,))
        _ = tester.cur2.fetchone()

        t1_committed = False
        t2_committed = False

        # 3. T1 updates k2 and commits.
        try:
            tester.cur1.execute("UPDATE kv SET v = 100 WHERE k = %s", (k2,))
            tester.conn1.commit()
            t1_committed = True
        except psycopg2.errors.SerializationFailure:
            tester.conn1.rollback()

        # 4. T2 updates k1 and commits.
        try:
            tester.cur2.execute("UPDATE kv SET v = 100 WHERE k = %s", (k1,))
            tester.conn2.commit()
            t2_committed = True
        except psycopg2.errors.SerializationFailure:
            tester.conn2.rollback()

        # Anomaly "success" if both commits succeeded.
        return t1_committed and t2_committed

    def _attack_p3_phantom(self) -> bool:
        """
        Phantom Read (P3) on predicate v = 10.

        Uses `target_key` as the key for the inserted phantom row.

        Returns True if the phantom is observed (count grows within T1).
        """
        tester = self._get_tester()

        # Setup: ensure there are at least two rows with v = 10.
        tester.cur1.execute("BEGIN")
        tester.cur1.execute(
            "INSERT INTO kv (k, v) VALUES (%s, 10) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
            (1,),
        )
        tester.cur1.execute(
            "INSERT INTO kv (k, v) VALUES (%s, 10) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
            (2,),
        )
        tester.conn1.commit()

        tester.cur1.execute("BEGIN")
        tester.cur2.execute("BEGIN")

        # T1: initial count.
        tester.cur1.execute("SELECT count(*) FROM kv WHERE v = 10")
        row = tester.cur1.fetchone()
        initial_count = int(row[0]) if row is not None else 0

        # T2: insert a new row with v = 10 and commit.
        try:
            tester.cur2.execute(
                "INSERT INTO kv (k, v) VALUES (%s, 10) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
                (self.target_key,),
            )
            tester.conn2.commit()
        except psycopg2.errors.SerializationFailure:
            tester.conn2.rollback()
            # Phantom prevented.
            tester.conn1.rollback()
            return False

        # Optional stagger after T2 commit before T1's re-check.
        if self.stagger_seconds and self.stagger_seconds > 0:
            time.sleep(self.stagger_seconds)

        # T1: re-run the count.
        try:
            tester.cur1.execute("SELECT count(*) FROM kv WHERE v = 10")
            row2 = tester.cur1.fetchone()
            final_count = int(row2[0]) if row2 is not None else 0
            tester.conn1.commit()
        except psycopg2.errors.SerializationFailure:
            tester.conn1.rollback()
            # Phantom prevented.
            return False

        # Anomaly "success" if the count grew within one transaction.
        return final_count > initial_count

    def _attack_non_repeatable(self) -> bool:
        """
        Non-Repeatable Read on `target_key`.

        Returns True if T1 sees two different values within one transaction.
        """
        tester = self._get_tester()

        tester.cur1.execute("BEGIN")
        tester.cur1.execute(
            "INSERT INTO kv (k, v) VALUES (%s, 0) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
            (self.target_key,),
        )
        tester.conn1.commit()

        tester.cur1.execute("BEGIN")
        tester.cur2.execute("BEGIN")

        tester.cur1.execute("SELECT v FROM kv WHERE k = %s", (self.target_key,))
        row1 = tester.cur1.fetchone()
        first_v = row1[0] if row1 is not None else None

        try:
            tester.cur2.execute("UPDATE kv SET v = 50 WHERE k = %s", (self.target_key,))
            tester.conn2.commit()
        except psycopg2.errors.SerializationFailure:
            tester.conn2.rollback()
            tester.conn1.rollback()
            
            return False

        if self.stagger_seconds and self.stagger_seconds > 0:
            time.sleep(self.stagger_seconds)

        try:
            tester.cur1.execute("SELECT v FROM kv WHERE k = %s", (self.target_key,))
            row2 = tester.cur1.fetchone()
            second_v = row2[0] if row2 is not None else None
            tester.conn1.commit()
        except psycopg2.errors.SerializationFailure:
            tester.conn1.rollback()
            return False

        return first_v != second_v

    def _attack_read_skew(self) -> bool:
        """
        Read Skew using `target_key` and `target_key + 1`.

        Returns True if T1 observes an inconsistent total (e.g., 50 + 60 = 110).
        """
        tester = self._get_tester()

        k1 = self.target_key
        k2 = self.target_key + 1

        tester.cur1.execute("BEGIN")
        tester.cur1.execute(
            "INSERT INTO kv (k, v) VALUES (%s, 50) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
            (k1,),
        )
        tester.cur1.execute(
            "INSERT INTO kv (k, v) VALUES (%s, 50) ON CONFLICT (k) DO UPDATE SET v = EXCLUDED.v",
            (k2,),
        )
        tester.conn1.commit()

        tester.cur1.execute("BEGIN")
        tester.cur2.execute("BEGIN")

        tester.cur1.execute("SELECT v FROM kv WHERE k = %s", (k1,))
        row1 = tester.cur1.fetchone()
        v1 = row1[0] if row1 is not None else None

        t2_failed = False
        try:
            tester.cur2.execute("UPDATE kv SET v = 40 WHERE k = %s", (k1,))
            tester.cur2.execute("UPDATE kv SET v = 60 WHERE k = %s", (k2,))
            tester.conn2.commit()
        except psycopg2.errors.SerializationFailure:
            t2_failed = True
            tester.conn2.rollback()

        if self.stagger_seconds and self.stagger_seconds > 0:
            time.sleep(self.stagger_seconds)

        t1_failed = False
        v2 = None
        try:
            tester.cur1.execute("SELECT v FROM kv WHERE k = %s", (k2,))
            row2 = tester.cur1.fetchone()
            v2 = row2[0] if row2 is not None else None
            tester.conn1.commit()
        except psycopg2.errors.SerializationFailure:
            t1_failed = True
            tester.conn1.rollback()

        if t1_failed or t2_failed:
            return False

        if v1 is None or v2 is None:
            return False

        total = v1 + v2
        return total == 110

    def _run_single_attack(self) -> bool:
        """
        Run one attack attempt and return True if the anomaly was observed.
        """
        if self.anomaly_type in ("g0", "dirty", "dirty_read"):
            return self._attack_g0_dirty_read()
        if self.anomaly_type in ("g1", "lost_update"):
            return self._attack_g1_lost_update()
        if self.anomaly_type in ("g2", "write_skew", "write-skew"):
            return self._attack_g2_write_skew()
        if self.anomaly_type in ("p3", "phantom"):
            return self._attack_p3_phantom()
        if self.anomaly_type in ("nr", "nonrepeatable", "non-repeatable"):
            return self._attack_non_repeatable()
        if self.anomaly_type in ("read_skew", "read-skew", "rs"):
            return self._attack_read_skew()
        raise ValueError(f"Unsupported attack type: {self.anomaly_type!r}")

    def run(self, stop_event: threading.Event) -> None:
        """
        Main loop: repeatedly run the configured attack until `stop_event` is set.
        """
        period = 1.0 / self.frequency if self.frequency > 0 else 0.0

        while not stop_event.is_set():
            start = time.time()

            try:
                vulnerable = self._run_single_attack()
            except Exception as exc:
                with self._lock:
                    self.stats.attempts += 1
                print(f"[AttackInjector] ERROR during attack attempt: {exc}", file=sys.stderr)
            else:
                with self._lock:
                    self.stats.attempts += 1
                    if vulnerable:
                        self.stats.successes += 1

            if period > 0:
                elapsed = time.time() - start
                sleep_for = period - elapsed
                if sleep_for > 0:
                    time.sleep(sleep_for)

        if self._tester is not None:
            self._tester.teardown()


class BackgroundNoise:
    """
    Generate random SELECT / UPDATE noise on keys other than `target_key`.
    """

    def __init__(
        self,
        dsn: str,
        isolation_level: str,
        thread_count: int,
        target_key: int,
    ) -> None:
        self.dsn = dsn
        self.isolation_level = isolation_level
        self.thread_count = max(thread_count, 0)
        self.target_key = target_key
        self._threads: list[threading.Thread] = []

    def _noise_worker(self, stop_event: threading.Event) -> None:
        try:
            conn = psycopg2.connect(self.dsn)
        except Exception as exc:
            print(f"[BackgroundNoise] Failed to connect: {exc}", file=sys.stderr)
            return

        conn.autocommit = False

        from litmus_utils import _ISOLATION_LEVELS

        iso_normalized = " ".join(str(self.isolation_level).upper().split())
        level_sql = _ISOLATION_LEVELS.get(iso_normalized, "READ COMMITTED")

        try:
            with conn.cursor() as c:
                c.execute(
                    f"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {level_sql}"
                )
            conn.commit()
        except Exception:
            conn.rollback()

        cur = conn.cursor()

        try:
            while not stop_event.is_set():
                key = self.target_key
                while key == self.target_key:
                    key = random.randint(1, 1000)

                try:
                    cur.execute("BEGIN")
                    if random.random() < 0.5:
                        cur.execute("SELECT v FROM kv WHERE k = %s", (key,))
                        _ = cur.fetchone()
                    else:
                        delta = random.randint(-5, 5)
                        cur.execute(
                            "UPDATE kv SET v = COALESCE(v, 0) + %s WHERE k = %s",
                            (delta, key),
                        )
                    conn.commit()
                except Exception:
                    conn.rollback()

                time.sleep(0.005)
        finally:
            try:
                cur.close()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass

    def start(self, stop_event: threading.Event) -> None:
        for _ in range(self.thread_count):
            t = threading.Thread(target=self._noise_worker, args=(stop_event,), daemon=True)
            t.start()
            self._threads.append(t)

    def join(self) -> None:
        for t in self._threads:
            t.join(timeout=1.0)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Mixed-mode runner: inject anomaly attacks under noisy background load.",
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
        "--attack",
        required=True,
        help='Anomaly to inject (e.g., "g1", "g2", "phantom").',
    )
    parser.add_argument(
        "--target-key",
        type=int,
        default=1,
        help="Target key for the attack pattern (default: 1).",
    )
    parser.add_argument(
        "--frequency",
        type=float,
        default=2.0,
        help="Attack attempts per second (default: 2.0).",
    )
    parser.add_argument(
        "--noise-threads",
        type=int,
        default=10,
        help="Number of background noise threads (default: 10).",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=30.0,
        help="Duration of the experiment in seconds (default: 30).",
    )
    parser.add_argument(
        "--stagger",
        type=float,
        default=0.0,
        help=(
            "Optional delay in seconds to pause the victim transaction "
            "after the attacker commit but before the victim resumes."
        ),
    )

    args = parser.parse_args()

    stop_event = threading.Event()

    injector = AttackInjector(
        dsn=args.dsn,
        isolation_level=args.isolation,
        anomaly_type=args.attack,
        target_key=args.target_key,
        frequency=args.frequency,
        stagger_seconds=args.stagger if args.stagger and args.stagger > 0 else None,
    )

    noise = BackgroundNoise(
        dsn=args.dsn,
        isolation_level=args.isolation,
        thread_count=args.noise_threads,
        target_key=args.target_key,
    )

    print(
        f"Starting mixed attack: attack={args.attack}, target_key={args.target_key}, "
        f"isolation={args.isolation}, frequency={args.frequency}/s, "
        f"noise_threads={args.noise_threads}, duration={args.duration}s, "
        f"stagger={args.stagger or 0.0}s",
        file=sys.stderr,
    )

    noise.start(stop_event)
    attack_thread = threading.Thread(target=injector.run, args=(stop_event,), daemon=True)
    attack_thread.start()

    try:
        time.sleep(args.duration)
    except KeyboardInterrupt:
        print("\n[run_mixed_attack] Interrupted by user; stopping...", file=sys.stderr)

    stop_event.set()
    attack_thread.join(timeout=5.0)
    noise.join()

    attempts = injector.stats.attempts
    successes = injector.stats.successes
    success_rate = (successes / attempts * 100.0) if attempts > 0 else 0.0

    print("\n=== Mixed Attack Summary ===")
    print(f"Attack type          : {args.attack}")
    print(f"Isolation level      : {args.isolation}")
    print(f"Target key           : {args.target_key}")
    print(f"Duration (s)         : {args.duration}")
    print(f"Total Attacks Attempted: {attempts}")
    print(f"Successful Anomalies    : {successes}")
    print(f"Success Rate            : {success_rate:.2f}%")

    return 0


if __name__ == "__main__":
    sys.exit(main())

