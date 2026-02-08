from __future__ import annotations

"""
Elle-style append-only register helper.

`ElleTester` manages a single connection to a dedicated `kv_elle` table (separate
from the litmus `kv` table which has `v bigint`). Schema:

    kv_elle(
      k INT PRIMARY KEY,
      v JSONB NOT NULL  -- append-only list of transaction IDs
    )

Each transaction:
  - Reads the current list for a key.
  - Appends a unique transaction ID.
  - Writes the new list back.
"""

import sys
from typing import List, Optional

import psycopg2
from psycopg2.extensions import connection as PGConnection

from .litmus_utils import _ISOLATION_LEVELS

ELLE_TABLE = "kv_elle"


class ElleTester:
    """Helper for append-only list workloads used for Elle-style analysis."""

    def __init__(
        self,
        dsn: str,
        isolation_level: str = "READ COMMITTED",
        quiet: bool = False,
    ) -> None:
        self.dsn = dsn
        iso_normalized = " ".join(str(isolation_level).upper().split())
        if iso_normalized not in _ISOLATION_LEVELS:
            valid = ", ".join(sorted(_ISOLATION_LEVELS))
            raise ValueError(f"Unsupported isolation level {isolation_level!r}. Valid options: {valid}")

        try:
            self.conn: PGConnection = psycopg2.connect(dsn)
        except Exception as exc:
            if not quiet:
                print(f"[ElleTester] Failed to connect using DSN={dsn!r}: {exc}", file=sys.stderr)
            raise

        self.conn.autocommit = False

        level_sql = _ISOLATION_LEVELS[iso_normalized]
        try:
            with self.conn.cursor() as c:
                c.execute(
                    f"SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL {level_sql}"
                )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def setup_db(self) -> None:
        """
        Ensure the `kv_elle` table exists with `v JSONB` and initialize key 1 with an empty list.
        Uses a separate table so litmus's `kv(k, v bigint)` is unchanged.
        """
        try:
            with self.conn.cursor() as c:
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
                    INSERT INTO {ELLE_TABLE} (k, v)
                    VALUES (1, '[]'::jsonb)
                    ON CONFLICT (k) DO NOTHING
                    """
                )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def append_txn_id(self, key: int, txn_id: str) -> bool:
        """
        Append a transaction ID to the list at `key`.

        Returns True if the transaction committed successfully, False if it
        failed with SerializationFailure (rolled back).
        """
        try:
            with self.conn.cursor() as c:
                c.execute("BEGIN")

                c.execute(
                    f"SELECT v FROM {ELLE_TABLE} WHERE k = %s FOR UPDATE",
                    (key,),
                )
                row = c.fetchone()
                if row is None:
                    c.execute(
                        f"INSERT INTO {ELLE_TABLE} (k, v) VALUES (%s, %s::jsonb)",
                        (key, f'["{txn_id}"]'),
                    )
                else:
                    c.execute(
                        f"UPDATE {ELLE_TABLE} SET v = v || %s::jsonb WHERE k = %s",
                        (f'["{txn_id}"]', key),
                    )

                self.conn.commit()
                return True
        except psycopg2.errors.SerializationFailure:
            self.conn.rollback()
            return False
        except Exception:
            self.conn.rollback()
            raise

    def read_txn_ids(self, key: int) -> List[str]:
        """
        Read the list of transaction IDs for `key`.
        """
        with self.conn.cursor() as c:
            c.execute(f"SELECT v FROM {ELLE_TABLE} WHERE k = %s", (key,))
            row = c.fetchone()
            if row is None or row[0] is None:
                return []
            value = row[0]
            # psycopg2 maps JSONB to Python objects (list[str]).
            if isinstance(value, list):
                return [str(x) for x in value]
            
            import json

            try:
                parsed = json.loads(value)
            except Exception:
                return []
            if isinstance(parsed, list):
                return [str(x) for x in parsed]
            return []

    def teardown(self) -> None:
        try:
            if not self.conn.closed:
                self.conn.close()
        except Exception:
            pass

