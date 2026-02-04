## Litmus Tests for PostgreSQL Anomalies

This folder contains small, focused **litmus tests** that exercise specific transaction anomalies against a simple `kv(k INT PRIMARY KEY, v INT NOT NULL)` table.

All tests share a common helper, `LitmusTester` (`scripts/litmus_utils.py`), which:
- Opens **two PostgreSQL connections**.
- Resets the `kv` table and seeds keys `1` and `2` with value `0`.
- Configures a **transaction isolation level** for the session.
- Prints **PASS (Safe)** / **FAIL (Vulnerable)** results.

By default, tests connect to:
- `postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable`

You can override this with `--dsn`.

### Isolation Level

Each test accepts an `--isolation` flag and maps it to a PostgreSQL isolation level:

- `READ COMMITTED` (default)
- `REPEATABLE READ`
- `SERIALIZABLE`

Example:

```bash
python3 litmus/test_g1_lost_update.py --isolation "SERIALIZABLE"
```

This runs the G1 lost-update litmus under **SERIALIZABLE** and should typically report **PASS (Safe)** if the database correctly blocks the anomaly (often via a `SerializationFailure`).


