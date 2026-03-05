# Docker setup for tx-fuzzer

Use MySQL (and optionally Redis) for litmus tests and mixed-attack fuzzing.

## Quick start: MySQL

From the project root:

```bash
docker compose up -d
```

Then run litmus tests against MySQL (default DSN already points at the compose MySQL):

```bash
python scripts/run_litmus.py --target mysql --test lost_update
python scripts/run_litmus.py --target mysql --test atomicity
```

See **scripts/mysql.md** for full MySQL usage (chaos, mixed-attack, options).

## MySQL + Redis (compose)

| Service | Container      | Port (host) | DSN |
|---------|----------------|-------------|-----|
| MySQL   | `mysql-litmus` | 3307        | `mysql://test:test@localhost:3307/test` |
| Redis   | `redis-litmus` | 6380        | `redis://localhost:6380/0` |

```bash
docker compose up -d
docker compose down
```

Default DSNs in `run_litmus.py` and `run_mixed_attack.py` match the table above.
