# Redis with tx-fuzzer

How to run litmus tests and chaos/fault-injection against **Redis**.

## Prerequisites

- **Redis** running (local or in Docker): `redis-server` or `docker run -d -p 6379:6379 redis`
- **redis-py**: `pip install redis`

## 1. Run litmus tests

Use **`run_litmus.py`** with `--target redis`:

```bash
# G1 Lost Update (Redis: GET/SET with pipeline MULTI/EXEC)
python scripts/run_litmus.py --target redis --test lost_update

# Atomicity (two keys updated in one pipeline)
python scripts/run_litmus.py --target redis --test atomicity
```

Default DSN is `redis://localhost:6379/0`. Override with `--dsn` if needed:

```bash
python scripts/run_litmus.py --target redis --test lost_update --dsn 'redis://:mypassword@localhost:6379/0'
```

### Options

| Flag | Description |
|------|-------------|
| `--target redis` | Use Redis adapter. |
| `--test lost_update \| atomicity` | Which litmus test to run. |
| `--dsn 'redis://[user:password@]host:port/db'` | Optional. Default: `redis://localhost:6379/0`. |

**Note:** Redis has no SQL or transaction isolation levels. The adapter maps:

- **Begin** → `pipeline.multi()`
- **Commit** → `pipeline.execute()`
- **SELECT v FROM kv WHERE k = ?** → `GET kv:k`
- **UPDATE kv SET v = ? WHERE k = ?** → `SET kv:k v`

The `--isolation` flag is accepted but has no effect for Redis.

### Interpretation

- **lost_update**: Same idea as SQL—T1 read, T2 update+commit, T1 update+commit. Result depends on Redis ordering and whether you use WATCH (adapter uses pipelines).
- **atomicity**: One logical transaction sets key 1 and key 2; PASS if both are visible after commit (Redis pipelines are atomic).

## 2. ChaosMonkey (fault injection) with Redis

The **nemesis** (`scripts/nemesis.py`) injects Docker-level faults (pause, kill, disconnect) into **any** container. It does not speak Redis; it targets the container name.

If Redis runs in Docker, run ChaosMonkey against that container while you run litmus or other clients against Redis:

**Terminal 1 – start Redis in Docker (if not already):**

```bash
docker run -d --name redis-litmus -p 6379:6379 redis
```

**Terminal 2 – run nemesis against the Redis container:**

```bash
python scripts/nemesis.py --container redis-litmus --faults pause,kill,disconnect --interval-min 2 --interval-max 8
```

**Terminal 3 – run litmus** (or your own Redis workload):

```bash
python scripts/run_litmus.py --target redis --test atomicity
```

Clients will see connection errors when the container is down; ChaosMonkey will recover the container so you can observe behavior under faults.

### Nemesis options

| Flag | Description |
|------|-------------|
| `--container NAME` | Docker container to fault (default: `txfuzz-pg`). Set to your Redis container (e.g. `redis-litmus`). |
| `--faults pause,kill,disconnect` | Comma-separated list of faults. |
| `--interval-min`, `--interval-max` | Min/max seconds between fault cycles. |

## 3. Uncovering anomalies (mixed-mode attack)

**`run_mixed_attack.py`** repeatedly injects a chosen anomaly pattern while background threads generate random GET/SET noise. Use it to see how often Redis **exhibits** the anomaly under load.

### Run anomaly-hunt against Redis

```bash
# Lost update (G1)
python scripts/run_mixed_attack.py --target redis --attack g1 --duration 60 --noise-threads 10

# Write skew (G2), dirty read (G0), non-repeatable read, read skew
python scripts/run_mixed_attack.py --target redis --attack g2 --duration 60
python scripts/run_mixed_attack.py --target redis --attack dirty_read --duration 60
python scripts/run_mixed_attack.py --target redis --attack non_repeatable --duration 60
python scripts/run_mixed_attack.py --target redis --attack read_skew --duration 60
```

**Note:** `phantom` (P3) is **not supported** for Redis (adapter has no predicate/count reads); it is skipped and never reported as a success.

### Options

| Flag | Description |
|------|-------------|
| `--target redis` | Use Redis. |
| `--attack g1 \| g2 \| dirty_read \| non_repeatable \| read_skew` | Anomaly to inject (no phantom). |
| `--dsn` | Optional. Default `redis://localhost:6379/0`. |
| `--duration N` | Run for N seconds. |
| `--noise-threads N` | Background noise threads (default 10). |
| `--frequency F` | Attack attempts per second (default 2). |

**Interpretation:** **Successful Anomalies** = number of attempts where the anomaly was observed. **Success Rate** > 0% means Redis showed that behavior under the test.

## 4. Other runners (Postgres-only)

- **`graph_fuzzer.py`** – graph-based workload fuzzer; Postgres.
- **`elle_runner.py`** – Elle-style append-only JSONB workload + optional ChaosMonkey; Postgres + JSONB.
- **`experiments/run_scaling_benchmark.py`** – scaling benchmark and chaos mode; Postgres + Go runner.

To use **ChaosMonkey** with Redis, run `nemesis.py` with `--container <your-redis-container>` and run `run_mixed_attack.py --target redis` or `run_litmus.py --target redis` in another terminal.
