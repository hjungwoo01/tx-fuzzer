# MySQL with tx-fuzzer

How to run litmus tests and chaos/fault-injection against **MySQL** (or MariaDB).

## Prerequisites

- **MySQL or MariaDB** running (local or in Docker).
- **pymysql**: `pip install pymysql`

## 1. Setup (first time)

On many Linux systems, `root` uses socket authentication and will get **Access denied (1698)** over TCP. Create a dedicated user with password auth:

```bash
# From project root. If root has a password:
mysql -u root -p < scripts/setup_mysql_litmus.sql

# If root uses socket auth (typical on Linux):
sudo mysql < scripts/setup_mysql_litmus.sql
```

This creates:

- Database: `test`
- User: `test` / Password: `test`
- Full privileges on `test.*`

## 2. Run litmus tests

Use **`run_litmus.py`** with `--target mysql` and `--dsn`:

```bash
# G1 Lost Update
python scripts/run_litmus.py --target mysql --test lost_update

# Atomicity (two keys updated in one transaction)
python scripts/run_litmus.py --target mysql --test atomicity
```

### Options

| Flag | Description |
|------|-------------|
| `--target mysql` | Use MySQL adapter. |
| `--test lost_update \| atomicity` | Which litmus test to run. |
| `--dsn 'mysql://user:pass@host:port/db'` | **Required** for MySQL (no safe default due to 1698). |
| `--isolation READ COMMITTED \| REPEATABLE READ \| SERIALIZABLE` | Transaction isolation level (MySQL/MariaDB syntax). |

### Interpretation

- **lost_update**: PASS = DB prevented lost update (e.g. final value 10 or T1 aborted). FAIL = final value 1 (lost update observed).
- **atomicity**: PASS = both keys updated atomically. FAIL = only one key visible after commit.

Try with `--isolation "SERIALIZABLE"` to see MySQL enforce stronger guarantees (e.g. lost_update should PASS).

## 3. ChaosMonkey (fault injection) with MySQL

The **nemesis** (`scripts/nemesis.py`) injects Docker-level faults (pause, kill, disconnect) into **any** container. It does not speak MySQL; it targets the container name.

If MySQL runs in Docker, run ChaosMonkey against that container while you run workloads or litmus against MySQL:

**Terminal 1 – start MySQL in Docker (if not already):**

```bash
docker run -d --name mysql-litmus -e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=test -e MYSQL_USER=test -e MYSQL_PASSWORD=test -p 3306:3306 mysql:8
```

**Terminal 2 – run nemesis against the MySQL container:**

```bash
python scripts/nemesis.py --container mysql-litmus --faults pause,kill,disconnect --interval-min 2 --interval-max 8
```

**Terminal 3 – run litmus or other clients** against `mysql://test:test@localhost:3306/test` (or your DSN). ChaosMonkey will pause/kill/disconnect the container; clients will see connection errors and can retry.

### Nemesis options

| Flag | Description |
|------|-------------|
| `--container NAME` | Docker container to fault (default: `txfuzz-pg`). Set to your MySQL container. |
| `--faults pause,kill,disconnect` | Comma-separated list of faults. |
| `--interval-min`, `--interval-max` | Min/max seconds between fault cycles. |

## 4. Uncovering anomalies (mixed-mode attack)

**`run_mixed_attack.py`** repeatedly injects a chosen anomaly pattern while background threads generate random read/write noise. Use it to see how often MySQL **exhibits** the anomaly under load (e.g. lost update, write skew).

### Run anomaly-hunt against MySQL

```bash
# Lost update (G1) – success rate > 0% means the anomaly was observed
python scripts/run_mixed_attack.py --target mysql --attack g1 --dsn 'mysql://test:test@localhost:3306/test' --duration 60 --noise-threads 10

# Write skew (G2)
python scripts/run_mixed_attack.py --target mysql --attack g2 --isolation SERIALIZABLE --duration 60

# Dirty read (G0), phantom (P3), non-repeatable read, read skew
python scripts/run_mixed_attack.py --target mysql --attack dirty_read --dsn 'mysql://test:test@localhost:3306/test' --duration 60
python scripts/run_mixed_attack.py --target mysql --attack phantom --isolation SERIALIZABLE --dsn 'mysql://test:test@localhost:3306/test' --duration 60
```

### Options

| Flag | Description |
|------|-------------|
| `--target mysql` | Use MySQL. |
| `--attack g1 \| g2 \| phantom \| dirty_read \| non_repeatable \| read_skew` | Anomaly to inject. |
| `--dsn` | MySQL DSN (required for mysql). |
| `--isolation` | Isolation level: use underscores, e.g. `REPEATABLE_READ`, `READ_COMMITTED`, `SERIALIZABLE` (no quotes). |
| `--duration N` | Run for N seconds. |
| `--noise-threads N` | Background noise threads (default 10). |
| `--frequency F` | Attack attempts per second (default 2). |

**Interpretation:** **Successful Anomalies** = number of attempts where the anomaly was observed (vulnerable behavior). **Success Rate** > 0% means the system showed that anomaly under the test. Try `--isolation SERIALIZABLE` to see lower or zero success rate when MySQL prevents the anomaly.

## 5. Other runners (Postgres-only)

- **`graph_fuzzer.py`** – graph-based workload fuzzer; Postgres and `SerializationFailure`.
- **`elle_runner.py`** – Elle-style append-only JSONB workload + optional ChaosMonkey; Postgres + JSONB.
- **`experiments/run_scaling_benchmark.py`** – scaling benchmark and chaos mode; Postgres + Go runner.

To use **ChaosMonkey** with MySQL, run `nemesis.py` with `--container <your-mysql-container>` and run `run_mixed_attack.py --target mysql` or `run_litmus.py --target mysql` in another terminal.
