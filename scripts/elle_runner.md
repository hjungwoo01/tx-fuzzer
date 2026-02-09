## Elle-Style Append-Only Runner (`elle_runner.py`)

This script runs an **append-only list workload** (Elle-style) against the `kv` table while optionally enabling the `ChaosMonkey` nemesis.

- **`ElleTester` (`elle_utils.py`)**:
  - Ensures `kv(k INT PRIMARY KEY, v JSONB NOT NULL DEFAULT '[]')` exists.
  - Each transaction:
    - Reads `v` for a key (JSONB array).
    - Appends a unique transaction ID (UUID).
    - Writes back the updated JSONB list.
  - This lets you reconstruct exactly which transactions were persisted.

- **`elle_runner.py`**:
  - Spawns multiple client threads, each repeatedly appending new IDs to the same key.
  - Optionally starts `ChaosMonkey` (see below) to inject Docker-level faults into `txfuzz-pg`.
  - At the end:
    - Collects all IDs that reported **COMMIT OK**.
    - Reads the final JSONB list from the database.
    - Flags an **Atomic Visibility Violation** if any committed IDs are missing.

### CLI Usage (`elle_runner.py`)

Key flags:

- `--dsn`  
  PostgreSQL DSN (defaults to the standard tx-fuzzer DSN).

- `--isolation`  
  Transaction isolation level (`READ COMMITTED`, `REPEATABLE READ`, `SERIALIZABLE`).

- `--threads`  
  Number of client threads appending to the list.

- `--key`  
  Integer key in `kv` whose JSONB list to append to (default `1`).

- `--duration`  
  How long to run the workload (seconds).

- `--enable-nemesis`  
  Turn on the ChaosMonkey nemesis (see below).

- **Nemesis configuration**:
  - `--nemesis-faults` (e.g. `pause,kill,disconnect`)
  - `--nemesis-interval-min` / `--nemesis-interval-max` (fault duration / cooldown bounds)

Example:

```bash
python3 scripts/elle_runner.py \
  --threads 16 \
  --key 1 \
  --isolation "READ COMMITTED" \
  --duration 120 \
  --enable-nemesis \
  --nemesis-faults pause,kill,disconnect \
  --nemesis-interval-min 2 \
  --nemesis-interval-max 8
```

The summary will include:

- **Total COMMIT OK**
- **Final list length**
- **Lost commit IDs** (count and sample), if any.

### Tips for finding anomalies

1. **Atomic visibility (this runner + nemesis)**  
   To increase the chance of **lost commits** (commit reported but ID missing from the list):
   - Increase stress: `--threads 32` (or more), `--duration 300` (or longer).
   - Make chaos more aggressive: `--nemesis-faults kill` (only kill, no pause/disconnect) and shorter intervals, e.g. `--nemesis-interval-min 1 --nemesis-interval-max 4`.
   - Run **many times** (e.g. 10–50 runs); anomalies are often rare.

2. **Isolation anomalies (lost update, phantom, read skew, etc.)**  
   Use the **mixed-mode runner** instead: `run_mixed_attack.py` with `--attack g1` (or `phantom`, `read_skew`, etc.) and `--isolation "READ COMMITTED"`.  
   - **Success Rate > 0%** in the summary means the anomaly was observed (e.g. lost update happened).  
   - With `--isolation "SERIALIZABLE"` you typically see 0% (DB prevents the anomaly).  
   - Use `--noise-threads 20` and `--duration 60` (or more) so the attack runs under load and has more chances to interleave badly.

3. **Deterministic litmus tests**  
   Run the scripts in `litmus/` (e.g. `python3 litmus/test_g1_lost_update.py --isolation "READ COMMITTED"`) to confirm the DB **fails** the test (vulnerable) or **passes** (safe). Use `SERIALIZABLE` to see passes.

***

## Nemesis / ChaosMonkey (`nemesis.py`)

The `ChaosMonkey` is a simple Jepsen-style **fault injector** for the `txfuzz-pg` Docker container.

- **Supported faults**:
  - `pause` / `unpause`
  - `kill` / `start`
  - `disconnect` / `reconnect` (from Docker networks)

- **Behavior**:
  - Picks a random fault from the configured list.
  - Applies it to `txfuzz-pg`.
  - Sleeps for a random duration in \[interval_min, interval_max].
  - Recovers the container (unpause, start, reconnect to at least the `bridge` network).
  - Repeats until stopped.

### CLI Usage (`nemesis.py`)

```bash
python3 scripts/nemesis.py \
  --container txfuzz-pg \
  --faults pause,kill,disconnect \
  --interval-min 2 \
  --interval-max 10
```

Or, import and run it inside another script (as `elle_runner.py` does) to combine **fault injection** with your workloads.

***

## Mixed-Mode Attack Runner (`run_mixed_attack.py`)

This script lets you run a **deterministic anomaly attack** (based on your litmus tests) while a noisy background workload is hammering the same database.

It answers questions like:

- "Does this anomaly still show up when the system is under load?"
- "How often can we trigger Lost Update / Phantom / Read Skew in practice?"

### How it Works

- **`AttackInjector`**:
  - Reuses a single `LitmusTester` (two PostgreSQL connections) to repeatedly execute one anomaly pattern.
  - Supports all six litmus-style anomalies:
    - `g0` / `dirty` / `dirty_read` → **Dirty Read**
    - `g1` / `lost_update` → **Lost Update**
    - `g2` / `write_skew` → **Write Skew**
    - `nr` / `nonrepeatable` / `non-repeatable` → **Non-Repeatable Read**
    - `p3` / `phantom` → **Phantom Read**
    - `read_skew` / `read-skew` / `rs` → **Read Skew**
  - Tracks:
    - **Attempts**: how many times the attack was attempted.
    - **Successful anomalies**: how many times the vulnerable behavior was actually observed.

- **`BackgroundNoise`**:
  - Spawns worker threads that open their **own connections** and continuously:
    - Run random `SELECT`s and `UPDATE`s on keys *other than* the configured `target_key`.
  - Uses the same isolation level configuration as `LitmusTester` (via `SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL ...`).

- **Orchestrator (`main`)**:
  - Starts noise threads and the attack injector.
  - Runs for a fixed duration, then stops everything and prints a summary.

### CLI Usage

Default DSN (can be overridden with `--dsn`):

- `postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable`

Supported flags:

- `--dsn`  
  PostgreSQL connection string (defaults to the above).

- `--isolation`  
  Transaction isolation level for both the attack and noise:
  - `READ COMMITTED` (default)
  - `REPEATABLE READ`
  - `SERIALIZABLE`

- `--attack`  
  Which anomaly to inject (required). Aliases:
  - `g0`, `dirty`, `dirty_read`
  - `g1`, `lost_update`
  - `g2`, `write_skew`, `write-skew`
  - `nr`, `nonrepeatable`, `non-repeatable`
  - `p3`, `phantom`
  - `read_skew`, `read-skew`, `rs`

- `--target-key`  
  Integer key used by the attack pattern (default `1`).
  - Some attacks also use `target_key + 1` (e.g., write skew / read skew).

- `--frequency`  
  Attack attempts per second (default `2.0`).

- `--noise-threads`  
  Number of background noise threads (default `10`).

- `--duration`  
  How long to run the mixed attack in seconds (default `30`).

- `--stagger`  
  Optional delay (seconds) to pause the **victim** transaction inside the attack:
  - Applied between the attacker commit and the victim’s next step / commit.
  - Useful for probing timing-sensitive anomalies.

### Example Commands

- **Lost Update under noisy READ COMMITTED load**:

```bash
python3 scripts/run_mixed_attack.py \
  --attack g1 \
  --isolation "READ COMMITTED" \
  --target-key 1 \
  --frequency 5.0 \
  --noise-threads 20 \
  --duration 60
```

- **Phantom Read with staggered victim under REPEATABLE READ**:

```bash
python3 scripts/run_mixed_attack.py \
  --attack phantom \
  --isolation "REPEATABLE READ" \
  --target-key 999 \
  --frequency 2.0 \
  --noise-threads 10 \
  --duration 45 \
  --stagger 0.05
```

- **Read Skew under SERIALIZABLE**:

```bash
python3 scripts/run_mixed_attack.py \
  --attack read_skew \
  --isolation "SERIALIZABLE" \
  --target-key 100 \
  --frequency 1.0 \
  --noise-threads 8 \
  --duration 30
```

At the end of each run, you’ll see a summary like:

- **Total Attacks Attempted: X**
- **Successful Anomalies: Y**
- **Success Rate: Z%**

