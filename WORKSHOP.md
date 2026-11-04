# Workshop Guide: Breaking (and Fixing) Transactions on Purpose

This is the facilitator guide and attendee handout for the hands-on session.
The submission package (abstract, outcomes, audience) is in
[docs/workshop/SUBMISSION.md](docs/workshop/SUBMISSION.md).  The audit
behind the tool's current design is in
[docs/workshop/AUDIT.md](docs/workshop/AUDIT.md).

Everything below uses one entry point, `python -m isotest`, run from the
repository root.  **Part 1 needs only Python 3.9+** (SQLite ships with
Python: no Docker, no pip install, no network).  Parts 2 and 3 need a
PostgreSQL server.

## Before the session (attendees, 5 minutes)

```bash
git clone --depth 1 https://github.com/hjungwoo01/tx-fuzzer.git   # ~14 MB; full history is ~60 MB
cd tx-fuzzer
python3 -m isotest doctor            # must end with "Ready."
python3 -m isotest lab --dry-run     # shows the 8 steps; touches nothing
```

For Parts 2 and 3 (PostgreSQL), either use the room's shared server (below)
or run one locally:

```bash
docker compose -f docker-compose-multidb.yml up -d pg16_2   # PostgreSQL 16 on localhost:54326
python3 -m pip install psycopg2-binary
python3 -m isotest doctor --db postgres
```

`doctor` probes `localhost:5432` (native install) and then `localhost:54326`
(the compose file).  Set `POSTGRES_DSN` to use any other server.

## Facilitator: one shared PostgreSQL for the whole room

Every `isotest` session works in a private schema (`isotest_<pid>_<hex>`)
that it drops on exit, so 30 attendees can share one database without
clobbering each other's tables.  Each attendee's role needs `CREATE` on the
database:

```sql
CREATE DATABASE workshop;
CREATE ROLE attendee LOGIN PASSWORD 'change-me';
GRANT CREATE, CONNECT ON DATABASE workshop TO attendee;
```

```bash
export POSTGRES_DSN='postgres://attendee:change-me@<host>:5432/workshop'
```

If `CREATE` is denied, the CLI warns and falls back to the shared schema,
and concurrent attendees would then interfere.  Stale schemas from killed
sessions are harmless; drop them with
`DROP SCHEMA isotest_<...> CASCADE`.

## Agenda (90 minutes)

| Time | Segment | Attendees run |
|------|---------|---------------|
| 0:00 | **Why isolation bugs escape tests.**  A check-then-act bug that a concurrent test catches 1 run in 15. | `isotest stress` |
| 0:10 | **Lab 1 (SQLite, zero install).**  The full loop in 5 seconds, then each step by hand. | `isotest lab` |
| 0:25 | **Concepts.**  Delay points, serial baseline, schedule search, delta debugging, reading a timeline. | `isotest show` |
| 0:40 | **Lab 2 (PostgreSQL).**  The same code and the same single pause, now a true isolation anomaly. | `explore`, `minimize`, `replay --control` |
| 0:60 | **Lab 3: the fix that wasn't.**  Break a textbook fix, then write one that survives. | `sweep`, `explore --expect clean` |
| 0:80 | **Ship it.**  A minimized artifact becomes a CI regression test; wrap-up. | `replay --control` in CI |

A 3-hour version adds Lab 4 (depth-2 bugs; your own workload) and the
research segment on coverage and baselines (`make eval-fast`).

## Lab 1: the full loop on SQLite (no install)

```bash
python -m isotest lab
```

It runs eight steps on `bank_overdraft` (withdraw 80 from a balance of 100,
with two concurrent workers) and prints the equivalent command for each:

1. **Serial baseline.**  Workers start a calibrated stagger apart (about 10 ms)
   and the oracle stays quiet.  "Your tests pass."
2. **Stress.**  Random arrival jitter, no targeted pauses.  On the
   reference machine the oracle fired in 2 of 30 runs: a flaky test.
3. **Explore.**  The schedule pool finds a bug-triggering schedule.
4. **Minimize.**  Delta debugging cuts it to **one** pause:
   `s0_t0_q2_exec`, i.e. worker 0 pauses before its 2nd statement (the
   `UPDATE`).  The minimizer also reports the race window (about the stagger).
5. **Explain.**  The interleaving, statement by statement:

   ```text
       time    │ w0                                           │ w1
   ────────────┼──────────────────────────────────────────────┼─────────────────────────────────
        0.1 ms │ BEGIN                                        │
        0.1 ms │ SELECT balance FROM accounts WHERE id… → 100 │
        0.3 ms │ ⏸ pause 20.1 ms [q2_exec]                    │
       10.2 ms │                                              │ BEGIN
       10.2 ms │                                              │ SELECT balance FROM … → 100
       10.3 ms │                                              │ UPDATE accounts SET … → 1 row
       10.3 ms │                                              │ INSERT INTO withdrawals… → 1 row
       10.3 ms │                                              │ COMMIT
       20.4 ms │ UPDATE accounts SET balance = balanc… ✗ BUSY │
       20.5 ms │ INSERT INTO withdrawals (account_id,… ✗ BUSY │
       20.6 ms │ COMMIT                                       │
     BUG: Phantom success: 2 worker(s) reported withdrawn but successful_withdrawals=1
   ```

   SQLite *refused* the stale write.  The code never checks `exec()`
   results, so it reports success anyway.  This is a `swallowed_error`, not
   an isolation anomaly, and the tool labels it that way.
6. **Verify the fix.**  Replay the exact schedule: buggy 10/10, fixed 0/10.
   Then attack the fix's own statements one pause at a time.
7. **Challenge.**  A pointer to Lab 3.
8. **Regression test.**  The command to pin the artifact in CI.

Then do it by hand:

```bash
python -m isotest explore  --workload bank_overdraft --db sqlite
python -m isotest minimize results/isotest/bank_overdraft_sqlite.json
python -m isotest show     results/isotest/bank_overdraft_sqlite.min.json
python -m isotest replay   results/isotest/bank_overdraft_sqlite.min.json --control
```

**Discussion prompt:** why did the bug need exactly one pause, and why
there?  (Answer: it is the only point where another transaction can slip
between this worker's read and its write.)

## Lab 2: the same code on PostgreSQL

```bash
python -m isotest explore  --workload bank_overdraft --db postgres
python -m isotest minimize results/isotest/bank_overdraft_postgres.json
python -m isotest replay   results/isotest/bank_overdraft_postgres.min.json --control
```

Same workload, same single pause point, different database: under READ
COMMITTED, worker 0's `UPDATE balance = balance - 80` applies to the newly
committed balance of 20, and the balance ends at **-60**
(`isolation_anomaly`).

The other shipped findings show the same pattern:

```bash
python -m isotest show examples/repros/job_queue_claim_postgres.json   # double claim
python -m isotest show examples/repros/doctors_on_call_postgres.json   # write skew (G2-item)
```

**Discussion prompt:** which isolation level would stop each of these, and
what would it cost?  (Try `SERIALIZABLE` and watch for SQLSTATE 40001.)

## Lab 3: the fix that wasn't

`doctors_on_call` ships a plausible fix (`naive_fix`): one "atomic"
conditional `UPDATE ... WHERE id = ? AND (SELECT COUNT(*) ...) > 1`.

```bash
python -m isotest lab --db postgres --workload doctors_on_call
```

Step 7 makes the central point of the workshop:

* replaying the *bug's* schedule against `naive_fix` comes back CLEAN, but
  the tool warns that **none of the scheduled pauses were reached**.
  Delay points name "the q-th statement", and `naive_fix` has different
  statements.  A regression replay is not a proof;
* attacking `naive_fix` *directly* (`isotest sweep --variant naive_fix`)
  breaks it with a single pause before `COMMIT`.

**Exercise:** make the starter in
[examples/exercises/doctors_fix.py](examples/exercises/doctors_fix.py) hold,
then attack your own code:

```bash
python -m isotest sweep   --workload doctors_on_call --db postgres \
    --variant examples/exercises/doctors_fix.py:doctors_fix --expect clean
python -m isotest explore --workload doctors_on_call --db postgres --workers 3 \
    --variant examples/exercises/doctors_fix.py:doctors_fix --expect clean --trials 300
```

Exit code 0 means your fix survived.  Any function with the signature
`fn(adapter, worker_id) -> {"outcome": ...}` can be attacked this way; it
runs against the workload's schema, seed data and oracle.

**Stretch (depth 2):** `fk_phantom`'s `naive_fix` survives every
single-pause attack but not exploration:

```bash
python -m isotest sweep    --workload fk_phantom --db postgres --variant naive_fix --workers 3   # 0 hits
python -m isotest show     examples/repros/fk_phantom_naive_fix_postgres.json                    # needs 2 pauses
```

## Lab 4 (3-hour version): pin it in CI

```bash
python -m isotest replay examples/repros/doctors_on_call_postgres.json --control --n 20 --min-rate 0.9
echo $?   # 0 only if the bug still reproduces AND the fix stays clean
make replay-examples
```

`--min-rate 0.9` accepts a bug that reproduces in 18 of 20 runs, because
shared CI runners have timing hiccups.  The fixed-variant control is always
strict: a single oracle hit fails the build.

The repository's own CI (`.github/workflows/ci.yml`) runs exactly this, plus
`isotest selfcheck --adversarial`, which attacks every fixed variant.

## Command reference

| Command | What it does | Needs a DB? |
|---------|--------------|-------------|
| `doctor` | Python, drivers, server reachability | probes |
| `list` | workloads, anomaly classes, variants | no |
| `lab [--pause]` | the guided 8-step run (`--pause` waits for Enter: presenter mode) | yes |
| `explore` | schedule search; saves an artifact (`--expect clean` to validate a fix) | yes |
| `minimize` | delta debugging + delay shrinking; `<artifact>.min.json` | yes |
| `show` | delay plan + recorded interleaving | **no** |
| `replay [--control\|--variant V]` | re-run N times; exit code = expectation held | yes |
| `sweep` | exhaustive depth-1: one pause at every statement | yes |
| `stress` | baseline: random arrival jitter, no targeted pauses | yes |
| `selfcheck [--adversarial]` | serial runs and fixed variants must be clean | yes |

Global flags: `--json` (machine-readable), `--ascii` (no box-drawing
characters, e.g. for Windows consoles), `--no-color`, `--no-sandbox`.  Every
command that executes has `--dry-run` or works offline (`show`).  Exit codes
are 0 (expectation met), 1 (not met), and 2 (usage or environment error).

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `doctor`: postgres server not reachable | start `pg16_2` (above) or set `POSTGRES_DSN` |
| `doctor`: missing psycopg2 | `pip install psycopg2-binary` (SQLite needs nothing) |
| Garbled boxes or arrows | add `--ascii` |
| "could not create a private schema" | grant `CREATE` on the database (facilitator section) |
| Explore finds nothing | `--trials 400`, `--workers 3`, or `isotest sweep` to see which pauses matter |
| A replay is FLAKY on a slow laptop | re-run `minimize` there; the stagger is calibrated per machine |

## Reproducing the paper evaluation (research track)

The research harness is separate from the workshop CLI and keeps the
historical `start_skew` injection mode, so published numbers stay
reproducible:

```bash
python scripts/experiments/run_final_eval.py --smoke --db sqlite          # ~seconds
docker compose -f docker-compose-multidb.yml up -d
python scripts/experiments/run_final_eval.py --quick --db postgres mysql sqlite \
    --output-dir results/schedule_pool_quick                              # 15 s/cell, 3 reps
python scripts/experiments/run_final_eval.py --paper --db postgres mysql sqlite \
    --output-dir results/schedule_pool_paper                              # 60 s/cell, 10 reps
```

Before relying on PostgreSQL `bank_overdraft` numbers produced before this
revision, read the audit: its oracle flagged 100% of PostgreSQL trials,
fixed variant included.

Secondary workflows (Go/PostgreSQL YAML runner with Elle histories, BEC
analysis, litmus utilities) remain in the repository; see
[docs/README.md](docs/README.md).
