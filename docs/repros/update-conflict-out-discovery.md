# update-conflict-out: Fuzzer Discovery Story

## The Bug: SSI Conflict-Out Anomaly

PostgreSQL's Serializable Snapshot Isolation (SSI) relies on tracking read/write
anti-dependencies ("conflict-out" edges) between concurrent transactions to detect
serialization anomalies. In PostgreSQL 12.3 and earlier, a subtle flaw in the
conflict-checking logic caused a serializability violation under specific
conditions involving three concurrent SERIALIZABLE transactions:

1. **foo** reads `txn0` (empty), inserts into `txn1`, commits.
2. **bar** inserts into `txn0` (creating an rw-antidependency with foo),
   then reads `txn1` (seeing it empty).
3. **trouble** updates the row in `txn1` that foo inserted — creating an
   additional physical tuple version — while trouble is still in-flight.

The SSI checker examines the tuple chain for `txn1[7]` and encounters trouble's
in-flight version. Due to checking the wrong transaction ID (trouble's XID instead
of foo's XID), it fails to detect the rw-antidependency cycle between foo and bar.
Bar's `COMMIT` succeeds without `SQLSTATE 40001`, even though the history admits
no serial ordering equivalent.

### Why 12.3 vs 12.4

- **PostgreSQL 12.3** (vulnerable): SSI conflict-out check fails to detect the
  anomaly when an inserted tuple is subsequently modified by another in-flight
  transaction, creating a confusing physical tuple version chain.
- **PostgreSQL 12.4** (fixed): The
  [release notes](https://www.postgresql.org/docs/release/12.4/) describe a fix
  for "failure to detect conflict-out serialization anomalies involving an insert
  that was subsequently updated by another transaction." On 12.4+, `bar_select`
  correctly raises `ERROR: could not serialize access due to read/write
  dependencies among transactions` (40001).

### References

- [Jepsen analysis (PG 12.3)](https://www.postgresql.org/message-id/db7b729d-0226-d162-a126-8a8ab2dc4443@jepsen.io)
- [PG 12.4 release notes](https://www.postgresql.org/docs/release/12.4/)
- [Canonical isolation test spec](https://github.com/postgres/postgres/blob/master/src/test/isolation/specs/update-conflict-out.spec)

---

## Why the Baseline Hides the Bug

Under natural concurrent scheduling (no injected delays), the OS/runtime scheduler
controls the interleaving of three threads. In practice, the threads execute fast
enough that the precise multi-step ordering required to trigger the bug almost
never occurs:

| Condition | Why it rarely occurs naturally |
|-----------|-------------------------------|
| `foo_select` before `bar_insert` | Both fire immediately after barrier; race is coin-flip but insufficient alone |
| `trouble_update` after `foo_commit` | trouble's UPDATE runs at ~1ms, but foo needs ~5-10ms to finish all 4 steps |
| `bar_select` after `trouble_update` | bar_select runs at ~2ms, before trouble's delayed UPDATE |
| `trouble` still in-flight during `bar_select` | trouble ABORTs immediately after UPDATE (~1ms gap) |

The result: **0 anomalies in 200+ natural trials**. SSI correctly detects the
rw-antidependency cycle and raises `40001` every time, because without trouble's
confusing in-flight tuple version, the checker works properly.

```
$ python scripts/discover_update_conflict_out.py --mode baseline --pg 12.3 --trials 200
  BASELINE RESULTS:
    Anomalies detected: 0/200 (0.0%)
    Serialization failures (40001): 200/200
    → OS scheduler hides the bug: 0 hits in 200 natural trials
```

---

## What the Fuzzer Did

### Step 1: Parsed SQL and Extracted Keys

The fuzzer analyzed the 11-event seed trace (3 sessions × ~3-4 steps each) and
extracted table/key pairs from each SQL statement:

| Event ID | Session | SQL | Table | Key |
|----------|---------|-----|-------|-----|
| `foo_select` | foo | `SELECT * FROM txn0 WHERE id = 42` | txn0 | 42 |
| `foo_insert` | foo | `INSERT INTO txn1 SELECT 7, 'foo_insert'` | txn1 | 7 |
| `bar_insert` | bar | `INSERT INTO txn0 SELECT 42, 'bar_insert'` | txn0 | 42 |
| `bar_select` | bar | `SELECT * FROM txn1 WHERE id = 7` | txn1 | 7 |
| `trouble_update` | trouble | `UPDATE txn1 SET val = '...' WHERE id = 7` | txn1 | 7 |

### Step 2: Identified Anti-Dependencies

From the extracted keys, the fuzzer built cross-session dependency edges:

```
rw(foo_select[foo] → bar_insert[bar] on txn0[42])   — foo reads, bar writes same key
rw(bar_select[bar] → foo_insert[foo] on txn1[7])    — bar reads, foo writes same key
rw(bar_select[bar] → trouble_update[trouble] on txn1[7])  — bar reads, trouble writes
ww(foo_insert[foo] → trouble_update[trouble] on txn1[7])  — both write same key
```

The first two edges form the **rw-antidependency cycle** (foo→bar→foo) that makes
the history non-serializable.

### Step 3: Generated Delay Candidates (Tiered Heuristics)

The mutation engine ranked delay candidates by tier:

| Tier | Candidate | Weight | Justification |
|------|-----------|--------|---------------|
| `tier_rw_conflict` | `bar_select` | 10 | Delay read to widen window for write `foo_insert` on txn1[7] |
| `tier_rw_conflict` | `foo_select` | 10 | Delay read to widen window for write `bar_insert` on txn0[42] |
| `tier_commit_hold` | `trouble_abort` | 9 | Hold rollback to keep modified tuples in-flight |
| `tier_commit_hold` | `foo_commit` | 9 | Hold commit of foo to keep tuples in-flight |
| `tier_commit_hold` | `bar_commit` | 9 | Hold commit of bar |
| `tier_rw_conflict_writer` | `trouble_update` | 6 | Delay write to let read establish snapshot |
| `tier_rw_conflict_writer` | `bar_insert` | 6 | Delay write to let read establish snapshot |
| ... | ... | ... | ... |

The **`tier_commit_hold`** heuristic was critical: it targets COMMIT/ABORT events
that follow writes, recognizing that delaying transaction boundaries increases
overlap windows and exposes SSI/lock-manager edge cases.

### Step 4: Discovered the Bug via Weighted Lottery

Using K=3 delay points per mutation and a weighted lottery over the candidates,
the fuzzer explored delay combinations. It found the bug in **mutation #9** 
(25 total trials, ~5 seconds):

**Discovered delays:**
| Event | Delay | Tier | Justification |
|-------|-------|------|---------------|
| `trouble_update` | 57ms | `tier_rw_conflict_writer` | Delay write to let bar establish snapshot first |
| `bar_select` | 118ms | `tier_rw_conflict` | Delay read to widen window for write on txn1[7] |
| `trouble_abort` | 160ms | `tier_commit_hold` | Hold rollback to keep modified tuples in-flight |

**Engineered timeline:**
```
         trouble_begin:    +0.0ms  ──┐
             foo_begin:    +0.0ms  ──┤ All three start together
             bar_begin:    +0.0ms  ──┘
            foo_select:    +0.2ms  → reads txn0, empty (snapshot established)
            bar_insert:    +0.2ms  → inserts txn0[42] (snapshot established)
            foo_insert:    +0.8ms  → inserts txn1[7]
            foo_commit:    +1.0ms  → foo commits
        trouble_update:   +57.2ms  → snapshot sees foo's commit, UPDATE 1 row ← KEY
            bar_select:  +118.8ms  → bar's snapshot: txn1 empty → 0 rows ← BUG WINDOW
            bar_commit:  +119.3ms  → SSI misses cycle (confusing tuple chain) ← BUG!
         trouble_abort:  +218.1ms  → trouble still in-flight during bar_select ← KEY
```

**Why K=3 is necessary (not K=2):**
The bug requires three simultaneous timing conditions:
1. `trouble_update` must run **after** `foo_commit` (so trouble actually modifies
   the row, creating the confusing physical tuple version)
2. `bar_select` must run **after** `trouble_update` (so the confusing version exists
   in the tuple chain when the SSI checker runs)
3. `trouble_abort` must run **after** `bar_select` (so trouble is still in-flight,
   with its in-progress XID in the tuple chain, confusing the SSI checker)

Without the `trouble_abort` delay (K=2), trouble rolls back immediately after its
UPDATE (~1ms), and the SSI checker examines a "clean" tuple chain with only
committed/aborted versions — which it handles correctly even on 12.3.

---

## Cross-Version Validation Results

### PostgreSQL 12.3 (Vulnerable)

| Mode | Result |
|------|--------|
| **Baseline** (no delays, 200 trials) | 0/200 anomalies detected |
| **Fuzzer-discovered schedule** (20 replays) | **20/20 bugs** (100% reproducible) |
| Serialization failures (40001) | 0/20 |

### PostgreSQL 12.4 (Fixed)

| Mode | Result |
|------|--------|
| **Baseline** (no delays, 200 trials) | 0/200 anomalies |
| **Same fuzzer-discovered schedule** (20 replays) | **0/20 bugs** (fix works) |
| Serialization failures (40001) | **20/20** (SSI correctly detects cycle) |

---

## Reproduction Steps

### Prerequisites

- Docker and Docker Compose
- Python 3.9+ with `psycopg2-binary` (`pip install psycopg2-binary`)

### Quick Start

```bash
# Start vulnerable + fixed PostgreSQL containers
docker compose -f docker-compose-repro.yml up -d

# Run the full demo (baseline + discovery + validation)
python scripts/discover_update_conflict_out.py --mode full

# Or run individual steps:

# 1. Baseline (OS hides the bug)
python scripts/discover_update_conflict_out.py --mode baseline --pg 12.3 --trials 200

# 2. Discovery campaign (fuzzer finds the bug)
python scripts/discover_update_conflict_out.py --mode discover --pg 12.3 --k 3 --budget 500

# 3. Cross-version validation
python scripts/discover_update_conflict_out.py --mode validate --replay-trials 20
```

### Via run_campaign.py Integration

```bash
# Baseline
python scripts/run_campaign.py --litmus update-conflict-out --pg 12.3 --no-delays --trials 200

# Discovery
python scripts/run_campaign.py --litmus update-conflict-out --pg 12.3 --k-bounded 3 --budget 500

# Full demo
python scripts/run_campaign.py --litmus update-conflict-out --pg 12.3 --mode full
```

### Shell Script (One Command)

```bash
./scripts/discover_update_conflict_out.sh
```

### Expected Output

```
  baseline hidden:     0/100 anomalies in natural trials
  fuzzer discovered:   YES (in ~25-80 trials)
  12.3 vulnerable:     20/20 reproductions
  12.4 fixed:          0/20 bugs, 20/20 got 40001
```

---

## Artifact Paths

| File | Description |
|------|-------------|
| `scripts/discover_update_conflict_out.py` | Discovery engine (baseline, campaign, validation) |
| `scripts/discover_update_conflict_out.sh` | One-command demo script |
| `tests/litmus/update_conflict_out_update.json` | Seed trace (UPDATE variant) |
| `tests/litmus/update_conflict_out_delete.json` | Seed trace (DELETE variant) |
| `results/update-conflict-out/found.json` | Discovered artifact (delays + execution log) |
| `results/update-conflict-out/report.json` | Full demo report |
| `docker-compose-repro.yml` | Docker Compose for 12.3/12.4 side-by-side |
| `scripts/repro_update_conflict_out.py` | Deterministic reproducer (canonical schedule) |

---

## How the Fuzzer Engine Works

### Architecture

```
Seed Trace (natural, no delays)
    ↓
SQL Parser → Extract (table, key) per event
    ↓
Anti-Dependency Analyzer → rw/wr/ww edges between sessions
    ↓
Delay Candidate Generator → Tiered, weighted candidates
    ↓
Weighted Lottery → Select K delay points
    ↓
Concurrent Runner → 3 threads, barrier-synchronized, per-client FIFO
    ↓
Oracle → check_bug_found() — rw-cycle present + no 40001?
    ↓
Repeat until budget exhausted or bug found
```

### Heuristic Tiers

| Tier | Weight | Description |
|------|--------|-------------|
| `tier_rw_conflict` | 10 | Delay reads involved in rw-antidependencies |
| `tier_commit_hold` | 9 | **NEW**: Delay COMMIT/ABORT to keep tuples in-flight |
| `tier_wr_conflict` | 7 | Delay reads after writes on same key |
| `tier_rw_conflict_writer` | 6 | Delay writes to let reads establish snapshots |
| `tier_ww_conflict` | 5 | Delay conflicting writes |
| `tier_fallback` | 1 | Any SQL event (low priority) |

The `tier_commit_hold` heuristic targets the specific class of SSI bugs where
transaction boundary timing matters. By keeping COMMIT or ABORT events delayed,
modified tuple versions stay "in-flight" longer, exposing edge cases in SSI
conflict checking that depend on the physical tuple chain state.

### Bug-Found Predicate

```python
bug_found = (
    foo_select returned 0 rows  AND  # foo didn't see bar's insert
    bar_select returned 0 rows  AND  # bar didn't see foo's insert  
    foo committed successfully  AND  # rw-dep: foo→bar confirmed
    bar committed successfully  AND  # rw-dep: bar→foo confirmed
    no SQLSTATE 40001 raised         # SSI didn't catch the cycle
)
```

This predicate detects the **non-serializable history** without depending on a
known schedule — it verifies the actual rw-antidependency cycle condition.
