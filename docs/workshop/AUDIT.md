# Audit and Upgrade Record (Stages 1–2)

Scope: the `tx-fuzzer` / IsoTest repository at commit `aa4f05c`, audited on
2026-11-04 against SQLite 3.53 and PostgreSQL 16.15.  No MySQL server was
available, so MySQL paths are **unverified** here.  Every finding below was
reproduced by running the code; the commands are in
[Reproducing the evidence](#reproducing-the-evidence).

---

## Stage 1: Architectural and functional audit

### 1.1 Value proposition and persona

**The bottleneck.**  Isolation bugs in application transactions (lost
updates, double claims, overdrafts, write skew, phantoms) depend on timing.
Unit tests run serially and never see them.  Concurrent integration tests
see them rarely: on the reference machine a textbook check-then-act bug
failed **2 of 30** runs under realistic arrival jitter.  When such a run
fails, the failure cannot be reproduced or explained, and a proposed fix
cannot be shown to hold.  History checkers such as Elle (Kingsbury & Alvaro,
VLDB 2020) judge *recorded* histories.  They neither steer the interleaving
nor shrink it, and they are aimed at database engines more than at the
application code that misuses them.

**What the tool does.**  It controls *when* each SQL statement of each
session runs, entirely client-side: no engine patches, no SQL rewriting.
It searches the space of pause schedules for one that breaks an invariant
and stores it as a replayable artifact.

**Personas.**
1. *Backend engineers* writing transactional code against
   PostgreSQL/MySQL/SQLite.  This is the workshop's primary audience: "is my
   check-then-act safe, and does my fix actually hold?"
2. *SRE/QA* who must turn a once-a-week incident into a deterministic
   regression test.
3. *Database and systems researchers* comparing exploration strategies
   (the repository's research harness).
4. *Educators* who want to demonstrate Adya/Berenson anomalies live.

### 1.2 System mechanics

```text
 workload: setup SQL · txn variants (buggy / fixed / naive_fix / user file) · canonical reader · oracle
     │
     ▼
 schedule pool ── sample + mutate (add/adjust/remove/swap/jitter, capped) ──► candidate Schedule
     ▲                                                     [{event: s{w}_t{t}_q{q}_{phase}, delay_ms}]
     │ retain: new end state · new bug · more reliable · smaller      │
     │                                                                ▼
     │     execute_trial: reset DB → N threads behind a barrier → each through an
     │     InstrumentedAdapter (pauses at named events, records a statement trace)
     │                                                                │
     └──── FinalStateHash + categorized OracleVerdict ◄── StrictAdapter canonical read
                                                                      │
                                                                      ▼
            ScheduleArtifact JSON ──► minimize (ddmin + shrink) · replay --control · show · CI
```

| | |
|---|---|
| **Inputs** | workload id, database + DSN, worker count, variant, budget (trials/seconds), seed |
| **Core transforms** | stable event ids; weighted pool sampling (novelty/bug/replay/compactness); mutation with per-event/total/count caps; retention policy; canonical-state hashing |
| **External dependencies** | SQLite (stdlib); `psycopg2`, `pymysql` (lazy imports); optional Elle (JVM + Clojure), Go runner (`pgx`), Docker Compose |
| **Outputs** | ScheduleArtifact JSON, pool JSONL, coverage CSV/plots, Elle EDN histories |
| **Size** | ~45 k LOC Python under `scripts/` (dozens of one-off experiment scripts), ~2 k LOC Go, a Clojure runner, 13 workloads, 3 Compose files |

### 1.3 Critical failure modes (all reproduced)

Severity: **P0** = results are wrong; **P1** = reproducibility or security;
**P2** = developer experience, performance, operations.

| ID | Sev | Finding | Evidence before | Status |
|----|-----|---------|-----------------|--------|
| F1 | P0 | **False-green test suite.**  `tests/test_schedule_pool.py`'s `check()` printed and counted but never raised, so under `pytest` (`make test-py`) every test passed unconditionally. | A deliberately false check: `21 passed`.  Script mode revealed a real failure that had been hidden since `aa4f05c` (`OverdraftOracle` test inputs went stale when the oracle gained a cross-check). | **Fixed**: same injection now `1 failed`; stale test corrected. |
| F2 | P0 | **Delay points were not honored.**  The executor ignored every `commit` delay point and collapsed all `exec` delays of a worker into one `max()` sleep *before BEGIN*.  The schedule abstraction (`s{w}_t{t}_q{q}_{phase}`) promised per-statement control that was never delivered.  Only per-worker start skew was controlled, and start skew cannot express "pause between my read and my write". | 4×300 ms commit delays: trial 12.6 ms (baseline 15.5).  2×150 ms exec delays: 164 ms, not ~315. | **Fixed** (`step` mode): 315 ms / 311 ms.  The historical semantics remain as `start_skew` for paper reproducibility. |
| F3 | P0 | **Oracle false positives invalidated the "fixed variant is clean" control.**  A control sweep (fixed variant, no injected delays) flagged **6 of 26** workload×DB pairs: the 4 false positives below and the 2 broken fixes in F3b. | PG `bank_overdraft` 10/10 on *both* variants (`WHERE success = 1` on a BOOLEAN column errors on PostgreSQL; the error was swallowed and the count defaulted to 0).  `unique_violation` 5/5 on SQLite and PG (the "fix" never creates the UNIQUE constraint its docstring promises).  `long_fork` 3/5 on SQLite (the oracle flags *comparable* views such as `[(0,0),(1,0)]`, so it is unsound). | **Fixed**: 0/26.  Canonical reads now fail loudly (`HarnessError`). |
| F3b | P0 | **Two "fixes" did not fix anything.**  Both were *true* positives that the control had been reporting all along. | PG `doctors_on_call`: a conditional UPDATE whose subquery reads rows it never locks lets write skew through (5/5).  PG `fk_phantom`: `DELETE … NOT EXISTS` cannot see an in-flight child, and the deleter takes no lock (4/5). | **Fixed** with row locks.  Each old fix is kept as a runnable `naive_fix` teaching variant: exploration breaks it in 48% / 42% of 400 schedules, and the new fixes in 0/400. |
| F4 | P0 | **The flagship SQLite demo reported the wrong kind of bug.**  `bank_buggy` ignores `exec()` results.  SQLite rejects the stale write (`BUSY: database is locked`), the read-only COMMIT succeeds, the worker reports "withdrawn", and the oracle printed "Lost-write".  The database kept `balance=20`, which is correct. | Statement trace: `UPDATE → BUSY`, `INSERT → BUSY`, outcome `withdrawn`. | **Fixed**: verdicts now carry a category (`isolation_anomaly` / `swallowed_error` / `outcome_mismatch`); this one reads "Phantom success … [swallowed_error]". |
| F5 | P0 | **The shipped "known anomaly" artifacts did not reproduce.**  They were hand-written (null hashes, round delays), and the README's replay target did not exist. | PG double-claim 0/20.  PG overdraft "100%" only through F3.  Every replay printed "LOW: 0% match" because the expected hash was `None`, even with one distinct end state.  `examples/repros/bank_overdraft_sqlite.json`: not found. | **Fixed**: five tool-generated, minimized artifacts, each 30/30 reproduced with fixed 0/30 (20/20 and 0/20 with all CPU cores saturated).  The old files moved to `legacy/`. |
| F6 | P1 | Artifacts did not record worker count, injection mode, stagger, or DB version, and replay silently assumed 4 workers. | Format had no such fields. | **Fixed**: format v2.  v1 files load with legacy semantics and a warning. |
| F7 | P1 | **The Elle path was silently corrupt.**  EDN strings were not escaped.  The demo logged per-transaction result maps rather than read/write micro-ops, so Elle's checkers return `unknown`, and `make demo` masked that with `\|\| true`. | `:error "relation "accounts" does not exist"` parsed as `:error "relation "` plus a bogus key `accounts`. | **Escaping fixed** and verified by exact round-trip.  **Micro-op logging not fixed** (documented, F17). |
| F8 | P1 | Security hygiene. | `tempfile.mktemp` (CWE-377 race) in the demo.  Isolation level f-string-interpolated into `SET TRANSACTION` (an injection surface once levels come from flags or config).  Passwords printed in DSNs. | **Fixed**: private `mkdtemp` dirs, whitelist, DSN redaction.  Hard-coded test credentials (`postgres:postgres`) remain for local containers (documented). |
| F9 | P1 | **Concurrency hygiene in the harness.**  Three copy-pasted trial executors had drifted.  Each joined every thread for up to 30 s *sequentially*, then read the end state and closed connections while a worker could still be running.  The outcome list was built as `[{}] * n` (aliased dicts). | Code inspection plus timeout test. | **Fixed**: one `execute_trial`, a single deadline, cross-thread interrupt (`cancel()` / `interrupt()`), timed-out workers labeled. |
| F10 | P1 | **No run isolation.**  Every workload uses fixed table names in the default schema, so concurrent runs clobber each other: two terminals, CI next to a laptop, or a workshop room sharing one server. | A replay dropped to 19/20 while another run used the same database. | **Fixed**: each CLI session gets a private schema (PG `search_path`) or database (MySQL), dropped on exit.  Isolation is tested. |
| F11 | P2 | **DX friction.**  Three entry points (`make demo`, `run_demo.py`, `run_final_eval.py`), two product names, and default DSN ports (5432/3307) that do not match the Compose file (54326/33080). | Docs and code. | **Fixed**: one CLI (`python -m isotest`) that probes both port sets.  `doctor` reports what is missing and how to fix it. |
| F12 | P2 | Demo overwrote one artifact file per bug while reporting N saved. | "Saved 7 artifact(s)": 1 file. | **Fixed**: one file per finding plus a stable best-of pointer. |
| F13 | P2 | **Repository weight.**  ~95 MB of EDN experiment logs in the working tree, 59 MB packed history, committed Clojure build caches (`clj/.cpcache`, `clj/classes`). | Shallow clone ≈ 13.6 MB; ≈ 4.9 MB without root logs. | **Recommendation** (history rewrite is the owner's call): move logs to release assets or LFS. |
| F14 | P2 | **Connection churn instead of API rate limits** (the tool calls no external APIs).  Every trial opens N+2 connections, which hits connection limits on managed databases.  PgBouncer transaction pooling would also break `SET TRANSACTION` semantics. | Code inspection. | **Recommendation**: reuse worker connections across trials; detect poolers in `doctor`. |
| F15 | P2 | **Performance.**  Trial cost is dominated by injected pauses (by design) and connection setup. | SQLite: full 8-step lab in 2.8 s.  PG: lab 6.6 s; 200-trial 3-worker exploration 19 s (~10 trials/s). | Pauses scale with a calibrated stagger, so trials stay short. |
| F16 | P2 | Hidden coupling: `OverdraftOracle` hard-coded the workload's 100/80; `evaluate_scalability.py` mutates global oracle instances. | Code inspection. | Oracle parametrized; global mutation left (research harness). |
| F17 | P2 | Python-side Elle logging records results, not micro-ops, so Elle cannot check it. | F7. | **Open** (see recommendations). |
| F18 | P2 | **Delay points are positional** ("the q-th statement of a transaction").  A schedule replayed against code with *different* statements may never fire.  Found during this work. | A bug schedule replayed against `naive_fix`: CLEAN, with 0 of 34 ms of pauses reached. | **Mitigated**: `replay` warns when pauses are unreached; sweeps calibrate per variant; the workshop teaches it.  Statement-fingerprint event ids are recommended. |
| F19 | P2 | `SQLiteExpAdapter.begin()` swallows exceptions (BUSY on `BEGIN IMMEDIATE` goes unreported). | Code inspection. | Open (research behavior unchanged). |
| F20 | P1 | MySQL paths were not exercised: no server was available for this audit.  The PostgreSQL defects (F3) suggest the same control sweep should be run there. | n/a | **Open**: `isotest selfcheck --db mysql --adversarial` is the first step. |
| F21 | P2 | `run_final_eval.py` writes its generated results into *tracked* files (`docs/notes/schedule_pool_final_{methodology,results}.md`), so even `--smoke` overwrites the documented results with toy numbers. | Observed while re-running the smoke target for this audit (reverted). | **Recommendation**: write generated notes to `--output-dir` only, or behind an explicit `--update-docs` flag. |

---

## Stage 2: Upgrades

### 2.1 Core hardening

| Change | Where | Verified by |
|--------|-------|-------------|
| `check()` raises, so pytest sees failures; the stale oracle test is fixed | `tests/test_schedule_pool.py` | injected false check fails; script mode exits 0 |
| Statement-level injection proxy + trace; `step` / `start_skew` modes | `scripts/schedule_pool/injection.py` | probes above; `test_step_mode_*` |
| One shared executor: single deadline, interrupts, strict canonical reads, variant-aware setup | `scripts/schedule_pool/execution.py` (runner/replay delegate) | `test_executor_labels_timed_out_workers…` |
| Categorized verdicts (`BugCategory`); sound long-fork oracle; parametrized overdraft oracle | `types.py`, `oracle.py` | `test_oracle_*`, `test_long_fork_*` |
| PG BOOLEAN fix; UNIQUE-constraint fixed setup; locking fixes for doctors/fk; old fixes kept as `naive_fix` | `micro_workloads.py`, `extended_workloads.py` | control sweep 0/26; adversarial selfcheck 0 hits on fixed variants; 400-trial explorations |
| Artifact format v2 (workers, mode, stagger, category, DB version, evidence trace) | `schedule.py` | `test_artifact_*`, legacy loading |
| Honest replay verdicts (`REPRODUCED/FLAKY/NOT_REPRODUCED/CLEAN/BROKEN`); CI tolerance `--min-rate` applies to bug reproduction only, and the clean control stays strict | `replay.py`, CLI | `test_assess_replay_statuses`, `test_replay_min_rate_*` |
| EDN escaping; isolation whitelist; `mkdtemp`; DSN redaction | `elle_logger.py`, `db_adapter.py`, `run_demo.py`, CLI | round-trip test; whitelist test |
| Schedule minimization: ddmin + delay shrinking with a safety margin | `scripts/schedule_pool/minimize.py` | property test (1-minimality on random predicates) |
| Per-session database sandboxes | `isotest/cli.py` | `test_pg_sandbox_isolates_concurrent_sessions` |
| CI: stdlib-only lab, pytest, adversarial selfcheck (SQLite + PG 16), example replays, Go | `.github/workflows/ci.yml` | run locally step by step |

### 2.2 Workshop adaptations

* **One CLI, ten verbs:** `doctor · list · lab · explore · minimize · show · replay · sweep · stress · selfcheck`.
* **Zero-install first hour:** the SQLite path imports only the standard
  library.  Verified in a fresh virtualenv with no packages: the full lab
  completes in about 3 s.
* **Instant visual feedback:** a side-by-side interleaving timeline with
  pauses, results, errors and lock waits ("`DELETE … (waited 3.8 ms)`").
  `show` renders it from the artifact with no database.
* **Explanations rather than reproducers:** minimization reports how many
  pauses the bug needs (its depth) and the race window (the smallest
  passing pause).
* **Dry runs everywhere:** `explore/replay/minimize/lab --dry-run` need no
  database; `--json` is available for scripting; exit codes are meaningful
  in CI.
* **Presenter mode:** `lab --pause`.  **Robust terminals:** `--ascii`,
  `--no-color`, `NO_COLOR`.
* **Bring your own fix:** `--variant path/to/file.py:function` attacks
  attendee code against a bundled workload's schema and oracle.
  `explore --expect clean` and `sweep --expect clean` exit 0 when the fix
  holds.
* **Self-validating harness:** `selfcheck --adversarial` (serial runs clean,
  racing fixed variants clean, every single-pause attack on every fixed
  variant clean).
* **Shared-server ready:** sandboxed schemas, with a facilitator setup in
  `WORKSHOP.md`.

### 2.3 Headline measurements (reference machine: 4 vCPU, SQLite 3.53, PostgreSQL 16.15)

| Measurement | Result |
|-------------|--------|
| One 30 ms pause before worker 0's 2nd statement, PG, 2 workers, 20 runs each: `start_skew` vs `step` | overdraft 0/20 vs **20/20**; double claim 0/20 vs **20/20**; lost append 0/20 vs **20/20**; write skew 0/20 vs **20/20**; fixed variants 0/20 |
| Concurrent "stress" test with random arrival jitter | 2/30 (SQLite), 2/30 (PG, write skew) |
| Exploration → minimization | 2→1 pause (overdraft, double claim, write skew, phantom success); 3→2 (fk orphan, depth 2) |
| Depth-1 sweep vs depth-2 bug | the sweep finds 0 of 15 single pauses for `fk_phantom` `naive_fix`; exploration finds it in 3 trials |
| Fix validation (PG, 3 workers, 400 explored schedules) | `naive_fix` broken in 192 (doctors) and 168 (fk); fixed 0 and 0 |
| Shipped artifacts | 30/30 reproduced, fixed 0/30; under full CPU saturation 20/20 and 0/20.  One miss was observed in ~400 job-queue replays, which is why CI uses `--min-rate 0.9` |
| Test suite | 33 → 69 tests; pytest catches injected failures |

### 2.4 Recommendations not implemented here

1. **Statement-fingerprint event ids** (e.g. `s0_t0_UPDATE:accounts#1`) so
   schedules survive refactors and transfer across variants (F18).
2. **Connection reuse** across trials, and pooler detection in `doctor` (F14).
3. **Elle micro-op logging** from the trace, so `--elle` histories are
   checkable (F17).
4. **MySQL validation**: run `selfcheck --db mysql --adversarial` and
   regenerate MySQL examples (F20; no server available for this audit).
5. **Slim the repository**: move `*.edn` logs out of the tree; untrack
   `clj/.cpcache` and `clj/classes` (F13).
6. **Packaging**: a `pyproject.toml` with an `isotest` console script, so
   `pipx run isotest` works outside the repository.

---

## Reproducing the evidence

```bash
# F1: inject `check("x", 1 == 2)` into a copy of tests/test_schedule_pool.py and run pytest on it
python -m isotest selfcheck --db sqlite --db postgres --adversarial    # F3/F3b: 0 problems
python -m isotest sweep --workload doctors_on_call --db postgres --variant naive_fix --workers 3
python -m isotest replay examples/repros/legacy/job_queue_double_claim_postgres.json   # F5: 0/N
python -m isotest replay examples/repros/doctors_on_call_postgres.json --variant naive_fix   # F18 warning
python -m pytest tests -q
```
