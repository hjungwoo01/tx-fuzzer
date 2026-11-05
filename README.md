# IsoTest

Find, explain, and pin down transaction-isolation bugs by controlling *when*
each SQL statement runs, entirely from the client.

## Quick Start: workshop / hands-on (no Docker, no pip install)

```bash
python3 -m isotest doctor    # Python 3.9+ is all SQLite needs
python3 -m isotest lab       # 8 guided steps in ~3 s: serial → stress → explore →
                             # minimize → explain → verify fix → challenge → CI
```

```text
    time    │ w0                                           │ w1
────────────┼──────────────────────────────────────────────┼─────────────────────────────────
     0.1 ms │ SELECT balance FROM accounts WHERE id… → 100 │
     0.3 ms │ ⏸ pause 20.1 ms [q2_exec]                    │
    10.2 ms │                                              │ SELECT balance FROM … → 100
    10.3 ms │                                              │ UPDATE accounts SET … → 1 row
    10.3 ms │                                              │ COMMIT
    20.4 ms │ UPDATE accounts SET balance = balanc… ✗ BUSY │
    20.6 ms │ COMMIT                                       │
  BUG: Phantom success: 2 worker(s) reported withdrawn but successful_withdrawals=1
```

With PostgreSQL (`pip install psycopg2-binary`; any server via `POSTGRES_DSN`,
or `docker compose -f docker-compose-multidb.yml up -d pg16_2`), the same code
and the same single pause drive the balance to −60:

```bash
python3 -m isotest lab --db postgres --workload doctors_on_call
python3 -m isotest replay examples/repros/bank_overdraft_postgres.json --control
```

* Facilitator guide and labs: [WORKSHOP.md](WORKSHOP.md)
* Workshop submission package: [docs/workshop/SUBMISSION.md](docs/workshop/SUBMISSION.md)
* Audit and upgrade record (what was wrong, how it was verified): [docs/workshop/AUDIT.md](docs/workshop/AUDIT.md)

> **Note for users of earlier revisions.**  The PostgreSQL `bank_overdraft`
> oracle used to flag 100% of trials, fixed variant included.  Two
> "fixed" variants (`doctors_on_call`, `fk_phantom`) were not actually
> fixed.  The executor ignored commit delay points and collapsed exec delay
> points into one pre-transaction sleep.  `make test-py` could not fail.
> All of this is fixed and covered by tests; see the audit.  The research
> harness keeps the historical delay semantics (`start_skew`) so published
> numbers remain reproducible.

## Quick Start: paper artifact (Docker)

**Docker is the only requirement.**  The image bundles Python, Java, Clojure,
Elle, SQLite, and all dependencies — no local toolchain setup needed.

```bash
# 1. Build the container (~3 min first time; cached afterwards)
make build

# 2. Run the end-to-end demo: explore → artifact → replay → Elle verify (~30s)
make demo

# 3. Run the scaled-down evaluation (~2 min)
make eval-fast
```

All output artifacts (CSVs, JSON artifacts, Elle reports) are written to the
`results/` directory on your host machine via a Docker volume mount.

| Command | What it does | Time |
|---------|-------------|------|
| `make build` | Build the `isotest-artifact` Docker image | ~3 min |
| `make demo` | Explore a workload, save artifact, replay, Elle verify | ~30s |
| `make eval-fast` | Sensitivity + coverage benchmarks (scaled-down) | ~2 min |
| `make eval-full` | Full paper-quality evaluation | ~60 min |
| `make shell` | Interactive bash inside the container | — |

For the full paper evaluation (Table 2, Figures 3–5):

```bash
make eval-full    # ~60 min; results in results/sensitivity/ and results/longitudinal/
```

To explore interactively:

```bash
make shell
# Inside the container:
python scripts/evaluate_sensitivity.py --help
python scripts/evaluate_coverage_longitudinal.py --help
python scripts/verify_elle.py --help
```

---

**IsoTest** is a client-side schedule exploration and replay tool for database
isolation testing.  It discovers concurrency bugs by systematically exploring
transaction interleavings through controlled delay injection, then packages
each interesting interleaving as a portable **ScheduleArtifact** that can be
replayed deterministically against a fresh database.

The tool's design is intentionally schedule-only: it never mutates SQL text or
invents schemas.  Instead, it searches the timing and ordering space by
injecting client-side delays at stable **Event** identifiers, hashes the
canonical end-state after each execution, and retains artifacts that expose new
states or isolation violations.

## Key capabilities

- **Statement-level schedule control** — `step` injection pauses a session
  immediately before its BEGIN, its *n*-th statement, or its COMMIT, without
  touching the SQL.  The historical `start_skew` mode (per-session start
  offsets only) is kept for reproducing published results.
- **Minimization and explanation** — delta debugging shrinks a failing
  schedule to its causal pause(s); a statement-level timeline shows the
  interleaving, results, errors and lock waits.
- **Categorized verdicts** — isolation anomalies are reported separately from
  application code that ignored a rejected write (`swallowed_error`).
- **Self-checking harness** — `isotest selfcheck --adversarial` requires
  serial runs and every fixed variant (under every single-pause attack) to
  be clean.
- **High replay-success rates** — retained artifacts include measured replay
  rates; the tool verifies that replaying a `ScheduleArtifact` reproduces the
  exact same `FinalStateHash` as the original exploration run.
- **Delay-sensitivity analysis** — each artifact records which `DelayPoint`s
  were necessary to trigger the anomaly, enabling minimal reproduction cases.
- **Pluggable Oracle interface** — users define their own isolation checkers
  by implementing the `Oracle` ABC.  Built-in oracles cover double-claim,
  overdraft, and lost-append patterns.
- **Elle/Jepsen integration** — every exploration and replay run can
  automatically produce an Elle-compatible EDN history file via the built-in
  `ElleLogger`.
- **Multi-DB support** — works against PostgreSQL, MySQL, and SQLite through
  a normalized `DBAdapter` layer.

## Local Development Setup

### 1. Install dependencies

```bash
python -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

### 2. Run the smoke test (SQLite, no Docker)

```bash
make schedule-pool-smoke
```

### 3. Explore, save, and replay in one command

```bash
make demo-explore-replay
```

This runs a single exploration session on SQLite, saves any bug-triggering
`ScheduleArtifact` to `examples/repros/`, and immediately replays it to verify
the `FinalStateHash` matches.

### 4. Replay a pre-saved artifact

```bash
python -m isotest replay examples/repros/bank_overdraft_sqlite.json --control
# historical path, same artifact:
python scripts/run_demo.py replay examples/repros/bank_overdraft_sqlite.json
```

### 5. Multi-DB evaluation

```bash
docker compose -f docker-compose-multidb.yml up -d
python scripts/experiments/run_final_eval.py \
  --quick --db postgres mysql sqlite \
  --output-dir results/schedule_pool_quick
```

## Core abstractions

| Type | Purpose |
|------|---------|
| `Event` | A trackable database operation (`s{session}_t{txn}_q{query}_{phase}`) |
| `DelayPoint` | A client-side pause injected at an Event |
| `Schedule` | Ordered list of DelayPoints (canonical form) |
| `ScheduleArtifact` | Serializable record: schedule + outcome + metadata |
| `FinalStateHash` | Deterministic SHA-256 hash of canonical end-state |
| `Oracle` | Pluggable interface for isolation violation checking |
| `OracleVerdict` | Structured result from an Oracle check |
| `ReplayVerification` | Aggregate replay determinism summary |

## Architecture

IsoTest has two active layers:

**Primary: schedule exploration (Python)** — `scripts/schedule_pool/` implements
the core types, mutation operators, retention policy, and pool-based search.
`scripts/experiments/run_final_eval.py` orchestrates multi-DB evaluation.  The
pool-based exploration strategy (mutate → execute → retain novel → replay) is
one of several possible exploration strategies; the tool is modular so that
users can plug in their own.

**Secondary: Go/PostgreSQL runner** — `cmd/runner/` and `internal/` implement a
lower-level execution engine for YAML workloads with Elle-compatible EDN
history output, replay schedules, and hybrid replay + background load.

## Repository map

- `isotest/` — the workshop CLI (`python -m isotest`)
- `scripts/schedule_pool/` — core types, schedule model, event IDs, mutation,
  retention, pool, replay verification, Elle logger, Oracle interface;
  `injection.py` (statement-level delays + trace), `execution.py` (the one
  trial executor), `minimize.py` (ddmin + shrinking), `timeline.py`
- `scripts/experiments/` — multi-DB adapters, workloads, case studies,
  evaluation harness
- `scripts/evaluate_sensitivity.py` — delay-sensitivity benchmarking
- `scripts/evaluate_coverage_longitudinal.py` — longitudinal state-coverage
  analyzer
- `scripts/verify_elle.py`, `scripts/verify_elle.sh` — Elle/AWDIT verification
  pipeline wrappers
- `cmd/runner/`, `internal/` — Go execution engine for YAML workloads
- `workloads/` — YAML workload configurations
- `examples/repros/` — tool-generated, minimized ScheduleArtifacts (CI replays them);
  `examples/exercises/` — workshop exercise starters
- `docs/` — architecture and evaluation documentation
- `Dockerfile` — self-contained reviewer environment (Python + JDK + Clojure + Elle)

## Validation

```bash
make verify        # Full: go vet + shell check + Python compile + all tests
make test          # Go + Python tests
make test-py       # Python tests only
make selfcheck     # serial runs + fixed variants under single-pause attack (SQLite)
make replay-examples   # every shipped artifact still reproduces; fixes stay clean
```

## Extending IsoTest

**Add a new Oracle:**
```python
from scripts.schedule_pool.oracle import Oracle
from scripts.schedule_pool.types import OracleVerdict

class MyOracle(Oracle):
    @property
    def name(self) -> str:
        return "my_check"

    def check(self, outcomes, final_state) -> OracleVerdict:
        # your isolation check logic
        return OracleVerdict(bug_found=False, reason="OK")
```

**Add a new exploration strategy** by implementing the `BaseStrategy` ABC and
passing it to `run_pool_search(base_strategy=...)`.

## Documentation

- [WORKSHOP.md](WORKSHOP.md) — hands-on labs and facilitator guide
- [docs/workshop/SUBMISSION.md](docs/workshop/SUBMISSION.md) — workshop
  abstract, outcomes, audience
- [docs/workshop/AUDIT.md](docs/workshop/AUDIT.md) — audit findings, fixes,
  and how each was verified
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — component map and extension
  points
- [docs/multidb_evaluation.md](docs/multidb_evaluation.md) — evaluation
  workflow reference
- [docs/heuristic_scheduler.md](docs/heuristic_scheduler.md) — formal
  description of the Heuristic scheduler algorithm
