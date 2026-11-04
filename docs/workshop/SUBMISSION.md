# Workshop Submission

## Title

**Breaking Transactions on Purpose: Deterministic Reproduction, Minimization, and Fix Verification for Isolation Bugs**

*A hands-on workshop with `isotest`*

## Abstract

<!-- abstract:start -->
Isolation bugs such as lost updates, double claims, and write skew hide in
ordinary application transactions and surface only under rare interleavings.
In our measurements, a conventional concurrent test caught a textbook
check-then-act bug in 2 of 30 runs, and the failing run could not be replayed.
This hands-on workshop teaches participants to hunt such bugs
deterministically with isotest, an open-source tool that controls when each
SQL statement runs, entirely from the client: no database patches and no SQL
rewriting. Participants search the space of pause schedules, use delta
debugging to shrink a failing schedule to its causal pause, read the
statement-level interleaving it produces, and pin it as a replayable artifact
that fails CI if the bug returns or the fix regresses. One bank-withdrawal
transaction shows the contrast: SQLite rejects the stale write and the
application silently reports success, while PostgreSQL READ COMMITTED lets the
balance go negative. The capstone breaks a textbook "atomic UPDATE" fix for
write skew, explains why replaying the bug's schedule could not have caught
it, and has participants write a fix that survives systematic and randomized
attack. The first lab needs only Python.
<!-- abstract:end -->

## Key differentiators and technical novelty

1. **Statement-granular schedule control from the client.**  An adapter
   proxy pauses a session immediately before its *n*-th statement, its BEGIN,
   or its COMMIT.  It works against unmodified engines, including managed
   cloud databases where you cannot patch the server or attach a debugger.
   The SQL under test is never rewritten.  One harness covers PostgreSQL,
   MySQL, and SQLite.

2. **Serial-baseline preemption bounding.**  Sessions start a calibrated
   stagger apart, so the no-pause baseline is serial and every finding is
   attributable to specific pauses.  The size of the minimized schedule is
   then the bug's *depth*: one pause for the overdraft, double-claim and
   write-skew bugs, two for the referential phantom.  This carries the
   systematic-concurrency-testing ideas of CHESS (context bounding) and PCT
   (bug depth) over from threads to SQL sessions.  Live, a depth-1
   exhaustive sweep finds nothing while randomized exploration finds the
   depth-2 bug in three trials.

3. **Explanations, not just reproducers.**  Delta debugging (Zeller &
   Hildebrandt) over pauses, followed by delay shrinking, yields the causal
   pause *and* the race window (the smallest pause that still breaks the
   invariant).  A side-by-side timeline shows every statement, its result,
   its error, and its lock waits.

4. **Differential, categorized oracles.**  Verdicts separate a database that
   *let* an anomaly through (`isolation_anomaly`) from application code that
   ignored a rejected write (`swallowed_error`).  The two get conflated
   easily; the original demo of this very tool did.  The same schedule is
   replayed against buggy, fixed, plausible-but-wrong, and
   participant-written variants, and the exit code is the verdict.

5. **A result about the method, taught live.**  Delay points are positional,
   so replaying a bug's schedule against a structurally different fix can
   pass *vacuously*: none of its pauses ever fire.  Participants see the
   warning, then break the "fix" by attacking its own statements.  A
   regression replay is not a proof.

6. **Engineered for a room of laptops.**  The SQLite path imports only the
   Python standard library (the whole lab runs in about 3 s), `show` works
   offline, every command has a dry run, and a shared server is safe for
   30 attendees because each session gets a private schema.  ASCII and
   presenter modes are included, and the repository's CI runs the same
   checks.

7. **The tool was audited with itself.**  A control sweep of its own
   harness flagged 6 of 26 "fixed" variants.  Four were harness or oracle
   defects (false positives), including one that flagged 100% of PostgreSQL
   trials.  Two were textbook fixes that genuinely did not work.  All are
   corrected, and the two broken fixes became teaching material
   ([AUDIT.md](AUDIT.md)).

**Positioning.**  Elle/Jepsen check recorded histories of database engines
(isotest can emit Elle histories), and SQLancer-style fuzzers vary *queries*
to find engine logic bugs.  ACIDRain and IsoDiff study weak-isolation
anomalies in applications.  isotest is complementary: it keeps the
application's SQL fixed, *steers* the interleaving, and produces a minimal,
explained, replayable reproducer that can also judge a fix.

## Prerequisites and audience

| | |
|---|---|
| **Level** | Intermediate.  No prior concurrency-testing experience needed. |
| **Assumed knowledge** | SQL transactions (`BEGIN`/`COMMIT`), the SELECT-then-UPDATE pattern, reading short Python functions.  Isolation levels are covered in a 10-minute primer. |
| **Audience** | Backend and data engineers, database engineers, SRE/QA, educators, graduate students in databases and systems. |
| **Laptop** | Python 3.9+ (macOS, Linux, or Windows) and git.  Parts 2–3: PostgreSQL via Docker, a local install, or the room's shared server, plus `pip install psycopg2-binary`. |
| **Pre-flight** | `git clone --depth 1 …` (~14 MB), then `python -m isotest doctor`. |
| **Capacity** | Up to 40 participants per facilitator + TA with one shared PostgreSQL. |

## Measurable learning outcomes

By the end, each participant can do the following.  Each outcome is checked
by the tool's own exit code or output, so completion can be measured (with
consent, via `--json`).

| # | Outcome | Evidence of attainment |
|---|---------|------------------------|
| 1 | **Reproduce:** turn a flaky concurrency failure into an artifact that reproduces deterministically. | `isotest replay` reports REPRODUCED in at least 9 of 10 replays. |
| 2 | **Explain:** minimize a failing schedule and state which statement must interleave between which, and why at that point. | A one-sentence explanation that matches the minimized timeline. |
| 3 | **Classify:** tell an isolation anomaly from a swallowed-error bug by reading a timeline. | 3 of 3 correct on the shipped artifacts. |
| 4 | **Evaluate a fix:** show that a plausible fix is broken, and explain why replaying the bug's schedule alone could not show it. | `isotest sweep --variant naive_fix --expect clean` exits 1, plus the positional-delay-point explanation. |
| 5 | **Repair:** write a write-skew fix that holds under systematic and randomized attack on PostgreSQL. | `sweep --expect clean` and `explore --expect clean --trials 300` both exit 0 on their own file. |
| 6 | **Operationalize:** add a regression gate to CI and interpret its exit codes. | A CI step running `isotest replay <artifact> --control` that is green on the fix and red on the bug. |

## Session plan (90 minutes; a 3-hour extended version is available)

| Time | Segment | Mode |
|------|---------|------|
| 0:00 | Why isolation bugs escape tests (live: a 1-in-15 flaky failure) | demo |
| 0:10 | Lab 1, SQLite, zero install: explore → minimize → explain → verify → pin | hands-on |
| 0:25 | Concepts: delay points, serial baseline, depth, delta debugging, reading timelines | talk + Q&A |
| 0:40 | Lab 2, PostgreSQL: same code, true anomalies (overdraft, double claim, write skew) | hands-on |
| 0:60 | Lab 3: break a textbook fix; write one that survives systematic + randomized attack | hands-on |
| 0:80 | Shipping it: artifacts as CI regression tests; limitations; wrap-up | talk |

About 70% of the time is hands-on.  The full facilitator guide, with
expected output, troubleshooting and the shared-server setup, is
[WORKSHOP.md](../../WORKSHOP.md).

## Logistics and risk mitigation

| Risk | Mitigation |
|------|------------|
| Conference Wi-Fi | Lab 1 needs no network or packages; shallow clone ~14 MB; a USB copy of the repo works too. |
| Docker unavailable | Use the room's shared PostgreSQL; sessions are sandboxed in private schemas. |
| Slow or noisy laptops | Stagger and pauses are calibrated per machine; shipped artifacts were validated with every CPU core saturated (20/20 reproduced, fixed 0/20). |
| Windows terminals | `--ascii`, `--no-color`; no shell-specific steps. |
| Facilitator time | `isotest lab --pause` doubles as the slide deck; `selfcheck` validates the room's server in about a minute. |

**AV needs:** projector, power strips.  Network is optional.

## Availability

Open source: [github.com/hjungwoo01/tx-fuzzer](https://github.com/hjungwoo01/tx-fuzzer).
CI (`.github/workflows/ci.yml`) runs the zero-dependency lab, the test
suite, an adversarial self-check against SQLite and PostgreSQL 16, and
replays of every shipped example artifact.
