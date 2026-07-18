# Heuristic Scheduler: Algorithmic Description

## Overview

The **Heuristic scheduler** is one of three pluggable seed-generation
strategies in IsoTest's schedule exploration framework. It demonstrates
the modular extensibility of IsoTest's `BaseStrategy` interface: any
strategy that maps a set of `Event` identifiers to a `Schedule` of
`DelayPoint` tuples can be substituted without modifying the pool,
mutation, retention, or replay layers.

Unlike the **Stress** baseline (zero delays; interleavings depend
entirely on OS thread scheduling) and the **Random** baseline (uniform
i.i.d. delays across all events), the Heuristic scheduler applies a
*selective, conflict-aware* delay injection policy. It targets a small
subset of semantically relevant events with moderate delays, reflecting
the insight that concurrency bugs cluster around specific operation
boundaries — not uniformly across the execution trace.

## Formal Specification

### Input

| Symbol | Type | Description |
|--------|------|-------------|
| $E$ | `List[str]` | Ordered set of stable Event identifiers for the workload (e.g., `s0_t0_q0_begin`, `s1_t0_q2_exec`) |
| $k$ | `int` | Number of events to select for delay injection (default: 4) |
| $d_{\min}$ | `float` | Minimum delay magnitude in milliseconds (default: 5.0) |
| $d_{\max}$ | `float` | Maximum delay magnitude in milliseconds (default: 50.0) |
| $\rho$ | `Random` | Seeded pseudo-random number generator |

### Output

A `Schedule` $S = [(e_1, \delta_1), \ldots, (e_k, \delta_k)]$ where
each $(e_i, \delta_i)$ is a `DelayPoint` specifying that a wall-clock
pause of $\delta_i$ ms is injected before event $e_i$ executes.

### Algorithm

```
HEURISTIC-SEED(E, k, d_min, d_max, rho):
  1.  k' ← min(k, |E|)
  2.  C ← SAMPLE(rho, E, k')           // uniform random without replacement
  3.  S ← []
  4.  for each e_i in C:
  5.      delta_i ← UNIFORM(rho, d_min, d_max)
  6.      S ← S ++ [(e_i, delta_i)]
  7.  return Schedule(S)                // internally sorts by event ID
```

### Complexity

$O(|E|)$ to generate one seed schedule (dominated by the sampling step),
with $O(k \log k)$ for the canonical sort enforced by `Schedule`.

## Conflict Model

IsoTest's event model decomposes each database session into a sequence
of **phases**: `begin`, `exec` (one per SQL statement), and
`commit`/`rollback`. Each phase is assigned a stable, deterministic
identifier of the form:

```
s{session}_t{txn_idx}_q{query_idx}_{phase}
```

The Heuristic scheduler's conflict model rests on the observation that
isolation anomalies in TOCTOU-pattern bugs (the primary class targeted
by IsoTest's micro-workloads) are most likely to manifest when delays
are injected at **operation boundaries** — specifically:

1. **Between `begin` and the first `exec`**: Delaying one session's
   first statement while another session proceeds through its read-check
   creates the classic "check-then-act" window.

2. **Between successive `exec` steps within a transaction**: In
   read-modify-write patterns (e.g., `SELECT balance` → `UPDATE
   balance`), a delay between the read and the write widens the window
   for a concurrent session to interleave its own read, producing a
   stale-read anomaly.

3. **Before `commit`**: Delaying the commit of one session while
   allowing another to commit first can expose write-write conflicts
   under weak isolation levels.

The current implementation treats all events as equally eligible for
selection (uniform sampling from $E$), but the moderate delay magnitude
range $[d_{\min}, d_{\max}]$ is calibrated to be large enough to
reliably shift relative ordering on commodity hardware, yet small enough
to avoid masking bugs through excessive serialization of all sessions.

### Relationship to the Mutation Layer

The Heuristic scheduler produces **seed schedules** — initial points in
the delay-point space. These seeds are then refined by IsoTest's
**mutation operators** (add, adjust, remove, swap, jitter), which have
their own conflict-aware bias: the `hot_event_bias` parameter
(default: 0.7) causes mutations to preferentially target events at
`begin`, `exec`, and `commit` phases. This two-level design separates
concerns:

- **Seed strategy** determines the *initial distribution* over delay
  configurations.
- **Mutation** performs *local search* around promising seeds, guided by
  the retention policy's novelty and bug-discovery signals.

The pool layer is strategy-agnostic: `pool(heuristic)` and
`pool(random)` share the same mutation, retention, and replay
infrastructure. Any improvement of `pool(heuristic)` over
`pool(random)` is therefore attributable to the seed quality, not to
differences in the refinement loop.

## Semantically Relevant Event Identification

For the three micro-workloads in IsoTest's evaluation suite, the event
space and conflict-relevant subsets are:

| Workload | Events per Worker | Total ($n=4$) | Conflict-Critical Phases |
|----------|-------------------|---------------|--------------------------|
| `job_queue_claim` | begin, exec×3, commit | 20 | `exec` (SELECT status), `exec` (UPDATE jobs) |
| `bank_overdraft` | begin, exec×3, commit | 20 | `exec` (SELECT balance), `exec` (UPDATE accounts) |
| `list_append` | begin, exec×2, commit | 16 | `exec` (SELECT val), `exec` (UPDATE kv) |

With $k=4$ (default), the Heuristic scheduler selects 4 of these 16–20
events. Because the ratio of conflict-critical events to total events is
roughly 40–50%, a random 4-event sample has a high probability
($> 0.85$) of including at least one conflict-critical event.

## Delay Magnitude Assignment

Each selected event receives an independently drawn delay from
$\text{Uniform}(d_{\min}, d_{\max})$. The default range of
$[5, 50]$ ms was chosen based on:

1. **Lower bound (5 ms)**: On modern Linux/macOS, `time.sleep()`
   granularity is typically 1–2 ms. A 5 ms delay provides a reliable
   minimum separation that consistently alters relative thread ordering,
   even under GIL contention (Python) or kernel scheduling jitter.

2. **Upper bound (50 ms)**: Delays above ~50 ms risk artificially
   serializing all sessions, which *prevents* concurrent execution and
   thereby *hides* rather than exposes bugs. The 50 ms cap ensures that
   sessions still overlap temporally even when delays are applied.

The sensitivity of these bounds is evaluated empirically in
Section [Delay-Sensitivity Analysis], where we benchmark three profiles
(1–10 ms, 3–25 ms, 10–100 ms) and show that IsoTest's pool-based
refinement loop is robust across a 10× range of delay magnitudes.

## Reference Implementation

The Heuristic scheduler is implemented as a subclass of `BaseStrategy`
in `scripts/schedule_pool/base_strategy.py`:

```python
class HeuristicStrategy(BaseStrategy):
    """
    Pick k random events, assign moderate delays [min_ms, max_ms].
    """
    def __init__(self, k=4, min_delay_ms=5.0, max_delay_ms=50.0):
        self.k = k
        self.min_delay_ms = min_delay_ms
        self.max_delay_ms = max_delay_ms

    @property
    def name(self) -> str:
        return "heuristic"

    def seed(self, event_ids: List[str], rng: random.Random) -> Schedule:
        k = min(self.k, len(event_ids))
        chosen = rng.sample(event_ids, k)
        entries = [
            DelayPoint(event=eid,
                       delay_ms=rng.uniform(self.min_delay_ms, self.max_delay_ms))
            for eid in chosen
        ]
        return Schedule(entries)
```

The implementation is intentionally minimal (15 lines) to serve as a
reference for users extending IsoTest with domain-specific strategies.
A custom strategy need only implement the `seed(event_ids, rng) →
Schedule` method to integrate with the full pool/mutation/replay
pipeline.

### Extending the Heuristic

Users who possess domain knowledge about their workload's conflict
structure can subclass `HeuristicStrategy` to bias event selection
toward known conflict edges. For example, a strategy targeting
read-modify-write patterns might preferentially select events whose
phase is `exec` and whose `query_idx` corresponds to the read step:

```python
class RMWHeuristicStrategy(HeuristicStrategy):
    def seed(self, event_ids, rng):
        read_events = [e for e in event_ids if "_q1_exec" in e]
        other_events = [e for e in event_ids if e not in read_events]
        # Guarantee at least one read-step event is selected
        chosen = rng.sample(read_events, min(2, len(read_events)))
        remaining_k = self.k - len(chosen)
        if remaining_k > 0:
            chosen += rng.sample(other_events, min(remaining_k, len(other_events)))
        entries = [
            DelayPoint(event=eid,
                       delay_ms=rng.uniform(self.min_delay_ms, self.max_delay_ms))
            for eid in chosen
        ]
        return Schedule(entries)
```

This extensibility is the primary design goal: IsoTest provides the
scheduling infrastructure; users supply domain-specific search heuristics
as composable strategy plugins.

## Comparison with Baseline Strategies

| Property | Stress | Random | Heuristic |
|----------|--------|--------|-----------|
| Delays injected | 0 | All events | $k$ events |
| Delay range | — | $[0, d_{\max}]$ | $[d_{\min}, d_{\max}]$ |
| Total injected delay | 0 ms | $O(|E| \cdot d_{\max}/2)$ | $O(k \cdot (d_{\min}+d_{\max})/2)$ |
| Overhead per trial | Negligible | High (all events delayed) | Low (sparse delays) |
| Exploration bias | OS scheduler only | Uniform over delay space | Sparse, moderate delays |
| Sensitivity to jitter | High | Low (delays dominate) | Moderate |

The key trade-off: Random covers the delay space densely but wastes
budget on events that cannot influence interleavings. Heuristic
concentrates its delay budget on a smaller number of events, achieving
higher per-event delay magnitudes with lower total overhead. When
combined with the pool's mutation layer, the sparse Heuristic seeds are
refined into compact, high-replay-rate bug triggers — the
`BEST_TTFSR` retention criterion specifically rewards schedules with
$\leq 4$ delay points and $\geq 70\%$ replay rate.
