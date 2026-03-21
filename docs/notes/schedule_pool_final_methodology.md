# Schedule Pool — Final Methodology

Evaluation date: 2026-03-21

## Audit Findings

### Schedule Representation
- On disk: JSONL, one ScheduleRecord per line
- Each record contains: schedule (list of {event, delay_ms}), metadata
  (schedule_id, parent_id, db, workload, variant, seed, canonical hash,
  bug info, retention reason, timestamps)
- Schedules are sorted by event ID for canonical form

### Parent-Child Mutation
- Child inherits all parent delays into a mutable dict
- 2 random mutations applied per step (add/adjust/remove/swap/jitter)
- Caps enforced: 200ms/event, 800ms total, 12 delay points
- Each mutation step produces a NEW schedule; parent is not modified

### Delay Stacking
- Stacking = child schedule inherits parent's delays + local edits
- NOT cumulative across trials: each trial samples a fresh parent from pool
- Caps prevent unbounded delay growth

### Exact Replay
- replay_schedule() applies the exact same delay_map N times
- Each replay starts from fresh DB state (setup_sql every time)
- Differences in outcome come from OS scheduling variation only

### Canonical Novelty
- canonical_state = timing-stripped logical outcome of trial
- canonical_hash = SHA-256[:16] of JSON(canonical_state, sort_keys=True)
- Tracked in RetentionPolicy._seen_states set
- New hash → RetentionReason.NEW_STATE → schedule retained

### Retention Criteria (active)
1. NEW_STATE: canonical hash not seen before
2. NEW_BUG: bug reason not seen for this workload/variant
3. REPLAY_IMPROVEMENT: higher buggy replay rate
4. SMALLER_SCHEDULE: fewer delays or ≤80% total delay
5. (BEST_TTFSR: defined but rarely triggered)

### Event-Gating in Discovery
**NO.** Schedules contain only delay magnitudes, no conditional guards.
All events have their delays applied unconditionally.
Event-gating (threading.Event) is used ONLY in heuristic case studies,
not in the schedule_pool workflow.

## Definitions

### Trial
One concurrent execution of n_workers=4 threads, each running one transaction.
Fresh DB state setup before every trial. 1 trial ≈ 4 SQL transactions.

### Throughput
Reported as trials/sec (not individual transactions/sec).
To convert: multiply by n_workers for approximate txns/sec.

### Coverage
Cumulative unique canonical states. Duration: 5.0s, Reps: 1.

### Reduced-Form Compromise
Delays are injected at the Python thread level:
- begin delay: time.sleep before worker starts transaction
- exec delays: collapsed max delay before worker's run_buggy() call
This is a reduced form because individual SQL statements inside
run_buggy() cannot be intercepted without modifying every workload.
The approach still provides meaningful coverage guidance.