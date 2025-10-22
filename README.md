## Feedback-Driven Transaction Fuzzer

This repository hosts an Elle-style transaction fuzzer with an adaptive feedback loop.
The workflow is:

1. Generate a workload configuration and execute it with the Go runner to produce an Elle history (`.edn`).
2. Analyze the history with `scripts/analyze_history.py` to extract dependency edges, cycles, and near-cycle opportunities.
3. Feed those metrics into `scripts/feedback_fuzzer.py`, which tunes the next workload (hot keys, mix weights, concurrency) and repeats.

The goal is to converge on workloads that expose transactional anomalies by continuously tightening contention around the most promising keys and transaction patterns.

---

### Requirements

- Go toolchain (for `go run ./cmd/runner`).
- A reachable PostgreSQL instance; set `DATABASE_URL` for the runner.
- Python 3.9+ with the following packages: `pyyaml`, `edn_format`, `networkx`, `matplotlib`.

Install the Python dependencies (ideally inside a virtualenv):

```bash
python -m venv .venv
. .venv/bin/activate
pip install pyyaml edn_format networkx matplotlib
```

---

### Quick Start

1. **Run Docker**
```bash
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/testdb?sslmode=disable"
docker start txfuzz-pg
```

2. **Seed workload** – copy one of the sample configs under `workloads/`.

3. **Run the feedback loop**:

```bash
python scripts/feedback_fuzzer.py \
    --config workloads/example.yaml \
    --iterations 3 \
    --output-dir runs/demo \
    --runner-cmd "go run ./cmd/runner" \
    --verbose
```

You can also reference workloads by name:

```bash
python scripts/feedback_fuzzer.py --workload write_skew --iterations 5 --output-dir runs/write-skew
```

Per iteration the fuzzer will:

- Write `runs/demo/iter_XX/workload.yaml` – the workload used for that run.
- Execute the runner to produce `runs/demo/iter_XX/history.edn`.
- Summarize the analysis in `runs/demo/iter_XX/analysis.json`.
- Render `runs/demo/iter_XX/minimal_dependency_graph.png`, a tiny subgraph that highlights the missing dependency edge the next workload should try to force.
- Update key hot-spots, mix weights, and client counts for the next iteration.

Use `--skip-run` together with a custom `--runner-cmd` (e.g., a copier script) when you want to replay pre-generated histories.

---

### Inspecting Histories Manually

You can invoke the analyzer directly on any Elle history:

```bash
python scripts/analyze_history.py history.edn --skip-graph
```

The analyzer prints:

- Edge counts (WR/WW/RW), dependency density, and strongly connected components.
- Detected cycles and near-miss paths (possible G2 anomalies).
- Suggestions for intensifying contention (e.g., hot keys, scheduling tweaks).
- Optional PNG visualization (`dependency_graph.png`) when the graph is small enough.

Because the analysis module exposes structured APIs (`AnalysisResult`, `FeedbackResult`), you can import it from other tooling to build custom heuristics.

---

### Adaptive Feedback Loop

For an end-to-end, self-documenting campaign, use the adaptive loop:

```bash
python scripts/adaptive_feedback.py \
    --history history.edn \
    --prev-workload workloads/current.yaml \
    --output-root runs \
    --iterations 2 \
    --execute
```

`adaptive_feedback.py` differs from the other helpers:

- **vs `scripts/analyze_history.py`** – the analyzer emits metrics and suggestions but leaves follow-up decisions to you. `adaptive_feedback.py` consumes those insights automatically, ranks near-miss triples, and produces ready-to-run workloads plus focused graphs.
- **vs `scripts/feedback_fuzzer.py`** – the original fuzzer mutates workloads using heuristics but relies on the CLI runner for iteration control. The adaptive module wraps the entire cycle: it copies the produced history/workload into `runs/iter_XX/`, renders both the full and minimal dependency graphs, explains the mutations in `notes.md`, and can immediately execute the next workload.

Use it when you want reproducible artefacts per iteration and a targeted push toward closing specific missing edges.

---

### Deterministic Replay

Every analysis pass now produces a deterministic replay config alongside the usual artefacts. `scripts/feedback_fuzzer.py` drops `iter_XX/replay.yaml`, and `scripts/adaptive_feedback.py` does the same for interactive runs. Each file augments the workload with a `replay` stanza:

```yaml
replay:
  enabled: true
  transactions:
    - alias: start
      txn: hot_writer
    - alias: via
      txn: read_skew_probe
    - alias: end
      txn: hot_writer
  schedule:
    - alias: start
      action: step
      steps: 2
    - alias: via
      action: step
      steps: 1
    - alias: via
      action: step
      steps: 2
    - alias: end
      action: step
      steps: 2
    - alias: start
      action: step
      steps: 0      # drain remaining steps
    - alias: via
      action: commit
    - alias: end
      action: commit
    - alias: start
      action: commit
  focus_key: 42
  pause_after:
    start: [2]
    via: [1, 3]
    end: [2]
```

Run the replay directly with the existing runner:

```bash
go run ./cmd/runner --config runs/demo/iter_03/replay.yaml
```

The runner honours the `schedule` list exactly: `step` events advance a transaction by the requested number of steps (zero means “drain the remainder”), while `commit`/`rollback` finalise the transaction. Optional `sleep` events insert delays, and additional aliases or actions can be edited in the generated YAML to experiment with custom interleavings. The `pause_after` map records the exact step counts chosen by the exporter so you can see where the replay pauses before hopping to another transaction. When `focus_key` is present the tooling has already narrowed choice lists so every replayed transaction targets that key.

Pass `--run-replay` to `scripts/feedback_fuzzer.py` to execute the replay immediately after each iteration. The analyser re-checks whether the missing edge now appears in `replay_history.edn`; stubborn near cycles that still refuse to close are given extra weight when selecting keys for the next fuzzing round.

Use the replay configs to deterministically drive a candidate interleaving before jumping back into the fuzzing loop.

---

### Feedback Heuristics

`scripts/feedback_fuzzer.py` implements conservative heuristics:

- **Hot keys**: targets up to four keys extracted from near-cycle paths and key-pressure counts; these replace `choice` lists to concentrate contention.
- **Mix balancing**: write-heavy transactions gain relative weight when edge density is low, while predominantly read-only flows are down-weighted once dependency edges saturate.
- **Scheduling**: client concurrency scales up when edges-per-transaction lag, and a light barrier cadence is introduced once near cycles appear to better synchronize conflicting operations.
- **Schema init**: by default only the first iteration honors `init_schema`; later iterations skip it to keep accumulated state intact (override with `--keep-schema-init`).

All decisions and the resulting knobs (weights, clients, target keys) are logged when `--verbose` is set, and they are fully captured in the iteration configs for reproducibility.

---

### Suggested Workflow

1. Start with a broad workload (many keys, mixed reads/writes).
2. Run several feedback iterations to let the fuzzer focus on problematic keys and increase concurrency.
3. Inspect the generated `analysis.json` files – look for growing edge counts, emerging cycles, and shorter near cycles.
4. When a cycle is detected, rerun the offending iteration with `scripts/analyze_history.py` to visualize and understand the anomaly.
5. Adjust workload templates as needed (e.g., add new transaction steps) and continue iterating.

---

### Troubleshooting

- **Runner fails immediately**: confirm `DATABASE_URL` is exported and reachable; use `cmd/runner`'s `--init` once to initialize schema.
- **No edges detected**: increase clients (`scheduling.clients`) or decrease the key set size; the feedback loop handles both automatically.
- **Graph rendering skipped**: histograms with more than `--max-graph-nodes` (default 200) are intentionally not rendered; rerun the analyzer with a higher limit if needed.
- **Python import errors**: install the dependencies listed above and ensure you activate the virtual environment before running the scripts.

---

### Project Structure

- `cmd/runner/` – Go entry point that exercises workloads against the target database and writes Elle histories.
- `internal/` – Go support packages (config parsing, workload execution, history writer).
- `scripts/analyze_history.py` – Rich EDN history analysis with reusable APIs.
- `scripts/feedback_fuzzer.py` – Iterative workload tuner that drives the run → analyze → mutate loop.
- `scripts/adaptive_feedback.py` – Full feedback engine that archives each iteration, renders minimal near-miss graphs, and optionally re-runs the workload automatically.

Use these building blocks to craft custom fuzzing campaigns or integrate the feedback loop into CI pipelines that guard against transactional anomalies.
