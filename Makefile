# ===========================================================================
# IsoTest Makefile
#
# Reviewer targets (Docker, "one-click"):
#   make help       — print this guide
#   make build      — build the Docker image
#   make demo       — fast end-to-end: explore → artifact → replay → Elle
#   make eval-fast  — 2-minute scaled-down sensitivity + coverage run
#   make eval-full  — full paper-quality evaluation (long-running)
#   make shell      — interactive bash inside the container
#
# Developer targets (native, requires local toolchain):
#   make test       — Go + Python tests
#   make verify     — full suite: vet, shell check, compile check, tests
#   make schedule-pool-smoke — quick SQLite smoke test (~5s)
# ===========================================================================

SEED ?= workloads/seed.yaml
WORKLOAD ?= workloads/next.yaml

OUT ?= history.edn
HIST ?= history.edn
BASE ?= workloads/next.yaml
NEXT_OUT ?= workloads/next.yaml
GOCACHE ?= $(CURDIR)/.gocache
MPLCONFIGDIR ?= $(CURDIR)/.mplconfig
XDG_CACHE_HOME ?= $(CURDIR)/.cache

IMAGE_NAME ?= isotest-artifact
DOCKER_RUN  = docker run --rm \
	-v $(CURDIR)/results:/app/results \
	-e MPLBACKEND=Agg \
	$(IMAGE_NAME)
DOCKER_RUN_IT = docker run --rm -it \
	-v $(CURDIR)/results:/app/results \
	-e MPLBACKEND=Agg \
	$(IMAGE_NAME)

.PHONY: help build demo eval-fast eval-full eval-conference shell \
	sensitivity-native coverage-native elle-verify-native \
	baselines-native scalability-native significance-native oracle-soundness-native \
	eval-paper eval-paper-check eval-paper-help eval-paper-parallel \
	init run analyse check full analyze-next analyze-only \
	vet-go test-go test-py check-shell compile-py verify test \
	schedule-pool-smoke demo-explore demo-replay demo-explore-replay

# ---- Default target: help ----
help:
	@echo ""
	@echo "  IsoTest — Client-Side Schedule Exploration for Isolation Testing"
	@echo "  ================================================================"
	@echo ""
	@echo "  QUICK START (Docker only — no other dependencies):"
	@echo ""
	@echo "    make build       Build the Docker image (~3 min first time)"
	@echo "    make demo        Fast end-to-end pipeline: explore → replay → Elle verify"
	@echo "    make eval-fast        Scaled-down evaluation (~2 min), CSVs in results/"
	@echo "    make eval-full        Full paper evaluation (long-running), CSVs in results/"
	@echo "    make eval-conference  Full conference-quality matrix: baselines,"
	@echo "                          significance, scalability, oracle soundness"
	@echo "    make shell       Interactive bash shell inside the container"
	@echo ""
	@echo "  PAPER EVALUATION (native, env-var configurable, multi-DB):"
	@echo ""
	@echo "    make eval-paper-check     Pre-flight: DSN reachability, smoke pass, time est."
	@echo "    make eval-paper           Real run; writes results/paper/<timestamp>/"
	@echo "    make eval-paper-parallel  Run one process per DB in EVAL_DBS_PARALLEL,"
	@echo "                              isolated EVAL_OUT and SQLITE_DSN per process."
	@echo "    make eval-paper-help      Print all env-var knobs and current defaults"
	@echo ""
	@echo "    See docs/eval_paper.md for the full operator's manual."
	@echo ""
	@echo "  Output artifacts are written to ./results/ on your host machine."
	@echo ""
	@echo "  DEVELOPER TARGETS (requires local Go, Python, Clojure):"
	@echo ""
	@echo "    make test                   Go + Python tests"
	@echo "    make verify                 Full suite: vet, shellcheck, compile, tests"
	@echo "    make schedule-pool-smoke    Quick SQLite smoke (~5s, no Docker)"
	@echo "    make demo-explore-replay    Local explore → save → replay (SQLite)"
	@echo ""
	@echo "  NATIVE EVALUATION (no Docker):"
	@echo ""
	@echo "    make sensitivity-native     Sensitivity benchmark (local Python)"
	@echo "    make coverage-native        Longitudinal coverage (local Python)"
	@echo "    make elle-verify-native     Elle verification (local Clojure)"
	@echo ""

# ===========================================================================
# Docker
# ===========================================================================

build:
	docker build -t $(IMAGE_NAME) .

# Fast end-to-end demo: explore → save artifact → replay → Elle verify
# Runs entirely on SQLite inside the container (~30s).
demo: build
	@echo ""
	@echo "=== IsoTest Demo: Explore → Artifact → Replay → Elle Verify ==="
	@echo ""
	@mkdir -p results/demo
	$(DOCKER_RUN) bash -c '\
		set -e; \
		echo "[1/4] Exploring bank_overdraft on SQLite (10s)..."; \
		SQLITE_DSN=$$(mktemp /tmp/isotest-demo.XXXXXX) \
			python scripts/run_demo.py explore \
				--db sqlite --workload bank_overdraft \
				--duration 10 --output-dir /app/results/demo; \
		echo ""; \
		echo "[2/4] Listing saved artifacts..."; \
		ls -la /app/results/demo/*.json 2>/dev/null || echo "  (no artifacts saved — bug may not have triggered in 10s)"; \
		echo ""; \
		ARTIFACT=$$(ls /app/results/demo/*_sqlite.json 2>/dev/null | head -1); \
		if [ -n "$$ARTIFACT" ]; then \
			echo "[3/4] Replaying artifact: $$ARTIFACT"; \
			python scripts/run_demo.py replay "$$ARTIFACT" || true; \
		else \
			echo "[3/4] Skipping replay (no artifact to replay)"; \
		fi; \
		echo ""; \
		EDN=$$(ls /app/results/demo/*.edn 2>/dev/null | head -1); \
		if [ -n "$$EDN" ]; then \
			echo "[4/4] Running Elle verification on $$EDN"; \
			python scripts/verify_elle.py "$$EDN" \
				--output-dir /app/results/demo \
				--models snapshot-isolation || true; \
		else \
			echo "[4/4] Skipping Elle (no EDN history generated)"; \
		fi; \
		echo ""; \
		echo "=== Demo complete. Check results/demo/ on your host. ===" \
	'

# Scaled-down evaluation (~2 min): sensitivity + longitudinal coverage
# Output CSVs are mounted to the host at ./results/
eval-fast: build
	@echo ""
	@echo "=== IsoTest Eval-Fast: Sensitivity + Coverage (~2 min) ==="
	@echo ""
	@mkdir -p results
	$(DOCKER_RUN) bash -c '\
		set -e; \
		echo "--- Delay-Sensitivity Benchmark (30s/profile, 1 rep) ---"; \
		python scripts/evaluate_sensitivity.py \
			--db sqlite \
			--duration 30 --reps 1 --n-replays 5 \
			--output-dir /app/results/sensitivity; \
		echo ""; \
		echo "--- Longitudinal Coverage (60s, 1 rep) ---"; \
		python scripts/evaluate_coverage_longitudinal.py \
			--db sqlite \
			--duration 60 --reps 1 --sample-interval 5 \
			--strategies stress random heuristic \
			--output-dir /app/results/longitudinal; \
		echo ""; \
		echo "=== Eval-fast complete. Results in results/ on your host. ===" \
	'

# Evaluation (long-running):
# - Sensitivity: 60s/profile, 3 reps
# - Longitudinal: 600s, 3 reps, all 5 strategies
eval-full: build
	@echo ""
	@echo "=== IsoTest Evaluation ==="
	@echo ""
	@mkdir -p results
	$(DOCKER_RUN) bash -c '\
		set -e; \
		echo "--- Delay-Sensitivity Benchmark (60s/profile, 3 reps) ---"; \
		python scripts/evaluate_sensitivity.py \
			--db sqlite \
			--duration 60 --reps 3 --n-replays 20 \
			--output-dir /app/results/sensitivity; \
		echo ""; \
		echo "--- Longitudinal Coverage (600s, 3 reps, all strategies) ---"; \
		python scripts/evaluate_coverage_longitudinal.py \
			--db sqlite \
			--duration 600 --reps 3 --sample-interval 2 \
			--output-dir /app/results/longitudinal; \
		echo ""; \
		echo "=== Eval-full complete. Results in results/ on your host. ===" \
	'

# Interactive shell for manual exploration
shell: build
	@echo "Dropping into container shell. results/ is mounted at /app/results."
	$(DOCKER_RUN_IT) bash

# ===========================================================================
# Native evaluation targets (no Docker — requires local Python + Clojure)
# ===========================================================================

sensitivity-native:
	python scripts/evaluate_sensitivity.py \
		--db sqlite --duration 60 --reps 3 \
		--output-dir results/sensitivity

coverage-native:
	python scripts/evaluate_coverage_longitudinal.py \
		--db sqlite --duration 600 --reps 3 \
		--output-dir results/longitudinal

elle-verify-native:
	bash scripts/verify_elle.sh results/demo/*.edn \
		--output-dir results/elle

# ---- Conference-quality evaluation extensions ----------------------------
# Each target writes CSV + reports under results/<axis>/.

baselines-native:
	python scripts/evaluate_baselines.py \
		--db sqlite --duration 120 --reps 3 \
		--output-dir results/baselines

scalability-native:
	python scripts/evaluate_scalability.py \
		--db sqlite --workloads bank_overdraft multikey_transfer \
		--workers 2 4 8 --scales 1 2 \
		--strategies stress random heuristic pct 'pool(heuristic)' \
		--duration 60 --reps 2 \
		--output-dir results/scalability

significance-native:
	python scripts/evaluate_significance.py \
		results/longitudinal/coverage_timeseries.csv \
		--metric states --output results/longitudinal/significance
	python scripts/evaluate_significance.py \
		results/baselines/baseline_timeseries.csv \
		--metric states --output results/baselines/significance

oracle-soundness-native:
	python scripts/validate_oracle_soundness.py \
		--db sqlite \
		--workloads bank_overdraft list_append job_queue_claim \
			lost_update_counter inventory_oversell unique_violation \
		--strategies random heuristic \
		--n-trials 200 --reps 1 \
		--output-dir results/oracle_soundness

# Run the entire conference-quality evaluation matrix:
#   1) external baselines      (PCT, naive, IID, Jepsen-style, stress vs IsoTest)
#   2) longitudinal coverage   (existing) + statistical significance
#   3) scalability sweep       (workers x table-size)
#   4) oracle soundness        (cross-validation vs Elle-style validators)
eval-conference: build
	@echo ""
	@echo "=== IsoTest Conference-Quality Evaluation ==="
	@echo ""
	@mkdir -p results
	$(DOCKER_RUN) bash -c '\
		set -e; \
		echo "[1/5] External baselines (PCT, naive, IID, Jepsen-style, stress)"; \
		python scripts/evaluate_baselines.py --db sqlite \
			--duration 120 --reps 3 \
			--output-dir /app/results/baselines; \
		echo ""; \
		echo "[2/5] Longitudinal coverage (existing 600s, 3 reps)"; \
		python scripts/evaluate_coverage_longitudinal.py --db sqlite \
			--duration 600 --reps 3 --sample-interval 5 \
			--output-dir /app/results/longitudinal; \
		echo ""; \
		echo "[3/5] Scalability sweep"; \
		python scripts/evaluate_scalability.py --db sqlite \
			--workers 2 4 8 --scales 1 2 \
			--strategies stress random heuristic pct "pool(heuristic)" \
			--duration 60 --reps 2 \
			--output-dir /app/results/scalability; \
		echo ""; \
		echo "[4/5] Statistical significance over the longitudinal CSV"; \
		python scripts/evaluate_significance.py \
			/app/results/longitudinal/coverage_timeseries.csv \
			--metric states \
			--output /app/results/longitudinal/significance; \
		python scripts/evaluate_significance.py \
			/app/results/baselines/baseline_timeseries.csv \
			--metric states \
			--output /app/results/baselines/significance; \
		echo ""; \
		echo "[5/5] Oracle soundness vs independent validators"; \
		python scripts/validate_oracle_soundness.py --db sqlite \
			--workloads bank_overdraft list_append job_queue_claim \
				lost_update_counter inventory_oversell unique_violation \
			--strategies random heuristic --n-trials 200 \
			--output-dir /app/results/oracle_soundness; \
		echo ""; \
		echo "=== Conference eval complete. CSVs under results/. ===" \
	'

# ===========================================================================
# Developer targets (native toolchain)
# ===========================================================================

init:
	GOCACHE=$(GOCACHE) go run ./cmd/runner -config $(SEED) -out seed_history.edn

run:
	GOCACHE=$(GOCACHE) go run ./cmd/runner -config $(WORKLOAD) -out $(OUT)

analyse:
	clj -M:run $(HIST)

check:
	clj -J-Djava.awt.headless=true -M:run $(HIST)

# Full workflow: run workload, analyze, check, and generate next workload
full: run
	python scripts/analyze_and_next.py \
		--workload $(WORKLOAD) \
		--history $(OUT) \
		--base $(BASE) \
		--out $(NEXT_OUT)

# Run analysis and generate next workload (skip running the workload)
analyze-next:
	python scripts/analyze_and_next.py \
		--workload $(WORKLOAD) \
		--history $(HIST) \
		--base $(BASE) \
		--out $(NEXT_OUT)

# Run analysis only (skip generating next workload)
analyze-only:
	python scripts/analyze_and_next.py \
		--workload $(WORKLOAD) \
		--history $(HIST) \
		--skip-next

vet-go:
	GOCACHE=$(GOCACHE) go vet ./...

test-go:
	GOCACHE=$(GOCACHE) go test ./...

test-py:
	python -m pytest tests/test_schedule_pool.py tests/test_multidb_canonical_hash.py tests/test_evaluation_extensions.py

check-shell:
	find . \
		\( -path './.git' -o -path './.venv' -o -path './.gocache' -o -path './.gomodcache' -o -path './.cache' -o -path './results' \) -prune \
		-o -type f -name '*.sh' -exec bash -n {} +

compile-py:
	python -m compileall -q scripts workloads litmus bench tests

verify: vet-go check-shell compile-py test

test: test-go test-py

schedule-pool-smoke:
	SQLITE_DSN=$$(mktemp /tmp/txfuzzer-schedule-pool-smoke.XXXXXX) MPLCONFIGDIR=$(MPLCONFIGDIR) XDG_CACHE_HOME=$(XDG_CACHE_HOME) python scripts/experiments/run_final_eval.py --smoke --db sqlite

demo-explore:
	SQLITE_DSN=$$(mktemp /tmp/isotest-demo.XXXXXX) python scripts/run_demo.py explore --db sqlite --workload bank_overdraft --duration 10 --output-dir examples/repros

demo-replay:
	python scripts/run_demo.py replay examples/repros/bank_overdraft_sqlite.json

demo-explore-replay:
	SQLITE_DSN=$$(mktemp /tmp/isotest-demo.XXXXXX) python scripts/run_demo.py explore-replay --db sqlite --workload bank_overdraft --duration 10 --output-dir examples/repros

# ===========================================================================
# Paper-quality evaluation — single-command, native, env-var configurable
# ===========================================================================
#
# Defaults give a 1-hour-per-cell run on sqlite only (~24h total at default
# matrix). Override any of the EVAL_* vars below; see docs/eval_paper.md.
#
#   make eval-paper-check    Pre-flight: tests, imports, DSN, smoke, time est.
#   make eval-paper          Real run; writes results/paper/<timestamp>/
#   make eval-paper-help     Print this section + the docs file
#
# Long runs (24h+ per cell):
#   EVAL_DURATION_S=86400 make eval-paper
# Multi-DB:
#   EVAL_DBS="sqlite postgres" \
#     POSTGRES_DSN="postgres://postgres:postgres@localhost:54323/postgres?sslmode=disable" \
#     make eval-paper

PYTHON ?= .venv/bin/python

EVAL_DBS                 ?= sqlite
EVAL_DURATION_S          ?= 3600
EVAL_SCALE_DURATION_S    ?= 600
EVAL_REPS                ?= 3
EVAL_N_WORKERS           ?= 4
EVAL_WORKLOADS_BASELINE  ?= bank_overdraft inventory_oversell list_append multikey_transfer
EVAL_WORKLOADS_SCALE     ?= bank_overdraft multikey_transfer
EVAL_WORKLOADS_ORACLE    ?= bank_overdraft inventory_oversell unique_violation list_append job_queue_claim lost_update_counter
EVAL_STRATEGIES          ?= stress pct heuristic 'pool(heuristic)'
EVAL_SCALE_WORKERS       ?= 2 8
EVAL_SCALE_TABLE_SCALES  ?= 1 2
EVAL_OUT                 ?= results/paper/$(shell date +%Y%m%d_%H%M%S)

# Export so the preflight script and child scripts see them.
export PYTHON
export EVAL_DBS EVAL_DURATION_S EVAL_SCALE_DURATION_S EVAL_REPS EVAL_N_WORKERS
export EVAL_WORKLOADS_BASELINE EVAL_WORKLOADS_SCALE EVAL_WORKLOADS_ORACLE
export EVAL_STRATEGIES EVAL_SCALE_WORKERS EVAL_SCALE_TABLE_SCALES EVAL_OUT

eval-paper-help:
	@echo ""
	@echo "  make eval-paper — single-command paper evaluation"
	@echo "  ================================================="
	@echo ""
	@echo "  Phases:"
	@echo "    1) baselines              — head-to-head vs PCT/naive/IID/Jepsen/stress"
	@echo "    2) scalability sweep      — workers x table-scale matrix"
	@echo "    3) statistical significance — MWU + Cliff's delta over baselines"
	@echo "    4) oracle soundness       — confusion matrix vs log-replay validator"
	@echo ""
	@echo "  Tunable env vars (current values shown):"
	@echo "    EVAL_DBS                 = $(EVAL_DBS)"
	@echo "    EVAL_DURATION_S          = $(EVAL_DURATION_S)   (per-cell, baselines)"
	@echo "    EVAL_SCALE_DURATION_S    = $(EVAL_SCALE_DURATION_S)   (per-cell, scalability)"
	@echo "    EVAL_REPS                = $(EVAL_REPS)"
	@echo "    EVAL_N_WORKERS           = $(EVAL_N_WORKERS)"
	@echo "    EVAL_WORKLOADS_BASELINE  = $(EVAL_WORKLOADS_BASELINE)"
	@echo "    EVAL_WORKLOADS_SCALE     = $(EVAL_WORKLOADS_SCALE)"
	@echo "    EVAL_WORKLOADS_ORACLE    = $(EVAL_WORKLOADS_ORACLE)"
	@echo "    EVAL_STRATEGIES          = $(EVAL_STRATEGIES)"
	@echo "    EVAL_SCALE_WORKERS       = $(EVAL_SCALE_WORKERS)"
	@echo "    EVAL_SCALE_TABLE_SCALES  = $(EVAL_SCALE_TABLE_SCALES)"
	@echo "    EVAL_OUT                 = $(EVAL_OUT)"
	@echo ""
	@echo "  Parallel-runner env vars (used by 'make eval-paper-parallel'):"
	@echo "    EVAL_DBS_PARALLEL        (default: same as EVAL_DBS)  one process per DB"
	@echo "    EVAL_OUT_ROOT            (default: results/paper/<timestamp>_parallel)"
	@echo ""
	@echo "  DSN env vars (set if EVAL_DBS includes postgres or mysql):"
	@echo "    POSTGRES_DSN, MYSQL_DSN, SQLITE_DSN"
	@echo ""
	@echo "  See docs/eval_paper.md for full details."
	@echo ""

eval-paper-check:
	@bash scripts/eval_paper_preflight.sh

# Parallel multi-DB eval: one process per DB, isolated EVAL_OUT, isolated
# SQLITE_DSN. See scripts/eval_paper_parallel.sh for the orchestration.
#
#   EVAL_DBS_PARALLEL="sqlite postgres mysql" \
#     POSTGRES_DSN="..." MYSQL_DSN="..." \
#     make eval-paper-parallel
eval-paper-parallel:
	@bash scripts/eval_paper_parallel.sh

eval-paper:
	@mkdir -p "$(EVAL_OUT)"
	@echo "" | tee "$(EVAL_OUT)/run.log"
	@echo "=== IsoTest paper evaluation -> $(EVAL_OUT) ===" | tee -a "$(EVAL_OUT)/run.log"
	@date | tee -a "$(EVAL_OUT)/run.log"
	@echo "" | tee -a "$(EVAL_OUT)/run.log"
	@printf "config:\n  EVAL_DBS=%s\n  EVAL_DURATION_S=%s\n  EVAL_SCALE_DURATION_S=%s\n  EVAL_REPS=%s\n  EVAL_N_WORKERS=%s\n  EVAL_WORKLOADS_BASELINE=%s\n  EVAL_WORKLOADS_SCALE=%s\n  EVAL_WORKLOADS_ORACLE=%s\n  EVAL_STRATEGIES=%s\n  EVAL_SCALE_WORKERS=%s\n  EVAL_SCALE_TABLE_SCALES=%s\n\n" \
	    "$(EVAL_DBS)" "$(EVAL_DURATION_S)" "$(EVAL_SCALE_DURATION_S)" "$(EVAL_REPS)" "$(EVAL_N_WORKERS)" \
	    "$(EVAL_WORKLOADS_BASELINE)" "$(EVAL_WORKLOADS_SCALE)" "$(EVAL_WORKLOADS_ORACLE)" \
	    "$(EVAL_STRATEGIES)" "$(EVAL_SCALE_WORKERS)" "$(EVAL_SCALE_TABLE_SCALES)" \
	    | tee -a "$(EVAL_OUT)/run.log"
	@echo "[1/4] baselines ($(EVAL_WORKLOADS_BASELINE) x $(EVAL_STRATEGIES) x $(EVAL_REPS) reps x $(EVAL_DURATION_S)s)" | tee -a "$(EVAL_OUT)/run.log"
	$(PYTHON) scripts/evaluate_baselines.py \
	    --db $(EVAL_DBS) \
	    --workloads $(EVAL_WORKLOADS_BASELINE) \
	    --strategies $(EVAL_STRATEGIES) \
	    --duration $(EVAL_DURATION_S) \
	    --reps $(EVAL_REPS) \
	    --n-workers $(EVAL_N_WORKERS) \
	    --output-dir "$(EVAL_OUT)/baselines" 2>&1 | tee -a "$(EVAL_OUT)/run.log"
	@echo "[2/4] scalability ($(EVAL_WORKLOADS_SCALE) x workers=$(EVAL_SCALE_WORKERS) x scales=$(EVAL_SCALE_TABLE_SCALES))" | tee -a "$(EVAL_OUT)/run.log"
	$(PYTHON) scripts/evaluate_scalability.py \
	    --db $(EVAL_DBS) \
	    --workloads $(EVAL_WORKLOADS_SCALE) \
	    --workers $(EVAL_SCALE_WORKERS) \
	    --scales $(EVAL_SCALE_TABLE_SCALES) \
	    --strategies $(EVAL_STRATEGIES) \
	    --duration $(EVAL_SCALE_DURATION_S) \
	    --reps $(EVAL_REPS) \
	    --output-dir "$(EVAL_OUT)/scalability" 2>&1 | tee -a "$(EVAL_OUT)/run.log"
	@echo "[3/4] significance over baseline_timeseries.csv" | tee -a "$(EVAL_OUT)/run.log"
	$(PYTHON) scripts/evaluate_significance.py \
	    "$(EVAL_OUT)/baselines/baseline_timeseries.csv" \
	    --metric states \
	    --output "$(EVAL_OUT)/baselines/significance" 2>&1 | tee -a "$(EVAL_OUT)/run.log"
	@echo "[4/4] oracle soundness vs log-replay validator" | tee -a "$(EVAL_OUT)/run.log"
	$(PYTHON) scripts/validate_oracle_soundness.py \
	    --db $(EVAL_DBS) \
	    --workloads $(EVAL_WORKLOADS_ORACLE) \
	    --strategies random heuristic \
	    --n-trials 200 \
	    --reps $(EVAL_REPS) \
	    --output-dir "$(EVAL_OUT)/oracle_soundness" 2>&1 | tee -a "$(EVAL_OUT)/run.log"
	@echo "" | tee -a "$(EVAL_OUT)/run.log"
	@date | tee -a "$(EVAL_OUT)/run.log"
	@echo "=== Paper eval complete -> $(EVAL_OUT) ===" | tee -a "$(EVAL_OUT)/run.log"
