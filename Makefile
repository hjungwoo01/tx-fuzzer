SEED ?= workloads/seed.yaml
WORKLOAD ?= workloads/next.yaml

OUT ?= history.edn
HIST ?= history.edn
BASE ?= workloads/next.yaml
NEXT_OUT ?= workloads/next.yaml
GOCACHE ?= $(CURDIR)/.gocache
MPLCONFIGDIR ?= $(CURDIR)/.mplconfig
XDG_CACHE_HOME ?= $(CURDIR)/.cache

.PHONY: init run analyse check full analyze-next analyze-only vet-go test-go test-py check-shell compile-py verify test schedule-pool-smoke

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
	python -m pytest tests/test_schedule_pool.py tests/test_multidb_canonical_hash.py

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
