SEED ?= workloads/seed.yaml
WORKLOAD ?= workloads/example.yaml

OUT ?= history.edn
HIST ?= history.edn

init:
	go run ./cmd/runner -config $(SEED) -out seed_history.edn

run:
	go run ./cmd/runner -config $(WORKLOAD) -out $(OUT)

analyse:
	clj -M:run $(HIST)

check:
	clj -J-Djava.awt.headless=true -M:run $(HIST)