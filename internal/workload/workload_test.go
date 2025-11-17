package workload

import (
	"testing"
	"time"

	"github.com/hjungwoo01/tx-fuzzer/internal/config"
)

func TestFlattenTxnQueries(t *testing.T) {
	tpl := config.TxnTemplate{
		Name: "test",
		Queries: []config.Query{
			{SQL: "SELECT 1"},
		},
		Steps: []config.Step{
			{
				SQL:   "UPDATE kv SET v=v+1 WHERE k=$1",
				Args:  []any{1},
				Sleep: 50 * time.Millisecond,
			},
			{
				Sleep: 100 * time.Millisecond,
				Queries: []config.Query{
					{SQL: "INSERT INTO kv(k,v) VALUES($1,$2)", Sleep: 10 * time.Millisecond},
					{SQL: "DELETE FROM kv WHERE k=$1"},
				},
			},
		},
	}

	plans := BuildPlans(config.Workload{Transactions: []config.TxnTemplate{tpl}})
	plan, ok := plans["test"]
	if !ok {
		t.Fatalf("plan for txn 'test' not found")
	}
	if len(plan.Queries) != 4 {
		t.Fatalf("expected 4 queries, got %d", len(plan.Queries))
	}
	if plan.Queries[0].Template.SQL != "SELECT 1" {
		t.Fatalf("unexpected first query: %s", plan.Queries[0].Template.SQL)
	}
	if plan.Queries[1].Template.Sleep != 50*time.Millisecond {
		t.Fatalf("expected step sleep to propagate (got %s)", plan.Queries[1].Template.Sleep)
	}
	if plan.Queries[2].Template.Sleep != 10*time.Millisecond {
		t.Fatalf("expected nested query sleep preserved (got %s)", plan.Queries[2].Template.Sleep)
	}
	if plan.Queries[3].Template.Sleep != 100*time.Millisecond {
		t.Fatalf("expected parent step sleep appended to final nested query (got %s)", plan.Queries[3].Template.Sleep)
	}
	if len(plan.Queries[1].ArgsSpec) != 1 {
		t.Fatalf("expected args to carry over (got %d)", len(plan.Queries[1].ArgsSpec))
	}
}
