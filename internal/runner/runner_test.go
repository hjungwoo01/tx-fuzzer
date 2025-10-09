package runner

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hjungwoo01/tx-fuzzer/internal/config"
	"github.com/hjungwoo01/tx-fuzzer/internal/driver"
	"github.com/hjungwoo01/tx-fuzzer/internal/history"
)

type fakeDriver struct{}

func (f *fakeDriver) Begin(ctx context.Context, iso driver.Isolation) (driver.Tx, error) {
	return &fakeTx{}, nil
}
func (f *fakeDriver) Clock(ctx context.Context) (string, error) { return "", nil }
func (f *fakeDriver) InitSchema(ctx context.Context) error      { return nil }
func (f *fakeDriver) Close()                                    {}

type fakeTx struct{}

func (t *fakeTx) Exec(ctx context.Context, sql string, args ...any) (int64, error) {
	return 1, nil
}
func (t *fakeTx) QueryRow(ctx context.Context, sql string, args ...any) (driver.ScanRow, error) {
	return fakeRow{}, nil
}
func (t *fakeTx) Commit(ctx context.Context) error   { return nil }
func (t *fakeTx) Rollback(ctx context.Context) error { return nil }

type fakeRow struct{}

func (fakeRow) Scan(dest ...any) error {
	if len(dest) > 0 {
		switch d := dest[0].(type) {
		case *any:
			*d = int64(42)
		}
	}
	return nil
}

func TestRunWritesHistory(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "history-*.edn")
	if err != nil {
		t.Fatal(err)
	}
	tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	hw, err := history.New(tmpFile.Name())
	if err != nil {
		t.Fatalf("history.New: %v", err)
	}

	cfg := &config.Root{
		Workload: config.Workload{
			Transactions: []config.TxnTemplate{
				{
					Name:      "inc",
					Isolation: "READ COMMITTED",
					Timeout:   100 * time.Millisecond,
					Steps: []config.Step{
						{
							SQL:  "UPDATE kv SET v=v+1 WHERE k=$1",
							Args: []any{1},
							Elle: &config.ElleMap{
								F:   ":write",
								Key: "$1",
							},
						},
					},
				},
			},
			Mix: []config.MixItem{
				{Txn: "inc", Weight: 1},
			},
		},
		Scheduling: config.Scheduling{
			Clients:  1,
			Duration: 25 * time.Millisecond,
		},
	}

	env := &Env{
		Drv:   &fakeDriver{},
		Cfg:   cfg,
		Hist:  hw,
		Start: time.Now(),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := Run(ctx, env); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := hw.Close(); err != nil {
		t.Fatalf("Close history: %v", err)
	}

	data, err := os.ReadFile(tmpFile.Name())
	if err != nil {
		t.Fatalf("read history: %v", err)
	}
	if len(data) == 0 {
		t.Fatalf("history file is empty")
	}
	content := string(data)
	if !strings.Contains(content, ":f :txn") {
		t.Fatalf("history missing txn entry:\n%s", content)
	}
}
