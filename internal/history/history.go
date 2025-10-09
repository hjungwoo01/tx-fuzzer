package history

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type OpType string

const (
	Invoke OpType = ":invoke"
	Ok     OpType = ":ok"
	Fail   OpType = ":fail"
	Info   OpType = ":info"
)

type Op struct {
	Type    OpType
	F       string
	Key     string
	Value   any
	Process int
	TimeNS  int64
	WallNS  int64
	TxnID   string
	Step    int
	TxnEnd  bool
	Err     string
	DBTime  string
}

// Mop is a single Elle micro-op within a transaction.
// NOTE: Key MUST be an integer for Elle's dense history.
type Mop struct {
	Kind  string // ":r" or ":w"
	Key   int    // integer key required by Elle
	Value any
}

type Writer struct {
	w       *bufio.Writer
	f       *os.File
	start   time.Time
	clockID int64
	mu      sync.Mutex
}

func New(path string) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	// can make buffer larger for higher concurrency
	return &Writer{w: bufio.NewWriterSize(f, 1<<20), f: f, start: time.Now()}, nil
}

func (hw *Writer) Close() error {
	hw.mu.Lock()
	defer hw.mu.Unlock()
	if err := hw.w.Flush(); err != nil {
		return err
	}
	return hw.f.Close()
}

func (hw *Writer) nowMonoNS() int64 { return time.Since(hw.start).Nanoseconds() }

func esc(v string) string { return strings.ReplaceAll(v, `"`, `\"`) }

func (hw *Writer) Write(op Op) error {
	hw.mu.Lock()
	defer hw.mu.Unlock()

	sb := &strings.Builder{}
	fmt.Fprintf(sb, "{:type %s :f %s :key \"%s\" :process %d :time %d :wall %d",
		op.Type, op.F, esc(op.Key), op.Process, op.TimeNS, op.WallNS)
	if op.Value != nil {
		switch v := op.Value.(type) {
		case string:
			fmt.Fprintf(sb, " :value \"%s\"", esc(v))
		default:
			fmt.Fprintf(sb, " :value %v", v)
		}
	}
	if op.TxnID != "" {
		fmt.Fprintf(sb, " :txn_id \"%s\"", esc(op.TxnID))
	}
	if op.Step != 0 {
		fmt.Fprintf(sb, " :step %d", op.Step)
	}
	if op.TxnEnd {
		fmt.Fprintf(sb, " :txn true")
	}
	if op.Err != "" {
		fmt.Fprintf(sb, " :error \"%s\"", esc(op.Err))
	}
	if op.DBTime != "" {
		fmt.Fprintf(sb, " :db_time \"%s\"", esc(op.DBTime))
	}
	sb.WriteString("}\n")
	_, err := hw.w.WriteString(sb.String())
	return err
}

func (hw *Writer) WriteTxn(process int, timeNS, wallNS int64, mops []Mop) error {
	var sb strings.Builder
	fmt.Fprintf(&sb, "{:type :ok :process %d :f :txn :time %d :wall %d :value [", process, timeNS, wallNS)
	for i, m := range mops {
		if i > 0 {
			sb.WriteByte(' ')
		}
		fmt.Fprintf(&sb, "[:%s %d ", strings.TrimPrefix(m.Kind, ":"), m.Key)
		switch v := m.Value.(type) {
		case string:
			fmt.Fprintf(&sb, "\"%s\"]", esc(v))
		default:
			fmt.Fprintf(&sb, "%v]", v)
		}
	}
	sb.WriteString("]}\n")

	hw.mu.Lock()
	defer hw.mu.Unlock()
	_, err := hw.w.WriteString(sb.String())
	return err
}

func NextTxnID() string {
	id := atomic.AddInt64(&global, 1)
	return fmt.Sprintf("tx-%d", id)
}

var global int64
