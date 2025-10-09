package history

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
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

type Keyword string

// Mop is a single Elle micro-op within a transaction.
// Keys are emitted as strings for compatibility with Elle EDN readers.
type Mop struct {
	Kind  string // ":r" or ":w"
	Key   string // Elle expects stringified keys in EDN output
	Value any
}

type Writer struct {
	w       *bufio.Writer
	f       *os.File
	start   time.Time
	clockID int64
	mu      sync.Mutex
	log     bool
	closed  bool
}

func New(path string) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	fmt.Printf("[diag] opening history file at %s\n", path)
	return &Writer{
		w:     bufio.NewWriterSize(f, 1<<20),
		f:     f,
		start: time.Now(),
	}, nil
}

func (hw *Writer) Close() error {
	hw.mu.Lock()
	defer hw.mu.Unlock()

	if hw.closed {
		return nil
	}
	hw.closed = true

	if err := hw.w.Flush(); err != nil {
		_ = hw.f.Close()
		return err
	}
	return hw.f.Close()
}

func esc(v string) string { return strings.ReplaceAll(v, `"`, `\"`) }

func (hw *Writer) Write(op Op) error {
	hw.mu.Lock()
	defer hw.mu.Unlock()

	if hw.closed {
		return fmt.Errorf("history: write attempted on closed writer")
	}

	var sb strings.Builder
	sb.WriteString("{:type ")
	sb.WriteString(string(op.Type))
	if op.F != "" {
		sb.WriteString(" :f ")
		sb.WriteString(normalizeKeyword(op.F))
	}
	if op.Key != "" {
		if i, err := strconv.Atoi(op.Key); err == nil {
			fmt.Fprintf(&sb, " :key %d", i)
		} else {
			fmt.Fprintf(&sb, " :key \"%s\"", esc(op.Key))
		}
	}
	fmt.Fprintf(&sb, " :process %d :time %d :wall %d",
		op.Process, op.TimeNS, op.WallNS)
	sb.WriteString(" :value ")
	if op.Value != nil {
		writeEDNValue(&sb, op.Value)
	} else {
		sb.WriteString("nil")
	}
	if op.TxnID != "" {
		fmt.Fprintf(&sb, " :txn_id \"%s\"", esc(op.TxnID))
	}
	if op.Step != 0 {
		fmt.Fprintf(&sb, " :step %d", op.Step)
	}
	if op.TxnEnd {
		sb.WriteString(" :txn true")
	}
	if op.Err != "" {
		fmt.Fprintf(&sb, " :error \"%s\"", esc(op.Err))
	}
	if op.DBTime != "" {
		fmt.Fprintf(&sb, " :db_time \"%s\"", esc(op.DBTime))
	}
	sb.WriteString("}\n")

	line := sb.String()
	if _, err := hw.w.WriteString(line); err != nil {
		return err
	}
	if hw.log {
		fmt.Printf("[history] write op: %s\n", strings.TrimSpace(line))
	}
	return hw.w.Flush()
}

func normalizeKeyword(s string) string {
	if s == "" {
		return s
	}
	if strings.HasPrefix(s, ":") {
		return s
	}
	return ":" + s
}

func (hw *Writer) SetLogWrites(enabled bool) {
	hw.mu.Lock()
	defer hw.mu.Unlock()
	hw.log = enabled
}

func writeMop(sb *strings.Builder, m Mop) {
	sb.WriteByte('[')
	sb.WriteString(normalizeKeyword(m.Kind))
	sb.WriteByte(' ')
	if i, err := strconv.Atoi(m.Key); err == nil {
		fmt.Fprintf(sb, "%d ", i)
	} else {
		fmt.Fprintf(sb, "\"%s\" ", esc(m.Key))
	}
	writeEDNValue(sb, m.Value)
	sb.WriteByte(']')
}

func writeEDNValue(sb *strings.Builder, v any) {
	switch x := v.(type) {
	case nil:
		sb.WriteString("nil")
	case string:
		fmt.Fprintf(sb, "\"%s\"", esc(x))
	case Keyword:
		sb.WriteString(string(x))
	case fmt.Stringer:
		fmt.Fprintf(sb, "\"%s\"", esc(x.String()))
	case []byte:
		fmt.Fprintf(sb, "\"%s\"", esc(string(x)))
	case []any:
		sb.WriteString("[")
		for i, elem := range x {
			if i > 0 {
				sb.WriteByte(' ')
			}
			writeEDNValue(sb, elem)
		}
		sb.WriteString("]")
	case []string:
		sb.WriteString("[")
		for i, elem := range x {
			if i > 0 {
				sb.WriteByte(' ')
			}
			writeEDNValue(sb, elem)
		}
		sb.WriteString("]")
	case map[string]any:
		sb.WriteString("{")
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for i, k := range keys {
			if i > 0 {
				sb.WriteByte(' ')
			}
			fmt.Fprintf(sb, ":%s ", k)
			writeEDNValue(sb, x[k])
		}
		sb.WriteString("}")
	case map[string]string:
		sb.WriteString("{")
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for i, k := range keys {
			if i > 0 {
				sb.WriteByte(' ')
			}
			fmt.Fprintf(sb, ":%s ", k)
			writeEDNValue(sb, x[k])
		}
		sb.WriteString("}")
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, bool:
		fmt.Fprintf(sb, "%v", x)
	default:
		fmt.Fprintf(sb, "\"%s\"", esc(fmt.Sprintf("%v", x)))
	}
}

func (hw *Writer) WriteTxn(opType OpType, process int, timeNS, wallNS int64, mops []Mop) error {
	var sb strings.Builder
	sb.WriteString("{:type ")
	sb.WriteString(string(opType))
	sb.WriteString(" :f :txn")
	fmt.Fprintf(&sb, " :process %d :time %d :wall %d", process, timeNS, wallNS)
	sb.WriteString(" :value [")
	for i, m := range mops {
		if i > 0 {
			sb.WriteByte(' ')
		}
		writeMop(&sb, m)
	}
	sb.WriteString("]}\n")
	line := sb.String()

	hw.mu.Lock()
	defer hw.mu.Unlock()
	if hw.closed {
		return fmt.Errorf("history: write txn attempted on closed writer")
	}
	if _, err := hw.w.WriteString(line); err != nil {
		return err
	}
	if hw.log {
		fmt.Printf("[history] write txn: %s\n", strings.TrimSpace(line))
	}
	return hw.w.Flush()
}

func NextTxnID() string {
	id := atomic.AddInt64(&global, 1)
	return fmt.Sprintf("tx-%d", id)
}

var global int64
