package workload

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/hjungwoo01/elle-runner/internal/config"
)

type ArgValue struct {
	Literal any
	Ref     string // "$1" style
}

type ArgGenKind int

const (
	Lit ArgGenKind = iota
	RandInt
	NowNS
	Choice
	RefArg
)

type GenSpec struct {
	Kind     ArgGenKind
	Min, Max int64
	Choices  []string
	Ref      string
}

// --- helpers to robustly coerce YAML scalars ---

func asInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int8:
		return int64(x), true
	case int16:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case uint:
		return int64(x), true
	case uint16:
		return int64(x), true
	case uint32:
		return int64(x), true
	case uint64:
		if x > uint64(math.MaxInt64) {
			return 0, false
		}
		return int64(x), true
	case float32:
		return int64(x), true
	case float64:
		return int64(x), true
	default:
		return 0, false
	}
}

func toStringSlice(xs []any) []string {
	out := make([]string, 0, len(xs))
	for _, x := range xs {
		switch s := x.(type) {
		case string:
			out = append(out, s)
		default:
			out = append(out, fmt.Sprint(s))
		}
	}
	return out
}

// Sentinel value for an "unset" seed (all 1s).
const seedUnset uint64 = ^uint64(0) // sentinel for "unset" (all 1s)

func ParseArg(a any) GenSpec {
	switch t := a.(type) {
	case string:
		if strings.HasPrefix(t, "$") {
			return GenSpec{Kind: RefArg, Ref: t}
		}
		// Literal value; runner overwrites nil later, so mark as Lit.
		return GenSpec{Kind: Lit}

	case int, int64, float64, bool:
		return GenSpec{Kind: Lit}

	case map[string]any:
		// rand_int: {min: N, max: M}
		if v, ok := t["rand_int"]; ok {
			if m, ok := v.(map[string]any); ok {
				var min, max int64
				if mv, ok := m["min"]; ok {
					if n, ok := asInt64(mv); ok {
						min = n
					}
				}
				if mv, ok := m["max"]; ok {
					if n, ok := asInt64(mv); ok {
						max = n
					}
				}
				// Ensure min <= max
				if max < min {
					min, max = max, min
				}
				return GenSpec{Kind: RandInt, Min: min, Max: max}
			}
		}
		// now_ns: {}
		if _, ok := t["now_ns"]; ok {
			return GenSpec{Kind: NowNS}
		}
		// choice: {from: [...]}
		if v, ok := t["choice"]; ok {
			if m, ok := v.(map[string]any); ok {
				if raw, ok := m["from"].([]any); ok {
					return GenSpec{Kind: Choice, Choices: toStringSlice(raw)}
				}
			}
		}
	}

	return GenSpec{Kind: Lit}
}

func MaterializeArgs(specs []GenSpec, prev []any) []any {
	out := make([]any, 0, len(specs))
	for _, s := range specs {
		switch s.Kind {
		case Lit:
			// Runner will overwrite with the user literal if present in Step.Args
			out = append(out, nil)

		case RandInt:
			// Safe even if min==max
			width := s.Max - s.Min
			if width < 0 {
				width = -width
			}
			out = append(out, int(s.Min)+rand.IntN(int(width+1)))

		case NowNS:
			out = append(out, time.Now().UnixNano())

		case Choice:
			if len(s.Choices) == 0 {
				out = append(out, nil)
			} else {
				out = append(out, s.Choices[rand.IntN(len(s.Choices))])
			}

		case RefArg:
			// "$3" etc. from previous step's materialized args
			idx := 0
			fmt.Sscanf(s.Ref, "$%d", &idx)
			if idx > 0 && idx <= len(prev) {
				out = append(out, prev[idx-1])
			} else {
				out = append(out, nil)
			}
		}
	}
	return out
}

type TxnPlan struct {
	Template    *config.TxnTemplate
	ArgsPerStep [][]GenSpec
}

func BuildPlans(wl config.Workload) map[string]TxnPlan {
	m := map[string]TxnPlan{}
	for i := range wl.Transactions {
		t := &wl.Transactions[i]
		steps := make([][]GenSpec, len(t.Steps))
		for si, st := range t.Steps {
			ss := make([]GenSpec, len(st.Args))
			for ai, a := range st.Args {
				ss[ai] = ParseArg(a)
			}
			steps[si] = ss
		}
		m[t.Name] = TxnPlan{Template: t, ArgsPerStep: steps}
	}
	return m
}

type Chooser struct {
	items   []string
	weights []float64
	sum     float64
}

func NewChooser(mix []config.MixItem) *Chooser {
	items := make([]string, len(mix))
	ws := make([]float64, len(mix))
	sum := 0.0
	for i, it := range mix {
		items[i] = it.Txn
		ws[i] = it.Weight
		sum += it.Weight
	}
	return &Chooser{items, ws, sum}
}

func (c *Chooser) Pick() string {
	x := rand.Float64() * c.sum
	acc := 0.0
	for i, w := range c.weights {
		acc += w
		if x < acc {
			return c.items[i]
		}
	}
	return c.items[len(c.items)-1]
}

func ContextWithTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, d)
}
