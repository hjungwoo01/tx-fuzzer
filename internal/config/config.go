package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type DBConfig struct {
	Kind string `yaml:"kind"` // e.g., "postgres"
	DSN  string `yaml:"dsn"`
}

type ArgGen map[string]any

type ElleMap struct {
	F                  string `yaml:"f"`   // ":read" | ":write" | ":append" | ":inc"
	Key                string `yaml:"key"` // "$1" or literal
	Value              any    `yaml:"value,omitempty"`
	ValueFromResultCol *int   `yaml:"value_from_result_col,omitempty"`
}

type Step struct {
	SQL   string        `yaml:"sql"`
	Args  []any         `yaml:"args,omitempty"` // literals or ArgGen instructions
	Elle  *ElleMap      `yaml:"elle,omitempty"`
	Sleep time.Duration `yaml:"sleep,omitempty"`
}

type TxnTemplate struct {
	Name      string        `yaml:"name"`
	Isolation string        `yaml:"isolation,omitempty"` // e.g., "SERIALIZABLE"
	Steps     []Step        `yaml:"steps"`
	Timeout   time.Duration `yaml:"timeout,omitempty"`
}

type MixItem struct {
	Txn    string  `yaml:"txn"`
	Weight float64 `yaml:"weight"`
}

type Workload struct {
	Datatype     string        `yaml:"datatype"` // "register", "list-append", ...
	Transactions []TxnTemplate `yaml:"transactions"`
	Mix          []MixItem     `yaml:"mix"`
}

type Scheduling struct {
	Clients              int           `yaml:"clients"`
	Duration             time.Duration `yaml:"duration"`
	Ramp                 time.Duration `yaml:"ramp,omitempty"`
	MaxInflightPerClient int           `yaml:"max_inflight_per_client,omitempty"`
	BarrierEvery         int           `yaml:"barrier_every,omitempty"`
	JitterMinMs          int           `yaml:"jitter_ms_min,omitempty"`
	JitterMaxMs          int           `yaml:"jitter_ms_max,omitempty"`
	RandomKillPct        int           `yaml:"random_kill_pct,omitempty"`
}

type ReplayTxn struct {
	Alias   string        `yaml:"alias"`
	Txn     string        `yaml:"txn"`
	Process int           `yaml:"process,omitempty"`
	Timeout time.Duration `yaml:"timeout,omitempty"`
}

type ReplayStep struct {
	Alias  string        `yaml:"alias,omitempty"`
	Action string        `yaml:"action,omitempty"`
	Steps  int           `yaml:"steps,omitempty"`
	Sleep  time.Duration `yaml:"sleep,omitempty"`
}

type Replay struct {
	Enabled      bool         `yaml:"enabled"`
	Transactions []ReplayTxn  `yaml:"transactions,omitempty"`
	Schedule     []ReplayStep `yaml:"schedule,omitempty"`
}

type Root struct {
	Database   DBConfig   `yaml:"database"`
	Workload   Workload   `yaml:"workload"`
	Scheduling Scheduling `yaml:"scheduling"`
	Seed       int64      `yaml:"seed,omitempty"`
	Out        string     `yaml:"out,omitempty"`
	InitSchema bool       `yaml:"init_schema,omitempty"`
	Replay     Replay     `yaml:"replay,omitempty"`
}

func Load(path string) (*Root, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}
	var cfg Root
	if err := yaml.Unmarshal(b, &cfg); err != nil {
		return nil, fmt.Errorf("parse yaml %q: %w", path, err)
	}

	// defaults
	if cfg.Database.Kind == "" {
		cfg.Database.Kind = "postgres"
	}
	if cfg.Scheduling.Clients <= 0 {
		cfg.Scheduling.Clients = 8
	}
	if cfg.Scheduling.Duration <= 0 {
		cfg.Scheduling.Duration = 30 * time.Second
	}
	if cfg.Scheduling.JitterMinMs < 0 || cfg.Scheduling.JitterMaxMs < 0 {
		cfg.Scheduling.JitterMinMs = 0
		cfg.Scheduling.JitterMaxMs = 0
	}
	if cfg.Scheduling.JitterMaxMs != 0 && cfg.Scheduling.JitterMaxMs < cfg.Scheduling.JitterMinMs {
		cfg.Scheduling.JitterMaxMs = cfg.Scheduling.JitterMinMs
	}

	// Basic validation
	if cfg.Database.DSN == "" {
		return nil, errors.New("database.dsn is required")
	}
	if len(cfg.Workload.Transactions) == 0 {
		return nil, errors.New("workload.transactions must not be empty")
	}

	return &cfg, nil
}
