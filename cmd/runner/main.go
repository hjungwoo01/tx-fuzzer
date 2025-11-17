package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"time"

	"github.com/hjungwoo01/tx-fuzzer/internal/config"
	"github.com/hjungwoo01/tx-fuzzer/internal/driver"
	"github.com/hjungwoo01/tx-fuzzer/internal/history"
	"github.com/hjungwoo01/tx-fuzzer/internal/runner"
	_ "github.com/lib/pq"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL environment variable not set")
	}

	testDBConnection(dsn)

	cfgPath := flag.String("config", "workloads/example.yaml", "YAML config")
	initSchema := flag.Bool("init", false, "init schema and exit")
	outPath := flag.String("out", "history.edn", "Elle history output path")
	runElle := flag.Bool("elle", false, "run Elle analysis after generating the history")
	elleTimeout := flag.Duration("elle-timeout", 2*time.Minute, "maximum time to wait for the Elle analysis")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatal(err)
	}
	if cfg.Out != "" {
		*outPath = cfg.Out
	}

	var drv driver.Driver
	switch cfg.Database.Kind {
	case "postgres":
		drv, err = driver.NewPostgres(cfg.Database.DSN)
	default:
		log.Fatalf("unsupported db kind: %s", cfg.Database.Kind)
	}
	if err != nil {
		log.Fatal(err)
	}
	defer drv.Close()

	ctx := context.Background()

	if *initSchema || cfg.InitSchema {
		if err := drv.InitSchema(ctx); err != nil {
			log.Fatal(err)
		}
		fmt.Println("schema initialized")
		if *initSchema {
			return
		}
	}

	hw, err := history.New(*outPath)
	if err != nil {
		log.Fatal(err)
	}

	env := &runner.Env{
		Drv: drv, Cfg: cfg, Hist: hw, Start: time.Now(),
	}
	var runErr error
	if cfg.Replay.Enabled {
		runErr = runner.RunReplay(ctx, env)
	} else {
		runErr = runner.Run(ctx, env)
	}
	if runErr != nil {
		log.Fatal(runErr)
	}

	if err := hw.Close(); err != nil {
		log.Fatal(err)
	}

	fmt.Println("run complete →", *outPath)

	if *runElle {
		fmt.Println("running Elle analysis…")
		if err := invokeElle(ctx, *outPath, *elleTimeout); err != nil {
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				log.Printf("Elle analysis exited with status %d", exitErr.ExitCode())
				os.Exit(exitErr.ExitCode())
			}
			log.Fatal(err)
		}
	}
}

func invokeElle(ctx context.Context, historyPath string, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 2 * time.Minute
	}
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	cmd := exec.CommandContext(runCtx, "clj", "-M:run", historyPath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()

	if err := cmd.Run(); err != nil {
		if runCtx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("elle analysis timed out after %s", timeout)
		}
		if errors.Is(err, exec.ErrNotFound) {
			return fmt.Errorf("clj command not found in PATH: %w", err)
		}
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return exitErr
		}
		return fmt.Errorf("elle analysis failed: %w", err)
	}
	return nil
}

func testDBConnection(dsn string) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		log.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	if err = db.Ping(); err != nil {
		log.Fatalf("Failed to connect to DB: %v", err)
	}

	log.Println("Database connection successful!")
}
