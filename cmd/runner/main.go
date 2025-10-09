package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/hjungwoo01/elle-runner/internal/config"
	"github.com/hjungwoo01/elle-runner/internal/driver"
	"github.com/hjungwoo01/elle-runner/internal/history"
	"github.com/hjungwoo01/elle-runner/internal/runner"
)

func main() {
	cfgPath := flag.String("config", "workloads/example.yaml", "YAML config")
	initSchema := flag.Bool("init", false, "init schema and exit")
	outPath := flag.String("out", "history.edn", "Elle history output path")
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
	defer hw.Close()

	env := &runner.Env{
		Drv: drv, Cfg: cfg, Hist: hw, Start: time.Now(),
	}
	if err := runner.Run(ctx, env); err != nil {
		log.Fatal(err)
	}
	fmt.Println("run complete →", *outPath)
}
