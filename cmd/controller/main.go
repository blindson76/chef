package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/umitbozkurt/orchestrator/internal/config"
	"github.com/umitbozkurt/orchestrator/internal/controller"
	"github.com/umitbozkurt/orchestrator/internal/leader"
	"github.com/umitbozkurt/orchestrator/internal/paths"
	"github.com/umitbozkurt/orchestrator/internal/store/consul"
)

func main() {
	var cfgPath string
	flag.StringVar(&cfgPath, "config", "config.yaml", "config file path")
	flag.Parse()

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatal(err)
	}

	st, err := consul.New(consul.Config{Address: cfg.Cluster.Consul.Address, Token: cfg.Cluster.Consul.Token})
	if err != nil {
		log.Fatal(err)
	}
	key := paths.Keyspace{Root: "orch/" + cfg.Cluster.Name}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	rt := controller.Runtime{Cfg: cfg, Store: st, Key: key}

	// run 2 independent leader loops (kafka + mongo)
	go func() {
		lock := key.LeaderLock("kafka")
		_ = leader.RunWithLock(ctx, st, lock, cfg.LeaderTTL(), func(lctx context.Context) error {
			return controller.RunLoop(lctx, rt, "kafka")
		})
	}()

	lock := key.LeaderLock("mongo")
	_ = leader.RunWithLock(ctx, st, lock, cfg.LeaderTTL(), func(lctx context.Context) error {
		return controller.RunLoop(lctx, rt, "mongo")
	})
}
