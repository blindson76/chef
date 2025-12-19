package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	capi "github.com/hashicorp/consul/api"

	"github.com/umitbozkurt/orchestrator/internal/config"
	"github.com/umitbozkurt/orchestrator/internal/logapi"
	"github.com/umitbozkurt/orchestrator/internal/paths"
	"github.com/umitbozkurt/orchestrator/internal/procman"
	"github.com/umitbozkurt/orchestrator/internal/store/consul"
	"github.com/umitbozkurt/orchestrator/internal/worker"
)

func main() {
	var cfgPath string
	flag.StringVar(&cfgPath, "config", "config.yaml", "config file path")
	flag.Parse()

	cfg, err := config.Load(cfgPath)
	if err != nil {
		log.Fatal(err)
	}

	st, err := consul.New(consul.Config{Address: cfg.Cluster.Consul.Address})
	if err != nil {
		log.Fatal(err)
	}

	ccfg := capi.DefaultConfig()
	ccfg.Address = cfg.Cluster.Consul.Address
	ccfg.Token = cfg.Cluster.Consul.Token
	consulClient, err := capi.NewClient(ccfg)
	if err != nil {
		log.Fatal(err)
	}

	key := paths.Keyspace{Root: "orch/" + cfg.Cluster.Name}
	pm := procman.New(cfg.Cluster.LogRoot)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	rt := worker.Runtime{
		Cfg: cfg, Store: st, Consul: consulClient, Key: key, Proc: pm,
		LogApiBase: "http://" + cfg.Cluster.AdvertiseIP + ":" + lastPort(cfg.Cluster.HTTP.Listen),
	}
	_ = worker.Run(ctx, rt)

	srv := &http.Server{Addr: cfg.Cluster.HTTP.Listen, Handler: logapi.New(pm, cfg.Cluster.HTTP.AuthToken).Handler()}
	go func() {
		log.Printf("[worker] log api listening on %s", cfg.Cluster.HTTP.Listen)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[worker] http error: %v", err)
			cancel()
		}
	}()

	<-ctx.Done()
	_ = srv.Shutdown(context.Background())
}

func lastPort(listen string) string {
	// handles "0.0.0.0:18080" or ":18080"
	for i := len(listen) - 1; i >= 0; i-- {
		if listen[i] == ':' {
			return listen[i+1:]
		}
	}
	return "18080"
}
