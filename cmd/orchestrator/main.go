package main

import (
    "context"
    "flag"
    "log"
    "os"
    "os/signal"
    "syscall"

    capi "github.com/hashicorp/consul/api"

    "github.com/umitbozkurt/orchestrator/internal/config"
    "github.com/umitbozkurt/orchestrator/internal/runtime"
    consulstore "github.com/umitbozkurt/orchestrator/internal/store/consul"
)

func main() {
    cfgPath := flag.String("config", "config.yaml", "config file path")
    flag.Parse()

    cfg, err := config.Load(*cfgPath)
    if err != nil {
        log.Fatalf("config load: %v", err)
    }

    // Consul client (for service registration helper) and store
    ccfg := capi.DefaultConfig()
    ccfg.Address = cfg.Cluster.Consul.Address
    if cfg.Cluster.Consul.Token != "" {
        ccfg.Token = cfg.Cluster.Consul.Token
    }
    c, err := capi.NewClient(ccfg)
    if err != nil {
        log.Fatalf("consul client: %v", err)
    }

    st, err := consulstore.New(consulstore.Config{
        Address: cfg.Cluster.Consul.Address,
        Token: cfg.Cluster.Consul.Token,
    })
    if err != nil {
        log.Fatalf("consul store: %v", err)
    }

    ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer cancel()

    if err := runtime.Run(ctx, runtime.Runtime{Cfg: cfg, Store: st, Consul: c}); err != nil {
        log.Printf("exit: %v", err)
        os.Exit(1)
    }
}
