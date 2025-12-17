package runtime

import (
    "context"
    "log"
    "sync"
    "time"

    capi "github.com/hashicorp/consul/api"

    "github.com/umitbozkurt/orchestrator/internal/agent"
    "github.com/umitbozkurt/orchestrator/internal/config"
    "github.com/umitbozkurt/orchestrator/internal/controller"
    "github.com/umitbozkurt/orchestrator/internal/logapi"
    "github.com/umitbozkurt/orchestrator/internal/paths"
    "github.com/umitbozkurt/orchestrator/internal/procman"
    "github.com/umitbozkurt/orchestrator/internal/store"
)

type Runtime struct {
    Cfg *config.Config
    Store store.Store
    Consul *capi.Client
}

func Run(ctx context.Context, rt Runtime) error {
    key := paths.Keyspace{Root: "orchestrator/" + rt.Cfg.Cluster.Name}

    proc := procman.New(rt.Cfg.Cluster.LogRoot)

    // Start log API
    srv := logapi.New(proc, rt.Cfg.Cluster.HTTP.AuthToken)
    httpErr := make(chan error, 1)
    go func() {
        log.Printf("[http] listening %s", rt.Cfg.Cluster.HTTP.Listen)
        httpErr <- ListenAndServe(ctx, rt.Cfg.Cluster.HTTP.Listen, srv.Handler())
    }()

    // Start agent loops
    _ = agent.Run(ctx, agent.Runtime{
        Cfg: rt.Cfg, Store: rt.Store, ConsulClient: rt.Consul, Key: key, Proc: proc,
    })

    // Leader election + controller start/stop
    go leaderLoop(ctx, rt, key)

    select {
    case <-ctx.Done():
        return ctx.Err()
    case err := <-httpErr:
        return err
    }
}

func leaderLoop(ctx context.Context, rt Runtime, key paths.Keyspace) {
    // two independent controllers (kafka, mongo), share same lock per kind.
    for _, kind := range []string{"kafka", "mongo"} {
        k := kind
        go func() {
            var mu sync.Mutex
            var cancel context.CancelFunc
            for {
                select {
                case <-ctx.Done():
                    mu.Lock()
                    if cancel != nil { cancel() }
                    mu.Unlock()
                    return
                default:
                }

                lh, err := rt.Store.Lock(ctx, key.LeaderLock(k), rt.Cfg.LeaderTTL())
                if err != nil || !lh.IsHeld() {
                    time.Sleep(1 * time.Second)
                    continue
                }
                log.Printf("[%s][leader] acquired", k)

                cctx, ccancel := context.WithCancel(ctx)
                mu.Lock()
                cancel = ccancel
                mu.Unlock()

                // keepalive in background
                kaErr := make(chan error, 1)
                go func() { kaErr <- lh.KeepAlive(cctx) }()

                // run controller loop
                ctrlErr := make(chan error, 1)
                go func() {
                    ctrlErr <- controller.RunLeader(cctx, controller.Runtime{
                        Cfg: rt.Cfg, Store: rt.Store, Key: key,
                    }, k)
                }()

                select {
                case <-ctx.Done():
                case err := <-kaErr:
                    log.Printf("[%s][leader] lost leadership: %v", k, err)
                case err := <-ctrlErr:
                    log.Printf("[%s][controller] stopped: %v", k, err)
                }

                _ = lh.Unlock(context.Background())
                mu.Lock()
                if cancel != nil { cancel(); cancel = nil }
                mu.Unlock()
                time.Sleep(1 * time.Second)
            }
        }()
    }
}
