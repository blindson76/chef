package agent

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "math/rand"
    "net"
    "os"
    "time"

    "github.com/hashicorp/consul/api"

    "github.com/umitbozkurt/orchestrator/internal/config"
    "github.com/umitbozkurt/orchestrator/internal/constraints"
    "github.com/umitbozkurt/orchestrator/internal/kafkahelper"
    "github.com/umitbozkurt/orchestrator/internal/maintenance"
    "github.com/umitbozkurt/orchestrator/internal/model"
    "github.com/umitbozkurt/orchestrator/internal/mongohelper"
    "github.com/umitbozkurt/orchestrator/internal/paths"
    "github.com/umitbozkurt/orchestrator/internal/procman"
    "github.com/umitbozkurt/orchestrator/internal/store"
    "github.com/umitbozkurt/orchestrator/internal/util"
    "github.com/umitbozkurt/orchestrator/internal/consulreg"
)

type Runtime struct {
    Cfg *config.Config
    Store store.Store
    ConsulClient *api.Client
    Key paths.Keyspace
    Proc *procman.Manager
}

func Run(ctx context.Context, rt Runtime) error {
    // Candidate + health reporters
    go candidateLoop(ctx, rt, "kafka")
    go candidateLoop(ctx, rt, "mongo")
    go healthLoop(ctx, rt, "kafka")
    go healthLoop(ctx, rt, "mongo")

    // Order executor loops
    go orderLoop(ctx, rt, "kafka")
    go orderLoop(ctx, rt, "mongo")

    return nil
}

func candidateLoop(ctx context.Context, rt Runtime, kind string) {
    ttl := rt.Cfg.CandidateTTL()
    key := rt.Key.Candidate(kind, rt.Cfg.Cluster.NodeID)

    // Lease key, renew periodically. Recreate if lost.
    var lease store.LeaseHandle
    renewTicker := time.NewTicker(ttl / 2)
    defer renewTicker.Stop()

    for {
        select {
        case <-ctx.Done():
            if lease != nil {
                _ = rt.Store.Release(context.Background(), lease)
            }
            return
        default:
        }

        // compute effective maintenance
        remoteKey := rt.Key.MaintenanceNode(kind, rt.Cfg.Cluster.NodeID)
        rm, rmOK, _ := maintenance.ReadRemote(ctx, rt.Store, remoteKey)
        eff := maintenance.Effective(rt.Cfg.Maintenance.LocalEnabled, rt.Cfg.Maintenance.LocalReason, rm, rmOK)

        rep := model.CandidateReport{
            NodeID: rt.Cfg.Cluster.NodeID,
            Timestamp: time.Now(),
            BootTime: bootTimeApprox(),
            Tags: rt.Cfg.Node.Tags,
            Maintenance: eff,
            Offline: model.OfflineReport{},
        }

        // Offline inspect
        if kind == "kafka" && rt.Cfg.Kafka.Enabled {
            to := parseTimeout(rt.Cfg.Kafka.InspectTimeout, 2*time.Second)
            cctx, cancel := context.WithTimeout(ctx, to)
            d := kafkahelper.Dirs{LogDir: util.ExpandNode(rt.Cfg.Kafka.LogDir, rt.Cfg.Cluster.NodeID), MetaDir: util.ExpandNode(rt.Cfg.Kafka.MetaLogDir, rt.Cfg.Cluster.NodeID)}
            st, err := kafkahelper.InspectOffline(cctx, d)
            cancel()
            if err != nil {
                rep.Offline.Warnings = append(rep.Offline.Warnings, "kafka offline inspect failed: "+err.Error())
            } else {
                rep.Offline.Kafka = st
            }
        }
        if kind == "mongo" && rt.Cfg.Mongo.Enabled {
            to := parseTimeout(rt.Cfg.Mongo.InspectTimeout, 2*time.Second)
            cctx, cancel := context.WithTimeout(ctx, to)
            st, err := mongohelper.InspectOffline(cctx, util.ExpandNode(rt.Cfg.Mongo.DBPath, rt.Cfg.Cluster.NodeID))
            cancel()
            if err != nil {
                rep.Offline.Warnings = append(rep.Offline.Warnings, "mongo offline inspect failed: "+err.Error())
            } else {
                rep.Offline.Mongo = st
            }
        }

        b, _ := json.Marshal(rep)

        if lease == nil {
            lh, err := rt.Store.PutLeased(ctx, key, b, ttl)
            if err != nil {
                log.Printf("[%s][agent] candidate lease put failed: %v", kind, err)
                time.Sleep(jitter(500*time.Millisecond, 1500*time.Millisecond))
                continue
            }
            lease = lh
        } else {
            // refresh value (best-effort)
            _, _ = rt.Store.Put(ctx, key, b)
            _ = rt.Store.Renew(ctx, lease, ttl)
        }

        select {
        case <-ctx.Done():
            return
        case <-renewTicker.C:
        }
    }
}

func healthLoop(ctx context.Context, rt Runtime, kind string) {
    ttl := rt.Cfg.HealthTTL()
    key := rt.Key.HealthKey(kind, rt.Cfg.Cluster.NodeID)
    var lease store.LeaseHandle

    tick := time.NewTicker(ttl/2)
    defer tick.Stop()

    for {
        select {
        case <-ctx.Done():
            if lease != nil { _ = rt.Store.Release(context.Background(), lease) }
            return
        default:
        }

        hs := model.HealthStatus{
            NodeID: rt.Cfg.Cluster.NodeID,
            Timestamp: time.Now(),
            Healthy: true,
            Detail: "ok",
        }
        // basic check: can resolve own advertise IP if set
        if rt.Cfg.Cluster.AdvertiseIP != "" {
            if ip := net.ParseIP(rt.Cfg.Cluster.AdvertiseIP); ip == nil {
                hs.Healthy = false
                hs.Detail = "invalid advertiseIp"
            }
        }
        b, _ := json.Marshal(hs)
        if lease == nil {
            lh, err := rt.Store.PutLeased(ctx, key, b, ttl)
            if err != nil {
                log.Printf("[%s][agent] health lease put failed: %v", kind, err)
                time.Sleep(jitter(500*time.Millisecond, 1500*time.Millisecond))
                continue
            }
            lease = lh
        } else {
            _, _ = rt.Store.Put(ctx, key, b)
            _ = rt.Store.Renew(ctx, lease, ttl)
        }

        select {
        case <-ctx.Done():
            return
        case <-tick.C:
        }
    }
}

func orderLoop(ctx context.Context, rt Runtime, kind string) {
    prefix := rt.Key.OrdersByNode(kind, rt.Cfg.Cluster.NodeID)

    // simple polling list loop; for production use WatchPrefix + diff.
    tick := time.NewTicker(2 * time.Second)
    defer tick.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-tick.C:
            vals, err := rt.Store.List(ctx, prefix)
            if err != nil {
                continue
            }
            for _, v := range vals {
                orderID := lastSegment(v.Key)
                ordVal, ok, err := rt.Store.Get(ctx, rt.Key.Order(kind, orderID))
                if err != nil || !ok {
                    continue
                }
                var ord model.Order
                if err := json.Unmarshal(ordVal.Data, &ord); err != nil {
                    continue
                }
                // execute order (idempotent)
                _ = executeOrder(ctx, rt, kind, ord)
            }
        }
    }
}

func executeOrder(ctx context.Context, rt Runtime, kind string, ord model.Order) error {
    // Maintenance enforcement
    remoteKey := rt.Key.MaintenanceNode(kind, rt.Cfg.Cluster.NodeID)
    rm, rmOK, _ := maintenance.ReadRemote(ctx, rt.Store, remoteKey)
    eff := maintenance.Effective(rt.Cfg.Maintenance.LocalEnabled, rt.Cfg.Maintenance.LocalReason, rm, rmOK)
    if eff.Effective {
        return writeStatus(ctx, rt, kind, ord.ID, model.OrderStatus{
            OrderID: ord.ID, NodeID: rt.Cfg.Cluster.NodeID, State: model.OrderStatusRejected,
            Reason: "MaintenanceMode", Detail: eff.Reason, Timestamp: time.Now(),
        })
    }

    // Constraints enforcement
    var clauses []config.ConstraintClause
    if kind == "kafka" {
        clauses = rt.Cfg.Constraints.Kafka
    } else {
        clauses = rt.Cfg.Constraints.Mongo
    }
    if res := constraints.Evaluate(rt.Cfg.Node.Tags, clauses); !res.OK {
        return writeStatus(ctx, rt, kind, ord.ID, model.OrderStatus{
            OrderID: ord.ID, NodeID: rt.Cfg.Cluster.NodeID, State: model.OrderStatusRejected,
            Reason: "ConstraintViolation", Detail: res.Reason, Timestamp: time.Now(),
        })
    }

    _ = writeStatus(ctx, rt, kind, ord.ID, model.OrderStatus{
        OrderID: ord.ID, NodeID: rt.Cfg.Cluster.NodeID, State: model.OrderStatusRunning,
        Timestamp: time.Now(),
    })

    switch ord.Type {
    case model.OrderEnsureKafka:
        return ensureKafka(ctx, rt, ord)
    case model.OrderEnsureMongo:
        return ensureMongo(ctx, rt, ord)
    case model.OrderStopKafka:
        return stopService(ctx, rt, "kafka", ord)
    case model.OrderStopMongo:
        return stopService(ctx, rt, "mongo", ord)
    case model.OrderRegisterSlot:
        return registerSlot(ctx, rt, kind, ord)
    case model.OrderDeregisterSlot:
        return deregisterSlot(ctx, rt, kind, ord)
    default:
        return writeStatus(ctx, rt, kind, ord.ID, model.OrderStatus{
            OrderID: ord.ID, NodeID: rt.Cfg.Cluster.NodeID, State: model.OrderStatusFailed,
            Reason: "UnknownOrder", Detail: string(ord.Type), Timestamp: time.Now(),
        })
    }
}

func ensureKafka(ctx context.Context, rt Runtime, ord model.Order) error {
    if !rt.Cfg.Kafka.Enabled {
        return succeeded(ctx, rt, "kafka", ord.ID, "kafka disabled")
    }
    instanceID := fmt.Sprintf("kafka-slot%d-epoch%d-rev%d", ord.Slot, ord.Epoch, ord.Revision)
    exe := rt.Cfg.Kafka.Binary
    args := append([]string{}, rt.Cfg.Kafka.StartArgs...)
    // allow payload override
    if v, ok := ord.Payload["args"].([]any); ok {
        args = []string{}
        for _, a := range v {
            if s, ok := a.(string); ok { args = append(args, s) }
        }
    }
    // Start process (idempotent: if already started, ok)
    if rt.Proc.Get(instanceID) == nil {
        _, err := rt.Proc.Start(ctx, "kafka", instanceID, exe, args, "")
        if err != nil {
            return failed(ctx, rt, "kafka", ord.ID, "StartFailed", err.Error())
        }
        publishProcMeta(ctx, rt, "kafka", instanceID, ord)
    }
    return succeeded(ctx, rt, "kafka", ord.ID, "started/ensured")
}

func ensureMongo(ctx context.Context, rt Runtime, ord model.Order) error {
    if !rt.Cfg.Mongo.Enabled {
        return succeeded(ctx, rt, "mongo", ord.ID, "mongo disabled")
    }
    instanceID := fmt.Sprintf("mongo-slot%d-epoch%d-rev%d", ord.Slot, ord.Epoch, ord.Revision)
    exe := rt.Cfg.Mongo.Binary
    args := append([]string{}, rt.Cfg.Mongo.StartArgs...)
    if rt.Proc.Get(instanceID) == nil {
        _, err := rt.Proc.Start(ctx, "mongo", instanceID, exe, args, "")
        if err != nil {
            return failed(ctx, rt, "mongo", ord.ID, "StartFailed", err.Error())
        }
        publishProcMeta(ctx, rt, "mongo", instanceID, ord)
    }
    return succeeded(ctx, rt, "mongo", ord.ID, "started/ensured")
}

func stopService(ctx context.Context, rt Runtime, service string, ord model.Order) error {
    instanceID := fmt.Sprintf("%s-slot%d-epoch%d-rev%d", service, ord.Slot, ord.Epoch, ord.Revision)
    mode := procman.StopTerminate
    if service == "kafka" && rt.Cfg.Kafka.StopMode == "interrupt" {
        mode = procman.StopInterrupt
    }
    if service == "mongo" && rt.Cfg.Mongo.StopMode == "interrupt" {
        mode = procman.StopInterrupt
    }
    if rt.Proc.Get(instanceID) != nil {
        _ = rt.Proc.Stop(ctx, instanceID, mode)
    }
    return succeeded(ctx, rt, service, ord.ID, "stopped (best-effort)")
}

func registerSlot(ctx context.Context, rt Runtime, kind string, ord model.Order) error {
    // Register service: <prefix>-<slot>
    prefix := rt.Cfg.Mongo.SlotServicePrefix
    port := rt.Cfg.Mongo.Port
    if kind == "kafka" {
        prefix = rt.Cfg.Kafka.SlotServicePrefix
        port = 9092
    }
    name := fmt.Sprintf("%s-%d", prefix, ord.Slot)
    sid := fmt.Sprintf("%s-%s-epoch%d", name, rt.Cfg.Cluster.NodeID, ord.Epoch)
    reg := consulreg.New(rt.ConsulClient)
    addr := rt.Cfg.Cluster.AdvertiseIP
    if addr == "" {
        addr = guessIP()
    }
    err := reg.Register(ctx, consulreg.Service{
        Name: name, ID: sid, Address: addr, Port: port,
        Tags: []string{ "kind="+kind, fmt.Sprintf("slot=%d", ord.Slot), fmt.Sprintf("epoch=%d", ord.Epoch)},
        TCPCheck: true,
    })
    if err != nil {
        return failed(ctx, rt, kind, ord.ID, "ServiceRegisterFailed", err.Error())
    }
    return succeeded(ctx, rt, kind, ord.ID, "service registered: "+name)
}

func deregisterSlot(ctx context.Context, rt Runtime, kind string, ord model.Order) error {
    prefix := rt.Cfg.Mongo.SlotServicePrefix
    if kind == "kafka" {
        prefix = rt.Cfg.Kafka.SlotServicePrefix
    }
    name := fmt.Sprintf("%s-%d", prefix, ord.Slot)
    sid := fmt.Sprintf("%s-%s-epoch%d", name, rt.Cfg.Cluster.NodeID, ord.Epoch)
    reg := consulreg.New(rt.ConsulClient)
    _ = reg.Deregister(ctx, sid)
    return succeeded(ctx, rt, kind, ord.ID, "service deregistered: "+name)
}

func publishProcMeta(ctx context.Context, rt Runtime, kind, instanceID string, ord model.Order) {
    meta := map[string]any{
        "instanceId": instanceID,
        "nodeId": rt.Cfg.Cluster.NodeID,
        "service": kind,
        "slot": ord.Slot,
        "epoch": ord.Epoch,
        "revision": ord.Revision,
        "startTime": time.Now().UTC().Format(time.RFC3339),
        "logApiBase": "http://" + rt.Cfg.Cluster.AdvertiseIP + rt.Cfg.Cluster.HTTP.Listen[stringsIndex(rt.Cfg.Cluster.HTTP.Listen, ":"):],
    }
    b, _ := json.Marshal(meta)
    _, _ = rt.Store.Put(ctx, rt.Key.ProcMeta(kind, instanceID), b)
}

func writeStatus(ctx context.Context, rt Runtime, kind, orderID string, st model.OrderStatus) error {
    b, _ := json.Marshal(st)
    _, err := rt.Store.Put(ctx, rt.Key.OrderStatus(kind, orderID, rt.Cfg.Cluster.NodeID), b)
    return err
}

func succeeded(ctx context.Context, rt Runtime, kind, orderID, detail string) error {
    return writeStatus(ctx, rt, kind, orderID, model.OrderStatus{
        OrderID: orderID, NodeID: rt.Cfg.Cluster.NodeID,
        State: model.OrderStatusSucceeded, Detail: detail, Timestamp: time.Now(),
    })
}

func failed(ctx context.Context, rt Runtime, kind, orderID, reason, detail string) error {
    return writeStatus(ctx, rt, kind, orderID, model.OrderStatus{
        OrderID: orderID, NodeID: rt.Cfg.Cluster.NodeID,
        State: model.OrderStatusFailed, Reason: reason, Detail: detail, Timestamp: time.Now(),
    })
}

func jitter(min, max time.Duration) time.Duration {
    if max <= min { return min }
    d := min + time.Duration(rand.Int63n(int64(max-min)))
    return d
}

func parseTimeout(s string, def time.Duration) time.Duration {
    if s == "" { return def }
    d, err := time.ParseDuration(s)
    if err != nil { return def }
    return d
}

func lastSegment(k string) string {
    i := len(k) - 1
    for i >= 0 && k[i] != '/' { i-- }
    return k[i+1:]
}

func bootTimeApprox() time.Time {
    // best-effort: use process start time as proxy
    fi, err := os.Stat(os.Args[0])
    if err == nil {
        _ = fi
    }
    return time.Now().Add(-1 * time.Hour)
}

func guessIP() string {
    // best-effort: pick first non-loopback ipv4
    ifaces, _ := net.Interfaces()
    for _, iface := range ifaces {
        addrs, _ := iface.Addrs()
        for _, a := range addrs {
            ipNet, ok := a.(*net.IPNet)
            if !ok { continue }
            ip := ipNet.IP.To4()
            if ip == nil { continue }
            if ip.IsLoopback() { continue }
            return ip.String()
        }
    }
    return "127.0.0.1"
}

func stringsIndex(s, sub string) int {
    for i := 0; i+len(sub) <= len(s); i++ {
        if s[i:i+len(sub)] == sub {
            return i
        }
    }
    return -1
}
