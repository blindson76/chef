package controller

import (
    "context"
    "crypto/sha1"
    "encoding/hex"
    "encoding/json"
    "log"
    "sort"
    "time"

    "github.com/umitbozkurt/orchestrator/internal/config"
    "github.com/umitbozkurt/orchestrator/internal/constraints"
    "github.com/umitbozkurt/orchestrator/internal/model"
    "github.com/umitbozkurt/orchestrator/internal/paths"
    "github.com/umitbozkurt/orchestrator/internal/store"
)

type Runtime struct {
    Cfg   *config.Config
    Store store.Store
    Key   paths.Keyspace
}

func RunLeader(ctx context.Context, rt Runtime, kind string) error {
    tick := time.NewTicker(rt.Cfg.ReconcileEvery())
    defer tick.Stop()

    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-tick.C:
            reconcileOnce(ctx, rt, kind)
        }
    }
}

func reconcileOnce(ctx context.Context, rt Runtime, kind string) {
    // list candidates
    candVals, err := rt.Store.List(ctx, rt.Key.Candidates(kind))
    if err != nil { return }

    candidates := []model.CandidateReport{}
    for _, v := range candVals {
        var cr model.CandidateReport
        if err := json.Unmarshal(v.Data, &cr); err == nil {
            candidates = append(candidates, cr)
        }
    }

    // list health
    healthVals, _ := rt.Store.List(ctx, rt.Key.Health(kind))
    health := map[string]model.HealthStatus{}
    for _, v := range healthVals {
        var hs model.HealthStatus
        if err := json.Unmarshal(v.Data, &hs); err == nil {
            health[hs.NodeID] = hs
        }
    }

    // filter eligible
    var clauses []config.ConstraintClause
    if kind == "kafka" {
        clauses = rt.Cfg.Constraints.Kafka
    } else {
        clauses = rt.Cfg.Constraints.Mongo
    }
    eligible := []model.CandidateReport{}
    for _, c := range candidates {
        if c.Maintenance.Effective {
            continue
        }
        hs, ok := health[c.NodeID]
        if !ok || !hs.Healthy {
            continue
        }
        if res := constraints.Evaluate(c.Tags, clauses); !res.OK {
            continue
        }
        eligible = append(eligible, c)
    }

    if len(eligible) < 3 {
        log.Printf("[%s][controller] insufficient eligible nodes: %d", kind, len(eligible))
        return
    }

    // pick most recent 3 (by Timestamp then NodeID)
    sort.SliceStable(eligible, func(i, j int) bool {
        if eligible[i].Timestamp.Equal(eligible[j].Timestamp) {
            return eligible[i].NodeID < eligible[j].NodeID
        }
        return eligible[i].Timestamp.After(eligible[j].Timestamp)
    })
    chosen := eligible[:3]

    // load existing spec
    existingV, ok, _ := rt.Store.Get(ctx, rt.Key.Spec(kind))
    var spec model.Spec
    if ok {
        _ = json.Unmarshal(existingV.Data, &spec)
    }
    if spec.Kind == "" {
        spec = model.Spec{Kind: kind, Epoch: time.Now().Unix(), Revision: 0, CreatedAt: time.Now()}
    }

    // if members unchanged and still healthy, keep
    if sameMembers(spec, chosen) {
        return
    }

    spec.Revision++
    spec.Members = []model.Member{
        {Slot: 1, NodeID: chosen[0].NodeID, Address: rt.Cfg.Cluster.AdvertiseIP},
        {Slot: 2, NodeID: chosen[1].NodeID, Address: rt.Cfg.Cluster.AdvertiseIP},
        {Slot: 3, NodeID: chosen[2].NodeID, Address: rt.Cfg.Cluster.AdvertiseIP},
    }
    spec.ConstraintsHash = hashConstraints(clauses)
    spec.CreatedAt = time.Now()

    b, _ := json.Marshal(spec)
    _, _ = rt.Store.Put(ctx, rt.Key.Spec(kind), b)

    // create orders for each chosen node:
    for _, m := range spec.Members {
        ensureType := model.OrderEnsureKafka
        if kind == "mongo" {
            ensureType = model.OrderEnsureMongo
        }
        ensureOrder := model.Order{
            ID: newOrderID(kind, m.NodeID, "ensure", spec.Epoch, spec.Revision, m.Slot),
            Kind: kind,
            Epoch: spec.Epoch,
            Revision: spec.Revision,
            TargetNode: m.NodeID,
            Slot: m.Slot,
            Type: ensureType,
            Payload: map[string]any{},
            CreatedAt: time.Now(),
        }
        writeOrder(ctx, rt, kind, ensureOrder)

        regOrder := model.Order{
            ID: newOrderID(kind, m.NodeID, "reg", spec.Epoch, spec.Revision, m.Slot),
            Kind: kind,
            Epoch: spec.Epoch,
            Revision: spec.Revision,
            TargetNode: m.NodeID,
            Slot: m.Slot,
            Type: model.OrderRegisterSlot,
            CreatedAt: time.Now(),
        }
        writeOrder(ctx, rt, kind, regOrder)
    }

    log.Printf("[%s][controller] published spec rev=%d members=%v", kind, spec.Revision, memberIDs(spec))
}

func writeOrder(ctx context.Context, rt Runtime, kind string, ord model.Order) {
    b, _ := json.Marshal(ord)
    _, _ = rt.Store.Put(ctx, rt.Key.Order(kind, ord.ID), b)
    // pointer by node
    _, _ = rt.Store.Put(ctx, rt.Key.OrderPtr(kind, ord.TargetNode, ord.ID), []byte(ord.ID))
}

func memberIDs(spec model.Spec) []string {
    out := make([]string, 0, len(spec.Members))
    for _, m := range spec.Members {
        out = append(out, m.NodeID)
    }
    return out
}

func sameMembers(spec model.Spec, chosen []model.CandidateReport) bool {
    if len(spec.Members) != 3 {
        return false
    }
    want := []string{chosen[0].NodeID, chosen[1].NodeID, chosen[2].NodeID}
    got := []string{spec.Members[0].NodeID, spec.Members[1].NodeID, spec.Members[2].NodeID}
    sort.Strings(want); sort.Strings(got)
    for i := 0; i < 3; i++ {
        if want[i] != got[i] {
            return false
        }
    }
    return true
}

func hashConstraints(clauses []config.ConstraintClause) string {
    h := sha1.New()
    for _, c := range clauses {
        h.Write([]byte(c.String()))
        h.Write([]byte{0})
    }
    return hex.EncodeToString(h.Sum(nil))
}

func newOrderID(kind, node, purpose string, epoch, rev int64, slot int) string {
    return kind + "-" + node + "-" + purpose + "-" + itoa(epoch) + "-" + itoa(rev) + "-s" + itoa(int64(slot)) + "-" + itoa(time.Now().UnixNano())
}

func itoa(x int64) string {
    // no strconv to keep tiny; this is fine
    if x == 0 { return "0" }
    neg := x < 0
    if neg { x = -x }
    buf := make([]byte, 0, 24)
    for x > 0 {
        d := x % 10
        buf = append(buf, byte('0'+d))
        x /= 10
    }
    if neg { buf = append(buf, '-') }
    // reverse
    for i, j := 0, len(buf)-1; i < j; i, j = i+1, j-1 {
        buf[i], buf[j] = buf[j], buf[i]
    }
    return string(buf)
}
