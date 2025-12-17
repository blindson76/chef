package controller

import (
	"context"
	"encoding/json"
	"fmt"
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

func RunLoop(ctx context.Context, rt Runtime, kind string) error {
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
	candVals, err := rt.Store.List(ctx, rt.Key.Candidates(kind))
	if err != nil {
		return
	}
	candidates := []model.CandidateReport{}
	for _, v := range candVals {
		var cr model.CandidateReport
		if json.Unmarshal(v.Data, &cr) == nil {
			candidates = append(candidates, cr)
		}
	}

	healthVals, _ := rt.Store.List(ctx, rt.Key.Health(kind))
	health := map[string]model.HealthStatus{}
	for _, v := range healthVals {
		var hs model.HealthStatus
		if json.Unmarshal(v.Data, &hs) == nil {
			health[hs.NodeID] = hs
		}
	}

	clauses := rt.Cfg.Constraints.Mongo
	if kind == "kafka" {
		clauses = rt.Cfg.Constraints.Kafka
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

	sort.SliceStable(eligible, func(i, j int) bool {
		if eligible[i].Timestamp.Equal(eligible[j].Timestamp) {
			return eligible[i].NodeID < eligible[j].NodeID
		}
		return eligible[i].Timestamp.After(eligible[j].Timestamp)
	})
	chosen := eligible[:3]

	sv, ok, _ := rt.Store.Get(ctx, rt.Key.Spec(kind))
	var spec model.Spec
	if ok {
		_ = json.Unmarshal(sv.Data, &spec)
	}
	if spec.Kind == "" {
		spec = model.Spec{Kind: kind, Epoch: time.Now().Unix(), Revision: 0, CreatedAt: time.Now()}
	}
	if sameMembers(spec, chosen) {
		return
	}

	spec.Revision++
	spec.CreatedAt = time.Now()
	spec.Members = []model.Member{
		{Slot: 1, NodeID: chosen[0].NodeID},
		{Slot: 2, NodeID: chosen[1].NodeID},
		{Slot: 3, NodeID: chosen[2].NodeID},
	}
	b, _ := json.Marshal(spec)
	_, _ = rt.Store.Put(ctx, rt.Key.Spec(kind), b)

	for _, m := range spec.Members {
		ensureType := model.OrderEnsureMongo
		if kind == "kafka" {
			ensureType = model.OrderEnsureKafka
		}
		ensure := model.Order{
			ID:         orderID(kind, m.NodeID, "ensure", spec.Epoch, spec.Revision, m.Slot),
			Kind:       kind,
			Epoch:      spec.Epoch,
			Revision:   spec.Revision,
			TargetNode: m.NodeID,
			Slot:       m.Slot,
			Type:       ensureType,
			CreatedAt:  time.Now(),
		}
		writeOrder(ctx, rt, kind, ensure)

		reg := model.Order{
			ID:         orderID(kind, m.NodeID, "reg", spec.Epoch, spec.Revision, m.Slot),
			Kind:       kind,
			Epoch:      spec.Epoch,
			Revision:   spec.Revision,
			TargetNode: m.NodeID,
			Slot:       m.Slot,
			Type:       model.OrderRegisterSlot,
			CreatedAt:  time.Now(),
		}
		writeOrder(ctx, rt, kind, reg)
	}

	log.Printf("[%s][controller] spec rev=%d members=%v", kind, spec.Revision, []string{chosen[0].NodeID, chosen[1].NodeID, chosen[2].NodeID})
}

func writeOrder(ctx context.Context, rt Runtime, kind string, ord model.Order) {
	b, _ := json.Marshal(ord)
	_, _ = rt.Store.Put(ctx, rt.Key.Order(kind, ord.ID), b)
	_, _ = rt.Store.Put(ctx, rt.Key.OrderPtr(kind, ord.TargetNode, ord.ID), []byte(ord.ID))
}

func sameMembers(spec model.Spec, chosen []model.CandidateReport) bool {
	if len(spec.Members) != 3 {
		return false
	}
	want := []string{chosen[0].NodeID, chosen[1].NodeID, chosen[2].NodeID}
	got := []string{spec.Members[0].NodeID, spec.Members[1].NodeID, spec.Members[2].NodeID}
	sort.Strings(want)
	sort.Strings(got)
	for i := 0; i < 3; i++ {
		if want[i] != got[i] {
			return false
		}
	}
	return true
}

func orderID(kind, node, purpose string, epoch, rev int64, slot int) string {
	return fmt.Sprintf("%s-%s-%s-%d-%d-s%d-%d", kind, node, purpose, epoch, rev, slot, time.Now().UnixNano())
}
