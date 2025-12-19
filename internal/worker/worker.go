package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strings"
	"time"

	capi "github.com/hashicorp/consul/api"

	"github.com/umitbozkurt/orchestrator/internal/config"
	"github.com/umitbozkurt/orchestrator/internal/constraints"
	"github.com/umitbozkurt/orchestrator/internal/consulreg"
	"github.com/umitbozkurt/orchestrator/internal/kafkahelper"
	"github.com/umitbozkurt/orchestrator/internal/maintenance"
	"github.com/umitbozkurt/orchestrator/internal/model"
	"github.com/umitbozkurt/orchestrator/internal/mongohelper"
	"github.com/umitbozkurt/orchestrator/internal/paths"
	"github.com/umitbozkurt/orchestrator/internal/procman"
	"github.com/umitbozkurt/orchestrator/internal/store"
	"github.com/umitbozkurt/orchestrator/internal/util"
)

type Runtime struct {
	Cfg        *config.Config
	Store      store.Store
	Consul     *capi.Client
	Key        paths.Keyspace
	Proc       *procman.Manager
	LogApiBase string
}

func Run(ctx context.Context, rt Runtime) error {
	go candidateLoop(ctx, rt, "kafka")
	go candidateLoop(ctx, rt, "mongo")
	go healthLoop(ctx, rt, "kafka")
	go healthLoop(ctx, rt, "mongo")
	go orderLoop(ctx, rt, "kafka")
	go orderLoop(ctx, rt, "mongo")
	return nil
}

func candidateLoop(ctx context.Context, rt Runtime, kind string) {
	ttl := rt.Cfg.CandidateTTL()
	key := rt.Key.Candidate(kind, rt.Cfg.Cluster.NodeID)
	var lease store.LeaseHandle
	tick := time.NewTicker(ttl / 2)
	defer tick.Stop()

	for {
		remoteKey := rt.Key.MaintenanceNode(kind, rt.Cfg.Cluster.NodeID)
		rm, rmOK, _ := maintenance.ReadRemote(ctx, rt.Store, remoteKey)
		eff := maintenance.Effective(rt.Cfg.Maintenance.LocalEnabled, rt.Cfg.Maintenance.LocalReason, rm, rmOK)

		rep := model.CandidateReport{
			NodeID:      rt.Cfg.Cluster.NodeID,
			Timestamp:   time.Now(),
			BootTime:    time.Now().Add(-1 * time.Hour),
			Tags:        rt.Cfg.Node.Tags,
			Maintenance: eff,
			Offline:     model.OfflineReport{},
		}

		if kind == "kafka" && rt.Cfg.Kafka.Enabled {
			d := kafkahelper.Dirs{
				LogDir:  util.ExpandNode(rt.Cfg.Kafka.LogDir, rt.Cfg.Cluster.NodeID),
				MetaDir: util.ExpandNode(rt.Cfg.Kafka.MetaLogDir, rt.Cfg.Cluster.NodeID),
			}
			st, err := kafkahelper.InspectOffline(ctx, d)
			if err != nil {
				rep.Offline.Warnings = append(rep.Offline.Warnings, err.Error())
			} else {
				rep.Offline.Kafka = st
			}
		}
		if kind == "mongo" && rt.Cfg.Mongo.Enabled {
			st, err := mongohelper.InspectOffline(ctx, util.ExpandNode(rt.Cfg.Mongo.DBPath, rt.Cfg.Cluster.NodeID))
			if err != nil {
				rep.Offline.Warnings = append(rep.Offline.Warnings, err.Error())
			} else {
				rep.Offline.Mongo = st
			}
		}

		b, _ := json.Marshal(rep)
		if lease == nil {
			lh, err := rt.Store.PutLeased(ctx, key, b, ttl)
			if err != nil {
				log.Printf("[%s][worker] candidate put failed: %v", kind, err)
				time.Sleep(jitter())
				goto wait
			}
			lease = lh
		} else {
			_, _ = rt.Store.Put(ctx, key, b)
			_ = rt.Store.Renew(ctx, lease, ttl)
		}

	wait:
		select {
		case <-ctx.Done():
			if lease != nil {
				_ = rt.Store.Release(context.Background(), lease)
			}
			return
		case <-tick.C:
		}
	}
}

func healthLoop(ctx context.Context, rt Runtime, kind string) {
	ttl := rt.Cfg.HealthTTL()
	key := rt.Key.HealthKey(kind, rt.Cfg.Cluster.NodeID)
	var lease store.LeaseHandle
	tick := time.NewTicker(ttl / 2)
	defer tick.Stop()

	for {
		hs := model.HealthStatus{NodeID: rt.Cfg.Cluster.NodeID, Timestamp: time.Now(), Healthy: true, Detail: "ok"}
		b, _ := json.Marshal(hs)
		if lease == nil {
			lh, err := rt.Store.PutLeased(ctx, key, b, ttl)
			if err != nil {
				time.Sleep(jitter())
				goto wait
			}
			lease = lh
		} else {
			_, _ = rt.Store.Put(ctx, key, b)
			_ = rt.Store.Renew(ctx, lease, ttl)
		}
	wait:
		select {
		case <-ctx.Done():
			if lease != nil {
				_ = rt.Store.Release(context.Background(), lease)
			}
			return
		case <-tick.C:
		}
	}
}

func orderLoop(ctx context.Context, rt Runtime, kind string) {
	prefix := rt.Key.OrdersByNode(kind, rt.Cfg.Cluster.NodeID)
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
				orderID := lastSeg(v.Key)
				ov, ok, err := rt.Store.Get(ctx, rt.Key.Order(kind, orderID))
				if err != nil || !ok {
					continue
				}
				var ord model.Order
				if err := json.Unmarshal(ov.Data, &ord); err != nil {
					continue
				}
				_ = executeOrder(ctx, rt, kind, ord)
			}
		}
	}
}

func executeOrder(ctx context.Context, rt Runtime, kind string, ord model.Order) error {
	log.Println("executing order", kind, ord.Type)
	remoteKey := rt.Key.MaintenanceNode(kind, rt.Cfg.Cluster.NodeID)
	rm, rmOK, _ := maintenance.ReadRemote(ctx, rt.Store, remoteKey)
	eff := maintenance.Effective(rt.Cfg.Maintenance.LocalEnabled, rt.Cfg.Maintenance.LocalReason, rm, rmOK)
	if eff.Effective {
		return writeStatus(ctx, rt, kind, ord.ID, model.OrderStatus{OrderID: ord.ID, NodeID: rt.Cfg.Cluster.NodeID, State: model.OrderStatusRejected, Reason: "MaintenanceMode", Detail: eff.Reason, Timestamp: time.Now()})
	}

	clauses := rt.Cfg.Constraints.Mongo
	if kind == "kafka" {
		clauses = rt.Cfg.Constraints.Kafka
	}
	if res := constraints.Evaluate(rt.Cfg.Node.Tags, clauses); !res.OK {
		return writeStatus(ctx, rt, kind, ord.ID, model.OrderStatus{OrderID: ord.ID, NodeID: rt.Cfg.Cluster.NodeID, State: model.OrderStatusRejected, Reason: "ConstraintViolation", Detail: res.Reason, Timestamp: time.Now()})
	}

	_ = writeStatus(ctx, rt, kind, ord.ID, model.OrderStatus{OrderID: ord.ID, NodeID: rt.Cfg.Cluster.NodeID, State: model.OrderStatusRunning, Timestamp: time.Now()})

	switch ord.Type {
	case model.OrderEnsureKafka:
		return ensureService(ctx, rt, "kafka", ord)
	case model.OrderEnsureMongo:
		return ensureService(ctx, rt, "mongo", ord)
	case model.OrderStopKafka:
		return stopService(ctx, rt, "kafka", ord)
	case model.OrderStopMongo:
		return stopService(ctx, rt, "mongo", ord)
	case model.OrderRegisterSlot:
		return registerSlot(ctx, rt, kind, ord)
	case model.OrderDeregisterSlot:
		return deregisterSlot(ctx, rt, kind, ord)
	default:
		return writeStatus(ctx, rt, kind, ord.ID, model.OrderStatus{OrderID: ord.ID, NodeID: rt.Cfg.Cluster.NodeID, State: model.OrderStatusFailed, Reason: "UnknownOrder", Detail: string(ord.Type), Timestamp: time.Now()})
	}
}

func ensureService(ctx context.Context, rt Runtime, service string, ord model.Order) error {
	enabled := rt.Cfg.Mongo.Enabled
	exe := rt.Cfg.Mongo.Binary
	args := rt.Cfg.Mongo.StartArgs
	if service == "kafka" {
		enabled = rt.Cfg.Kafka.Enabled
		exe = rt.Cfg.Kafka.Binary
		args = rt.Cfg.Kafka.StartArgs
	}
	if !enabled {
		return succeeded(ctx, rt, service, ord.ID, "disabled")
	}
	instanceID := fmt.Sprintf("%s-slot%d-epoch%d-rev%d", service, ord.Slot, ord.Epoch, ord.Revision)
	if rt.Proc.Get(instanceID) == nil {
		_, err := rt.Proc.Start(ctx, service, instanceID, exe, args, "")
		if err != nil {
			return failed(ctx, rt, service, ord.ID, "StartFailed", err.Error())
		}
		publishProcMeta(ctx, rt, service, instanceID, ord)
	}
	return succeeded(ctx, rt, service, ord.ID, "ensured")
}

func stopService(ctx context.Context, rt Runtime, service string, ord model.Order) error {
	instanceID := fmt.Sprintf("%s-slot%d-epoch%d-rev%d", service, ord.Slot, ord.Epoch, ord.Revision)
	mode := procman.StopTerminate
	if service == "kafka" && strings.ToLower(rt.Cfg.Kafka.StopMode) == "interrupt" {
		mode = procman.StopInterrupt
	}
	if service == "mongo" && strings.ToLower(rt.Cfg.Mongo.StopMode) == "interrupt" {
		mode = procman.StopInterrupt
	}
	if rt.Proc.Get(instanceID) != nil {
		_ = rt.Proc.Stop(ctx, instanceID, mode)
	}
	return succeeded(ctx, rt, service, ord.ID, "stopped(best-effort)")
}

func registerSlot(ctx context.Context, rt Runtime, kind string, ord model.Order) error {
	prefix := rt.Cfg.Mongo.SlotServicePrefix
	port := rt.Cfg.Mongo.Port
	if kind == "kafka" {
		prefix = rt.Cfg.Kafka.SlotServicePrefix
		port = 9092
	}
	name := fmt.Sprintf("%s-%d", prefix, ord.Slot)
	sid := fmt.Sprintf("%s-%s-epoch%d", name, rt.Cfg.Cluster.NodeID, ord.Epoch)
	addr := rt.Cfg.Cluster.AdvertiseIP
	if addr == "" {
		addr = guessIP()
	}
	reg := consulreg.New(rt.Consul)
	if err := reg.Register(ctx, consulreg.Service{Name: name, ID: sid, Address: addr, Port: port, Tags: []string{"kind=" + kind, fmt.Sprintf("slot=%d", ord.Slot)}, TCPCheck: true}); err != nil {
		return failed(ctx, rt, kind, ord.ID, "ServiceRegisterFailed", err.Error())
	}
	return succeeded(ctx, rt, kind, ord.ID, "registered "+name)
}

func deregisterSlot(ctx context.Context, rt Runtime, kind string, ord model.Order) error {
	prefix := rt.Cfg.Mongo.SlotServicePrefix
	if kind == "kafka" {
		prefix = rt.Cfg.Kafka.SlotServicePrefix
	}
	name := fmt.Sprintf("%s-%d", prefix, ord.Slot)
	sid := fmt.Sprintf("%s-%s-epoch%d", name, rt.Cfg.Cluster.NodeID, ord.Epoch)
	reg := consulreg.New(rt.Consul)
	_ = reg.Deregister(ctx, sid)
	return succeeded(ctx, rt, kind, ord.ID, "deregistered "+name)
}

func publishProcMeta(ctx context.Context, rt Runtime, kind, instanceID string, ord model.Order) {
	meta := map[string]any{
		"instanceId": instanceID,
		"nodeId":     rt.Cfg.Cluster.NodeID,
		"service":    kind,
		"slot":       ord.Slot,
		"epoch":      ord.Epoch,
		"revision":   ord.Revision,
		"startTime":  time.Now().UTC().Format(time.RFC3339),
		"logApiBase": rt.LogApiBase,
	}
	b, _ := json.Marshal(meta)
	_, _ = rt.Store.Put(ctx, rt.Key.ProcMeta(kind, instanceID), b)
}

func writeStatus(ctx context.Context, rt Runtime, kind, orderID string, st model.OrderStatus) error {
	log.Println("Order status:", st)
	b, _ := json.Marshal(st)
	_, err := rt.Store.Put(ctx, rt.Key.OrderStatus(kind, orderID, rt.Cfg.Cluster.NodeID), b)
	return err
}
func succeeded(ctx context.Context, rt Runtime, kind, orderID, detail string) error {
	return writeStatus(ctx, rt, kind, orderID, model.OrderStatus{OrderID: orderID, NodeID: rt.Cfg.Cluster.NodeID, State: model.OrderStatusSucceeded, Detail: detail, Timestamp: time.Now()})
}
func failed(ctx context.Context, rt Runtime, kind, orderID, reason, detail string) error {
	return writeStatus(ctx, rt, kind, orderID, model.OrderStatus{OrderID: orderID, NodeID: rt.Cfg.Cluster.NodeID, State: model.OrderStatusFailed, Reason: reason, Detail: detail, Timestamp: time.Now()})
}

func lastSeg(k string) string {
	i := strings.LastIndex(k, "/")
	if i < 0 {
		return k
	}
	return k[i+1:]
}

func jitter() time.Duration { return time.Duration(500+rand.Intn(1000)) * time.Millisecond }

func guessIP() string {
	ifaces, _ := net.Interfaces()
	for _, iface := range ifaces {
		addrs, _ := iface.Addrs()
		for _, a := range addrs {
			ipNet, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			ip := ipNet.IP.To4()
			if ip == nil || ip.IsLoopback() {
				continue
			}
			return ip.String()
		}
	}
	return "127.0.0.1"
}
