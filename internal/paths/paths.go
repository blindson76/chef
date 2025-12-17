package paths

import "fmt"

type Keyspace struct {
    Root string // e.g. orchestrator/main
}

func (k Keyspace) LeaderLock(kind string) string {
    return fmt.Sprintf("%s/%s/leader/lock", k.Root, kind)
}
func (k Keyspace) Candidates(kind string) string {
    return fmt.Sprintf("%s/%s/candidates/", k.Root, kind)
}
func (k Keyspace) Candidate(kind, nodeID string) string {
    return fmt.Sprintf("%s/%s/candidates/%s", k.Root, kind, nodeID)
}
func (k Keyspace) Health(kind string) string {
    return fmt.Sprintf("%s/%s/health/", k.Root, kind)
}
func (k Keyspace) HealthKey(kind, nodeID string) string {
    return fmt.Sprintf("%s/%s/health/%s", k.Root, kind, nodeID)
}
func (k Keyspace) Spec(kind string) string {
    return fmt.Sprintf("%s/%s/spec/current", k.Root, kind)
}
func (k Keyspace) Orders(kind string) string {
    return fmt.Sprintf("%s/%s/orders/", k.Root, kind)
}
func (k Keyspace) OrdersByNode(kind, nodeID string) string {
    return fmt.Sprintf("%s/%s/orders-by-node/%s/", k.Root, kind, nodeID)
}
func (k Keyspace) OrderPtr(kind, nodeID, orderID string) string {
    return fmt.Sprintf("%s/%s/orders-by-node/%s/%s", k.Root, kind, nodeID, orderID)
}
func (k Keyspace) Order(kind, orderID string) string {
    return fmt.Sprintf("%s/%s/orders/%s", k.Root, kind, orderID)
}
func (k Keyspace) OrderStatus(kind, orderID, nodeID string) string {
    return fmt.Sprintf("%s/%s/order-status/%s/%s", k.Root, kind, orderID, nodeID)
}
func (k Keyspace) MaintenanceNode(kind, nodeID string) string {
    return fmt.Sprintf("%s/%s/maintenance/nodes/%s", k.Root, kind, nodeID)
}
func (k Keyspace) ProcMeta(kind, instanceID string) string {
    return fmt.Sprintf("%s/%s/proc/%s/meta", k.Root, kind, instanceID)
}
