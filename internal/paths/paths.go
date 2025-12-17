package paths

import "fmt"

type Keyspace struct{ Root string }

func (k Keyspace) LeaderLock(kind string) string               { return fmt.Sprintf("%s/%s/leader/lock", k.Root, kind) }
func (k Keyspace) Candidates(kind string) string               { return fmt.Sprintf("%s/%s/candidates/", k.Root, kind) }
func (k Keyspace) Candidate(kind, node string) string          { return fmt.Sprintf("%s/%s/candidates/%s", k.Root, kind, node) }
func (k Keyspace) Health(kind string) string                   { return fmt.Sprintf("%s/%s/health/", k.Root, kind) }
func (k Keyspace) HealthKey(kind, node string) string          { return fmt.Sprintf("%s/%s/health/%s", k.Root, kind, node) }
func (k Keyspace) Spec(kind string) string                     { return fmt.Sprintf("%s/%s/spec/current", k.Root, kind) }
func (k Keyspace) Orders(kind string) string                   { return fmt.Sprintf("%s/%s/orders/", k.Root, kind) }
func (k Keyspace) OrdersByNode(kind, node string) string       { return fmt.Sprintf("%s/%s/orders-by-node/%s/", k.Root, kind, node) }
func (k Keyspace) OrderPtr(kind, node, order string) string    { return fmt.Sprintf("%s/%s/orders-by-node/%s/%s", k.Root, kind, node, order) }
func (k Keyspace) Order(kind, order string) string             { return fmt.Sprintf("%s/%s/orders/%s", k.Root, kind, order) }
func (k Keyspace) OrderStatus(kind, order, node string) string { return fmt.Sprintf("%s/%s/order-status/%s/%s", k.Root, kind, order, node) }
func (k Keyspace) MaintenanceNode(kind, node string) string    { return fmt.Sprintf("%s/%s/maintenance/nodes/%s", k.Root, kind, node) }
func (k Keyspace) ProcMeta(kind, instance string) string       { return fmt.Sprintf("%s/%s/proc/%s/meta", k.Root, kind, instance) }
