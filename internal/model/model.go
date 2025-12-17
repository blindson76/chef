package model

import "time"

type Maintenance struct {
    Enabled bool      `json:"enabled" yaml:"enabled"`
    Reason  string    `json:"reason" yaml:"reason"`
    SetBy   string    `json:"setBy" yaml:"setBy"`
    Since   time.Time `json:"since" yaml:"since"`
    Until   *time.Time `json:"until,omitempty" yaml:"until,omitempty"`
}

type CandidateReport struct {
    NodeID     string            `json:"nodeId"`
    Timestamp  time.Time         `json:"timestamp"`
    BootTime   time.Time         `json:"bootTime"`
    Tags       map[string]string `json:"tags"`
    Maintenance EffectiveMaintenance `json:"maintenance"`
    Offline    OfflineReport     `json:"offline"`
}

type EffectiveMaintenance struct {
    Effective bool   `json:"effective"`
    Source    string `json:"source"` // local|remote|both|none
    Reason    string `json:"reason"`
}

type OfflineReport struct {
    Kafka *KafkaOfflineStatus `json:"kafka,omitempty"`
    Mongo *MongoOfflineStatus `json:"mongo,omitempty"`
    Warnings []string         `json:"warnings,omitempty"`
}

type KafkaOfflineStatus struct {
    Formatted bool   `json:"formatted"`
    HasMetaProperties bool `json:"hasMetaProperties"`
    ClusterID string `json:"clusterId,omitempty"`
    NodeID    string `json:"nodeId,omitempty"`
    MetaDir   string `json:"metaDir,omitempty"`
    LogDir    string `json:"logDir,omitempty"`
    HasReadOnlyArtifacts bool `json:"hasReadOnlyArtifacts"`
    LastModified time.Time `json:"lastModified"`
    Warnings []string `json:"warnings,omitempty"`
}

type MongoOfflineStatus struct {
    HasWiredTiger bool `json:"hasWiredTiger"`
    HasLockFile   bool `json:"hasLockFile"`
    DBPath        string `json:"dbPath,omitempty"`
    LastModified  time.Time `json:"lastModified"`
    Warnings []string `json:"warnings,omitempty"`
}

type HealthStatus struct {
    NodeID    string    `json:"nodeId"`
    Timestamp time.Time `json:"timestamp"`
    Healthy   bool      `json:"healthy"`
    Detail    string    `json:"detail,omitempty"`
}

type Spec struct {
    Kind      string    `json:"kind"` // kafka|mongo
    Epoch     int64     `json:"epoch"`
    Revision  int64     `json:"revision"`
    Members   []Member  `json:"members"`
    CreatedAt time.Time `json:"createdAt"`
    ConstraintsHash string `json:"constraintsHash,omitempty"`
}

type Member struct {
    Slot   int    `json:"slot"`
    NodeID string `json:"nodeId"`
    Address string `json:"address,omitempty"`
    Port    int    `json:"port,omitempty"`
}

type OrderType string

const (
    OrderEnsureKafka OrderType = "ensure_kafka"
    OrderEnsureMongo OrderType = "ensure_mongo"
    OrderStopKafka   OrderType = "stop_kafka"
    OrderStopMongo   OrderType = "stop_mongo"
    OrderRegisterSlot OrderType = "register_slot"
    OrderDeregisterSlot OrderType = "deregister_slot"
)

type Order struct {
    ID        string    `json:"id"`
    Kind      string    `json:"kind"`
    Epoch     int64     `json:"epoch"`
    Revision  int64     `json:"revision"`
    TargetNode string   `json:"targetNode"`
    Slot      int       `json:"slot"`
    Type      OrderType `json:"type"`
    Payload   map[string]any `json:"payload,omitempty"`
    CreatedAt time.Time `json:"createdAt"`
}

type OrderStatusState string

const (
    OrderStatusRunning   OrderStatusState = "running"
    OrderStatusSucceeded OrderStatusState = "succeeded"
    OrderStatusFailed    OrderStatusState = "failed"
    OrderStatusRejected  OrderStatusState = "rejected"
)

type OrderStatus struct {
    OrderID   string           `json:"orderId"`
    NodeID    string           `json:"nodeId"`
    State     OrderStatusState `json:"state"`
    Reason    string           `json:"reason,omitempty"`
    Detail    string           `json:"detail,omitempty"`
    Timestamp time.Time        `json:"timestamp"`
}
