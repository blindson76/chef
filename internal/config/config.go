package config

import (
	"errors"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

var envPattern = regexp.MustCompile(`\${env.([A-Za-z_][A-Za-z_]*)\}`)

type Config struct {
	Cluster struct {
		Name   string `yaml:"name"`
		Consul struct {
			Address string `yaml:"address"`
			Token   string `yaml:"token"`
		} `yaml:"consul"`
		NodeID      string `yaml:"nodeId"`
		AdvertiseIP string `yaml:"advertiseIp"`
		HTTP        struct {
			Listen    string `yaml:"listen"`
			AuthToken string `yaml:"authToken"`
		} `yaml:"http"`
		LogRoot string `yaml:"logRoot"`
	} `yaml:"cluster"`

	Timing struct {
		LeaderLockTtl  string `yaml:"leaderLockTtl"`
		CandidateTtl   string `yaml:"candidateTtl"`
		HealthTtl      string `yaml:"healthTtl"`
		ReconcileEvery string `yaml:"reconcileEvery"`
	} `yaml:"timing"`

	Node struct {
		Tags map[string]string `yaml:"tags"`
	} `yaml:"node"`

	Maintenance struct {
		LocalEnabled bool   `yaml:"localEnabled"`
		LocalReason  string `yaml:"localReason"`
	} `yaml:"maintenance"`

	Constraints struct {
		Kafka []ConstraintClause `yaml:"kafka"`
		Mongo []ConstraintClause `yaml:"mongo"`
	} `yaml:"constraints"`

	Kafka KafkaConfig `yaml:"kafka"`
	Mongo MongoConfig `yaml:"mongo"`
}

type ConstraintClause struct {
	Key    string   `yaml:"key" json:"key"`
	Op     string   `yaml:"op" json:"op"`
	Value  string   `yaml:"value,omitempty" json:"value,omitempty"`
	Values []string `yaml:"values,omitempty" json:"values,omitempty"`
}

type KafkaConfig struct {
	Enabled           bool     `yaml:"enabled"`
	SlotServicePrefix string   `yaml:"slotServicePrefix"`
	Binary            string   `yaml:"binary"`
	LogDir            string   `yaml:"logDir"`
	MetaLogDir        string   `yaml:"metaLogDir"`
	InspectTimeout    string   `yaml:"inspectTimeout"`
	StartArgs         []string `yaml:"startArgs"`
	StopMode          string   `yaml:"stopMode"`
}

type MongoConfig struct {
	Enabled           bool     `yaml:"enabled"`
	SlotServicePrefix string   `yaml:"slotServicePrefix"`
	Binary            string   `yaml:"binary"`
	DBPath            string   `yaml:"dbPath"`
	Port              int      `yaml:"port"`
	InspectTimeout    string   `yaml:"inspectTimeout"`
	StartArgs         []string `yaml:"startArgs"`
	StopMode          string   `yaml:"stopMode"`
}

func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	expanded := expandEnvVars(string(raw))
	var c Config
	if err := yaml.Unmarshal([]byte(expanded), &c); err != nil {
		return nil, err
	}
	applyEnvOverrides(&c)

	if c.Cluster.Name == "" {
		return nil, errors.New("cluster.name required")
	}
	if c.Cluster.Consul.Address == "" {
		return nil, errors.New("cluster.consul.address required")
	}
	if c.Cluster.NodeID == "" {
		hn, _ := os.Hostname()
		c.Cluster.NodeID = hn
	}
	if c.Cluster.HTTP.Listen == "" {
		c.Cluster.HTTP.Listen = "0.0.0.0:18080"
	}
	if c.Cluster.LogRoot == "" {
		c.Cluster.LogRoot = "./logs"
	}
	if c.Node.Tags == nil {
		c.Node.Tags = map[string]string{}
	}
	if c.Kafka.SlotServicePrefix == "" {
		c.Kafka.SlotServicePrefix = "kafka"
	}
	if c.Mongo.SlotServicePrefix == "" {
		c.Mongo.SlotServicePrefix = "mongo"
	}
	return &c, nil
}

func expandEnvVars(in string) string {
	return envPattern.ReplaceAllStringFunc(in, func(m string) string {
		sub := envPattern.FindStringSubmatch(m)
		if len(sub) != 2 {
			return m
		}
		return os.Getenv(sub[1])
	})
}

func (c *Config) LeaderTTL() time.Duration    { return mustDur(c.Timing.LeaderLockTtl, 10*time.Second) }
func (c *Config) CandidateTTL() time.Duration { return mustDur(c.Timing.CandidateTtl, 15*time.Second) }
func (c *Config) HealthTTL() time.Duration    { return mustDur(c.Timing.HealthTtl, 15*time.Second) }
func (c *Config) ReconcileEvery() time.Duration {
	return mustDur(c.Timing.ReconcileEvery, 3*time.Second)
}

func mustDur(s string, def time.Duration) time.Duration {
	if strings.TrimSpace(s) == "" {
		return def
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return def
	}
	return d
}

// ORCH_CLUSTER_NODEID, ORCH_CLUSTER_ADVERTISEIP, ORCH_CLUSTER_HTTP_LISTEN, ORCH_CLUSTER_HTTP_AUTHTOKEN, ORCH_CLUSTER_LOGROOT
// ORCH_TAG_<key>=<val> (key uses '_' -> '.')
func applyEnvOverrides(c *Config) {
	get := func(k string) string { return strings.TrimSpace(os.Getenv(k)) }
	if v := get("ORCH_CLUSTER_NODEID"); v != "" {
		c.Cluster.NodeID = v
	}
	if v := get("ORCH_CLUSTER_ADVERTISEIP"); v != "" {
		c.Cluster.AdvertiseIP = v
	}
	if v := get("ORCH_CLUSTER_HTTP_LISTEN"); v != "" {
		c.Cluster.HTTP.Listen = v
	}
	if v := get("ORCH_CLUSTER_HTTP_AUTHTOKEN"); v != "" {
		c.Cluster.HTTP.AuthToken = v
	}
	if v := get("ORCH_CLUSTER_LOGROOT"); v != "" {
		c.Cluster.LogRoot = v
	}

	if c.Node.Tags == nil {
		c.Node.Tags = map[string]string{}
	}
	for _, e := range os.Environ() {
		if !strings.HasPrefix(e, "ORCH_TAG_") {
			continue
		}
		parts := strings.SplitN(e, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.ToLower(strings.ReplaceAll(strings.TrimPrefix(parts[0], "ORCH_TAG_"), "_", "."))
		c.Node.Tags[key] = parts[1]
	}
}
