# Orchestrator (Single-process Agent+Controller) – Consul-backed

This project provides a **single Windows-friendly binary** (`orchestrator.exe`) that runs on every node.
It always runs the **Agent** and runs the **Controller** only on the node that holds the **Consul leader lock**.

## What is implemented (v1)
- Generic `Store` interface + **ConsulStore** implementation (only store implementation).
- Leader election via Consul session lock.
- Agent:
  - Candidate reporting with TTL (includes **node tags**, **maintenance**, **offline status** for Kafka & Mongo).
  - Health reporting with TTL (basic node health).
  - Order watching/execution with **constraint + maintenance** enforcement (rejects when violated).
  - Local process management with stdout/stderr capture to rotating files.
  - Remote **Log API** (list/read/follow/download).
  - Consul **DNS slot service registration** (`mongo-1`, `mongo-2`, `mongo-3`, similarly for kafka).
- Controller:
  - Watches/polls candidates and health.
  - Applies **hard constraints** (+ maintenance) to select eligible nodes.
  - Selects the **most recent 3** nodes deterministically.
  - Publishes desired Spec and creates Orders for selected nodes.
  - Replaces failed/expired members as eligible nodes change.

> Note: provider actions are implemented as safe, idempotent stubs that can start real binaries
> (Kafka/Mongo) when configured, but do not assume any specific filesystem layout.
> Expand `internal/kafkahelper` and `internal/mongohelper` commands for your environment.

## Build
```bash
go build ./cmd/orchestrator
```

## Run (example)
```bash
orchestrator.exe -config config.yaml
```

## Consul DNS
Services registered for slots:
- `mongo-1.service.consul`, `mongo-2.service.consul`, `mongo-3.service.consul`
- `kafka-1.service.consul`, `kafka-2.service.consul`, `kafka-3.service.consul`

If you want literal `1.mongo`, use a local DNS CNAME to `mongo-1.service.consul`.

## Config
See `config.example.yaml`.

