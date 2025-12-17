# Orchestrator (Controller + Worker)

Two binaries:
- `controller`: leader-elected reconciler that writes Spec + Orders
- `worker`: runs on every node, publishes Candidate/Health, executes Orders, registers slot DNS services, exposes Log API

## Build
```bash
go build ./cmd/controller
go build ./cmd/worker
```

## Run
```bash
controller -config config.yaml
worker -config config.yaml
```

## Consul DNS slot services
Examples:
- `mongo-1.service.consul`, `mongo-2.service.consul`, `mongo-3.service.consul`
- `kafka-1.service.consul`, `kafka-2.service.consul`, `kafka-3.service.consul`

## Config
See `config.example.yaml`.
