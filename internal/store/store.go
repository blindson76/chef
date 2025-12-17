package store

import (
    "context"
    "time"
)

type EventType string

const (
    EventPut    EventType = "put"
    EventDelete EventType = "delete"
)

type Value struct {
    Key       string
    Data      []byte
    Revision  uint64
    SessionID string
    ModTime   time.Time
}

type Event struct {
    Type  EventType
    Value Value
}

type LeaseHandle interface {
    ID() string
}

type LockHandle interface {
    // KeepAlive maintains lock lease until ctx cancelled or error.
    KeepAlive(ctx context.Context) error
    Unlock(ctx context.Context) error
    IsHeld() bool
}

type Store interface {
    Lock(ctx context.Context, key string, ttl time.Duration) (LockHandle, error)

    PutLeased(ctx context.Context, key string, val []byte, ttl time.Duration) (LeaseHandle, error)
    Renew(ctx context.Context, lease LeaseHandle, ttl time.Duration) error
    Release(ctx context.Context, lease LeaseHandle) error

    Get(ctx context.Context, key string) (Value, bool, error)
    Put(ctx context.Context, key string, val []byte) (uint64, error)

    PutCAS(ctx context.Context, key string, val []byte, expectedRev uint64) (newRev uint64, ok bool, err error)

    List(ctx context.Context, prefix string) ([]Value, error)
    WatchPrefix(ctx context.Context, prefix string) (<-chan Event, error)
}
