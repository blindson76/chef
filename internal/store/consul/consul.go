package consul

import (
    "context"
    "errors"
    "fmt"
    "time"

    capi "github.com/hashicorp/consul/api"

    "github.com/umitbozkurt/orchestrator/internal/store"
)

type Config struct {
    Address string
    Token   string
}

type ConsulStore struct {
    c *capi.Client
}

func New(cfg Config) (*ConsulStore, error) {
    ccfg := capi.DefaultConfig()
    if cfg.Address != "" {
        ccfg.Address = cfg.Address
    }
    if cfg.Token != "" {
        ccfg.Token = cfg.Token
    }
    c, err := capi.NewClient(ccfg)
    if err != nil {
        return nil, err
    }
    return &ConsulStore{c: c}, nil
}

type lease struct{ id string }

func (l lease) ID() string { return l.id }

func (s *ConsulStore) PutLeased(ctx context.Context, key string, val []byte, ttl time.Duration) (store.LeaseHandle, error) {
    sessID, err := s.createSession(ctx, ttl)
    if err != nil {
        return nil, err
    }
    p := &capi.KVPair{Key: key, Value: val, Session: sessID}
    ok, _, err := s.c.KV().Acquire(p, nil)
    if err != nil {
        _ = s.destroySession(context.Background(), sessID)
        return nil, err
    }
    if !ok {
        _ = s.destroySession(context.Background(), sessID)
        return nil, fmt.Errorf("failed to acquire lease for key %s", key)
    }
    return lease{id: sessID}, nil
}

func (s *ConsulStore) Renew(ctx context.Context, lh store.LeaseHandle, ttl time.Duration) error {
    // best-effort periodic renewal; caller should call regularly
    _, _, err := s.c.Session().Renew(lh.ID(), nil)
    return err
}

func (s *ConsulStore) Release(ctx context.Context, lh store.LeaseHandle) error {
    return s.destroySession(ctx, lh.ID())
}

func (s *ConsulStore) Get(ctx context.Context, key string) (store.Value, bool, error) {
    p, qm, err := s.c.KV().Get(key, nil)
    if err != nil {
        return store.Value{}, false, err
    }
    if p == nil {
        return store.Value{}, false, nil
    }
    _ = qm
    return store.Value{
        Key:      p.Key,
        Data:     p.Value,
        Revision: uint64(p.ModifyIndex),
        SessionID: p.Session,
        ModTime:  time.Time{}, // Consul KV doesn't expose modtime; leave zero
    }, true, nil
}

func (s *ConsulStore) Put(ctx context.Context, key string, val []byte) (uint64, error) {
    p := &capi.KVPair{Key: key, Value: val}
    w, _, err := s.c.KV().Put(p, nil)
    if err != nil {
        return 0, err
    }
    return uint64(w), nil
}

func (s *ConsulStore) PutCAS(ctx context.Context, key string, val []byte, expectedRev uint64) (uint64, bool, error) {
    p := &capi.KVPair{Key: key, Value: val, ModifyIndex: uint64(expectedRev)}
    ok, wm, err := s.c.KV().CAS(p, nil)
    if err != nil {
        return 0, false, err
    }
    if wm == nil {
        return expectedRev, ok, nil
    }
    return uint64(wm.LastIndex), ok, nil
}

func (s *ConsulStore) List(ctx context.Context, prefix string) ([]store.Value, error) {
    pairs, _, err := s.c.KV().List(prefix, nil)
    if err != nil {
        return nil, err
    }
    out := make([]store.Value, 0, len(pairs))
    for _, p := range pairs {
        if p == nil {
            continue
        }
        out = append(out, store.Value{
            Key:       p.Key,
            Data:      p.Value,
            Revision:  uint64(p.ModifyIndex),
            SessionID: p.Session,
        })
    }
    return out, nil
}

func (s *ConsulStore) WatchPrefix(ctx context.Context, prefix string) (<-chan store.Event, error) {
    ch := make(chan store.Event, 64)
    // polling/blocking query loop using WaitIndex
    go func() {
        defer close(ch)
        var waitIndex uint64 = 0
        for {
            select {
            case <-ctx.Done():
                return
            default:
            }
            q := &capi.QueryOptions{WaitIndex: waitIndex, WaitTime: 10 * time.Second}
            pairs, meta, err := s.c.KV().List(prefix, q)
            if err != nil {
                // backoff
                time.Sleep(500 * time.Millisecond)
                continue
            }
            if meta != nil {
                waitIndex = meta.LastIndex
            }
            // Emit a coarse "put" snapshot events; consumers should re-list for exact state.
            for _, p := range pairs {
                if p == nil {
                    continue
                }
                ev := store.Event{
                    Type: store.EventPut,
                    Value: store.Value{
                        Key:       p.Key,
                        Data:      p.Value,
                        Revision:  uint64(p.ModifyIndex),
                        SessionID: p.Session,
                    },
                }
                select {
                case ch <- ev:
                default:
                    // drop if slow consumer
                }
            }
        }
    }()
    return ch, nil
}

type lockHandle struct {
    s       *ConsulStore
    key     string
    sessID  string
    held    bool
}

func (l *lockHandle) IsHeld() bool { return l.held }

func (l *lockHandle) KeepAlive(ctx context.Context) error {
    // renew session periodically
    ticker := time.NewTicker(3 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-ticker.C:
            _, _, err := l.s.c.Session().Renew(l.sessID, nil)
            if err != nil {
                l.held = false
                return err
            }
            // Also re-acquire lock key (best-effort)
            ok, _, err := l.s.c.KV().Acquire(&capi.KVPair{Key: l.key, Value: []byte(l.sessID), Session: l.sessID}, nil)
            if err != nil || !ok {
                l.held = false
                if err == nil {
                    err = errors.New("lock acquire failed")
                }
                return err
            }
        }
    }
}

func (l *lockHandle) Unlock(ctx context.Context) error {
    l.held = false
    // release key then destroy session
    _, _, _ = l.s.c.KV().Release(&capi.KVPair{Key: l.key, Session: l.sessID}, nil)
    return l.s.destroySession(ctx, l.sessID)
}

func (s *ConsulStore) Lock(ctx context.Context, key string, ttl time.Duration) (store.LockHandle, error) {
    sessID, err := s.createSession(ctx, ttl)
    if err != nil {
        return nil, err
    }
    ok, _, err := s.c.KV().Acquire(&capi.KVPair{Key: key, Value: []byte(sessID), Session: sessID}, nil)
    if err != nil {
        _ = s.destroySession(context.Background(), sessID)
        return nil, err
    }
    if !ok {
        _ = s.destroySession(context.Background(), sessID)
        return &lockHandle{s: s, key: key, sessID: sessID, held: false}, nil
    }
    return &lockHandle{s: s, key: key, sessID: sessID, held: true}, nil
}

func (s *ConsulStore) createSession(ctx context.Context, ttl time.Duration) (string, error) {
    se := &capi.SessionEntry{
        Behavior: capi.SessionBehaviorDelete,
        TTL:      ttl.String(),
        LockDelay: 0,
    }
    id, _, err := s.c.Session().Create(se, nil)
    return id, err
}

func (s *ConsulStore) destroySession(ctx context.Context, id string) error {
    _, err := s.c.Session().Destroy(id, nil)
    return err
}
