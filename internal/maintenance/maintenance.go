package maintenance

import (
    "context"
    "encoding/json"
    "time"

    "github.com/umitbozkurt/orchestrator/internal/model"
    "github.com/umitbozkurt/orchestrator/internal/store"
)

func ReadRemote(ctx context.Context, st store.Store, key string) (*model.Maintenance, bool, error) {
    v, ok, err := st.Get(ctx, key)
    if err != nil || !ok {
        return nil, ok, err
    }
    var m model.Maintenance
    if err := json.Unmarshal(v.Data, &m); err != nil {
        return nil, true, err
    }
    if m.Until != nil && time.Now().After(*m.Until) {
        return &m, true, nil
    }
    return &m, true, nil
}

func Effective(localEnabled bool, localReason string, remote *model.Maintenance, remoteOK bool) model.EffectiveMaintenance {
    eff := model.EffectiveMaintenance{Effective: false, Source: "none", Reason: ""}
    if localEnabled {
        eff.Effective = true
        eff.Source = "local"
        eff.Reason = localReason
    }
    if remoteOK && remote != nil && remote.Enabled {
        if eff.Effective {
            eff.Source = "both"
            if eff.Reason == "" {
                eff.Reason = remote.Reason
            }
        } else {
            eff.Effective = true
            eff.Source = "remote"
            eff.Reason = remote.Reason
        }
    }
    return eff
}
