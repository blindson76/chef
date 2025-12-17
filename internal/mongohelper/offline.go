package mongohelper

import (
	"context"
	"os"
	"path/filepath"
	"time"

	"github.com/umitbozkurt/orchestrator/internal/model"
)

func InspectOffline(ctx context.Context, dbPath string) (*model.MongoOfflineStatus, error) {
	st := &model.MongoOfflineStatus{DBPath: dbPath}
	if _, err := os.Stat(filepath.Join(dbPath, "WiredTiger")); err == nil {
		st.HasWiredTiger = true
	}
	if _, err := os.Stat(filepath.Join(dbPath, "mongod.lock")); err == nil {
		st.HasLockFile = true
		st.Warnings = append(st.Warnings, "mongod.lock exists (unclean shutdown likely)")
	}
	st.LastModified = latestModTime(dbPath)
	if !st.HasWiredTiger {
		st.Warnings = append(st.Warnings, "WiredTiger file not found (dbPath empty or different engine)")
	}
	return st, nil
}

func latestModTime(root string) time.Time {
	var best time.Time
	_ = filepath.Walk(root, func(pp string, info os.FileInfo, err error) error {
		if err != nil || info == nil {
			return nil
		}
		if info.ModTime().After(best) {
			best = info.ModTime()
		}
		return nil
	})
	return best
}
