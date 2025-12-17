package kafkahelper

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/umitbozkurt/orchestrator/internal/model"
)

type Dirs struct{ LogDir, MetaDir string }

func InspectOffline(ctx context.Context, d Dirs) (*model.KafkaOfflineStatus, error) {
	st := &model.KafkaOfflineStatus{LogDir: d.LogDir, MetaDir: d.MetaDir}
	mp := filepath.Join(d.MetaDir, "meta.properties")
	if _, err := os.Stat(mp); err == nil {
		st.HasMetaProperties = true
		st.Formatted = true
		parseMetaProperties(mp, st)
	}
	if entries, err := os.ReadDir(d.MetaDir); err == nil {
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), "__cluster_metadata") {
				st.Formatted = true
			}
		}
	}
	ro := false
	_ = filepath.WalkDir(d.MetaDir, func(p string, de os.DirEntry, err error) error {
		if err != nil || de == nil {
			return nil
		}
		if strings.HasSuffix(strings.ToLower(de.Name()), ".deleted") {
			ro = true
		}
		return nil
	})
	st.HasReadOnlyArtifacts = ro
	st.LastModified = latestModTime(d.MetaDir, d.LogDir)
	if !st.Formatted {
		st.Warnings = append(st.Warnings, "kafka storage appears unformatted")
	}
	return st, nil
}

func parseMetaProperties(path string, st *model.KafkaOfflineStatus) {
	f, err := os.Open(path)
	if err != nil {
		st.Warnings = append(st.Warnings, "failed to open meta.properties")
		return
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "cluster.id=") {
			st.ClusterID = strings.TrimPrefix(line, "cluster.id=")
		}
		if strings.HasPrefix(line, "node.id=") {
			st.NodeID = strings.TrimPrefix(line, "node.id=")
		}
	}
}

func latestModTime(paths ...string) time.Time {
	var best time.Time
	for _, p := range paths {
		_ = filepath.Walk(p, func(pp string, info os.FileInfo, err error) error {
			if err != nil || info == nil {
				return nil
			}
			if info.ModTime().After(best) {
				best = info.ModTime()
			}
			return nil
		})
	}
	return best
}
