package fs

import (
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/logstructured/fslog"
	"github.com/k3s-io/kine/pkg/util"
)

const (
	querySync             = "sync"
	querySnapshotInterval = "snapshot_interval"
	querySegmentBytes     = "segment_bytes"
)

func ParseConfig(cfg *drivers.Config) (fslog.Config, error) {
	if cfg == nil {
		return fslog.Config{}, fmt.Errorf("fs backend requires driver config")
	}
	if cfg.Endpoint == "" {
		return fslog.Config{}, fmt.Errorf("fs backend requires explicit endpoint")
	}

	u, err := util.ParseURL(cfg.Endpoint)
	if err != nil {
		return fslog.Config{}, err
	}
	if u.Scheme != "fs" {
		return fslog.Config{}, fmt.Errorf("fs backend requires scheme fs, got %q", u.Scheme)
	}
	if u.Host != "" {
		return fslog.Config{}, fmt.Errorf("fs backend requires absolute path DSN like fs:///var/lib/kine")
	}

	rootDir := filepath.Clean(u.Path)
	if rootDir == "." || rootDir == "" || !filepath.IsAbs(rootDir) {
		return fslog.Config{}, fmt.Errorf("fs backend requires absolute path DSN like fs:///var/lib/kine")
	}

	for key := range u.Query() {
		switch key {
		case querySync, querySnapshotInterval, querySegmentBytes:
		default:
			return fslog.Config{}, fmt.Errorf("fs backend does not support query parameter %q", key)
		}
	}

	result := fslog.Config{
		RootDir:          rootDir,
		SyncEveryWrite:   true,
		SnapshotEvery:    defaultSnapshotEvery,
		SegmentBytes:     defaultSegmentBytes,
		CompactMinRetain: cfg.CompactMinRetain,
	}

	if value := u.Query().Get(querySync); value != "" {
		syncEveryWrite, err := strconv.ParseBool(value)
		if err != nil {
			return fslog.Config{}, fmt.Errorf("invalid fs backend %q value %q: %w", querySync, value, err)
		}
		result.SyncEveryWrite = syncEveryWrite
	}

	if value := u.Query().Get(querySnapshotInterval); value != "" {
		snapshotEvery, err := strconv.ParseInt(value, 10, 64)
		if err != nil || snapshotEvery <= 0 {
			return fslog.Config{}, fmt.Errorf("invalid fs backend %q value %q", querySnapshotInterval, value)
		}
		result.SnapshotEvery = snapshotEvery
	}

	if value := u.Query().Get(querySegmentBytes); value != "" {
		segmentBytes, err := strconv.ParseInt(value, 10, 64)
		if err != nil || segmentBytes <= 0 {
			return fslog.Config{}, fmt.Errorf("invalid fs backend %q value %q", querySegmentBytes, value)
		}
		result.SegmentBytes = segmentBytes
	}

	return result, nil
}
