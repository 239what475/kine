package fs

import (
	"context"
	"sync"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/logstructured"
	"github.com/k3s-io/kine/pkg/logstructured/fslog"
	"github.com/k3s-io/kine/pkg/server"
)

const (
	defaultSnapshotEvery = int64(1000)
	defaultSegmentBytes  = int64(64 << 20)
)

func New(ctx context.Context, wg *sync.WaitGroup, cfg *drivers.Config) (bool, server.Backend, error) {
	_ = ctx
	_ = wg

	backend := logstructured.New(fslog.New(fslog.Config{
		RootDir:        cfg.DataSourceName,
		SyncEveryWrite: true,
		SnapshotEvery:  defaultSnapshotEvery,
		SegmentBytes:   defaultSegmentBytes,
	}))

	return false, backend, nil
}

func init() {
	drivers.Register("fs", New)
}
