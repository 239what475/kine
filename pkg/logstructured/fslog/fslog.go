package fslog

import (
	"context"
	"sync"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/tidwall/btree"
)

func New(config Config) *FSLog {
	return &FSLog{
		rootDir:        config.RootDir,
		byKey:          btree.NewMap[string, []*revOp](0),
		byRev:          map[int64]*revOp{},
		stream:         make(chan server.Events),
		syncEveryWrite: config.SyncEveryWrite,
		snapshotEvery:  config.SnapshotEvery,
		segmentBytes:   config.SegmentBytes,
		cond:           sync.NewCond(&sync.Mutex{}),
	}
}

func (f *FSLog) Start(context.Context) error {
	return nil
}

func (f *FSLog) CompactRevision(context.Context) (int64, error) {
	return f.compactRev.Load(), nil
}

func (f *FSLog) CurrentRevision(context.Context) (int64, error) {
	return f.currentRev.Load(), nil
}

func (f *FSLog) List(context.Context, string, string, int64, int64, bool, bool) (int64, server.Events, error) {
	return 0, nil, ErrNotImplemented
}

func (f *FSLog) Count(context.Context, string, string, int64) (int64, int64, error) {
	return 0, 0, ErrNotImplemented
}

func (f *FSLog) After(context.Context, string, int64, int64) (int64, server.Events, error) {
	return 0, nil, ErrNotImplemented
}

func (f *FSLog) Watch(context.Context, string) <-chan server.Events {
	result := make(chan server.Events)
	close(result)
	return result
}

func (f *FSLog) Append(context.Context, *server.Event) (int64, error) {
	return 0, ErrNotImplemented
}

func (f *FSLog) DbSize(context.Context) (int64, error) {
	return 0, nil
}

func (f *FSLog) Compact(context.Context, int64) (int64, error) {
	return 0, ErrNotImplemented
}

func (f *FSLog) WaitForSyncTo(int64) {}
