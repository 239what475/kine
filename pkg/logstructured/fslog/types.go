package fslog

import (
	"sync"
	"sync/atomic"

	"github.com/k3s-io/kine/pkg/broadcaster"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/tidwall/btree"
)

type Config struct {
	RootDir        string
	SyncEveryWrite bool
	SnapshotEvery  int64
	SegmentBytes   int64
}

type revOp struct {
	revision       int64
	create         bool
	delete         bool
	key            string
	createRevision int64
	prevRevision   int64
	lease          int64
	value          []byte
	prevValue      []byte
}

type FSLog struct {
	rootDir string

	mu sync.RWMutex

	currentRev atomic.Int64
	compactRev atomic.Int64
	appliedRev atomic.Int64

	byKey *btree.Map[string, []*revOp]
	byRev map[int64]*revOp

	broadcaster broadcaster.Broadcaster
	stream      chan server.Events

	syncEveryWrite bool
	snapshotEvery  int64
	segmentBytes   int64

	cond *sync.Cond
}
