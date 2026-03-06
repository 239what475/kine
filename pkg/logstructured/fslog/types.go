package fslog

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/k3s-io/kine/pkg/broadcaster"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/tidwall/btree"
)

const (
	lockFileName     = "LOCK"
	currentFileName  = "CURRENT"
	metadataFileName = "metadata.json"
	journalDirName   = "journal"
	snapshotDirName  = "snapshots"
)

type Config struct {
	RootDir        string
	SyncEveryWrite bool
	SnapshotEvery  int64
	SegmentBytes   int64
}

type metadata struct {
	CurrentRevision int64  `json:"currentRevision"`
	CompactRevision int64  `json:"compactRevision"`
	ActiveSegment   string `json:"activeSegment,omitempty"`
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

	lockFile      *os.File
	metadata      metadata
	lockPath      string
	currentPath   string
	metadataPath  string
	journalDir    string
	snapshotDir   string
	journalFiles  []string
	snapshotFiles []string
}

func (f *FSLog) initPaths() {
	f.lockPath = filepath.Join(f.rootDir, lockFileName)
	f.currentPath = filepath.Join(f.rootDir, currentFileName)
	f.metadataPath = filepath.Join(f.rootDir, metadataFileName)
	f.journalDir = filepath.Join(f.rootDir, journalDirName)
	f.snapshotDir = filepath.Join(f.rootDir, snapshotDirName)
}
