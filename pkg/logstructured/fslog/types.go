package fslog

import (
	"bufio"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/k3s-io/kine/pkg/broadcaster"
	"github.com/k3s-io/kine/pkg/server"
	"github.com/tidwall/btree"
)

const (
	// 根目录下的固定文件 / 目录名。
	lockFileName     = "LOCK"
	currentFileName  = "CURRENT"
	metadataFileName = "metadata.json"
	journalDirName   = "journal"
	snapshotDirName  = "snapshots"

	// 扫描和创建持久化文件时使用的后缀。
	journalFileSuffix  = ".log"
	snapshotFileSuffix = ".snapshot.json"
	tempFileSuffix     = ".tmp"
)

// Config 是文件后端自己的配置结构。
// 它已经是 DSN 解析之后的“存储引擎视角”配置，不再携带 driver 层的通用信息。
type Config struct {
	// RootDir 是整个后端状态所在的绝对目录。
	RootDir string
	// SyncEveryWrite 控制每次 append 返回前是否都对 journal 做 fsync。
	SyncEveryWrite bool
	// SnapshotEvery 控制多少个 revision 之后自动生成一次快照。
	SnapshotEvery int64
	// SegmentBytes 控制单个 journal segment 的滚动阈值。
	SegmentBytes int64
	// CompactMinRetain 对应 Kine 的最小保留窗口，用来约束 compaction。
	CompactMinRetain int64
}

// metadata 是独立于 journal 的小型控制面状态。
//
// 它不保存完整历史，只保存：
//   - 当前 revision 边界
//   - compact revision 边界
//   - 当前活跃 segment 的文件名
//
// 真正的历史内容仍然以 journal 和 snapshot 为准。
type metadata struct {
	CurrentRevision int64  `json:"currentRevision"`
	CompactRevision int64  `json:"compactRevision"`
	ActiveSegment   string `json:"activeSegment,omitempty"`
}

// JournalRecord 是单条持久化记录的磁盘格式。
//
// 每一条 JournalRecord 都代表一个逻辑 revision，内容足够重建：
//   - 当前值
//   - 之前的 revision / value
//   - create / delete / lease 等语义信息
//
// 这样 journal replay 时就能恢复出 watch 和历史查询需要的上下文。
type JournalRecord struct {
	Revision       int64  `json:"revision"`
	Key            string `json:"key"`
	Create         bool   `json:"create,omitempty"`
	Delete         bool   `json:"delete,omitempty"`
	CreateRevision int64  `json:"createRevision,omitempty"`
	PrevRevision   int64  `json:"prevRevision,omitempty"`
	Lease          int64  `json:"lease,omitempty"`
	Value          []byte `json:"value,omitempty"`
	PrevValue      []byte `json:"prevValue,omitempty"`
}

// SnapshotFile 是 snapshot 的文件格式。
//
// 当前实现会把“当前保留在内存中的记录集合”整体写进 snapshot。
// 这意味着：
//   - 普通周期性 snapshot 会保存截至当时仍被保留的全部历史记录
//   - compaction 之后写出的 snapshot 则会体现压缩后的基线 + 更晚的新历史
type SnapshotFile struct {
	CurrentRevision int64           `json:"currentRevision"`
	CompactRevision int64           `json:"compactRevision"`
	Records         []JournalRecord `json:"records"`
}

// revOp 是 journal record 的内存态表示。
//
// 它和 JournalRecord 信息基本一致，但更适合放进索引里被频繁读取。
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

// effectiveCreateRevision 返回这个 key 对外应暴露的 create revision。
// 某些记录会显式带 createRevision；否则就退化为自身 revision。
func (r *revOp) effectiveCreateRevision() int64 {
	if r == nil {
		return 0
	}
	if r.createRevision != 0 {
		return r.createRevision
	}
	return r.revision
}

// FSLog 是 `logstructured.Log` 的文件系统实现。
//
// 它同时维护三类状态：
//   - 内存索引：支撑当前读、历史读、watch 追平
//   - 磁盘文件：journal / snapshot / metadata / lock
//   - 同步原语：写锁、watch 广播、可见性等待
//
// 第一版明确假设：同一 rootDir 只会有一个写者进程。
type FSLog struct {
	// rootDir 是整个文件后端状态所在的根目录。
	rootDir string

	// mu 保护需要以“原子状态转换”方式出现的内存 / 磁盘更新。
	mu sync.RWMutex

	// 这些 revision 边界会被频繁读取，所以用 atomic 暴露给无锁读路径。
	currentRev   atomic.Int64
	compactRev   atomic.Int64
	appliedRev   atomic.Int64
	watchStarted atomic.Bool

	// byKey 适合做按 key 读取 / 前缀扫描；
	// byRev 适合做按 revision 顺序回放。
	byKey *btree.Map[string, []*revOp]
	byRev map[int64]*revOp

	// broadcaster / stream 用来支撑 Watch 的实时事件分发。
	broadcaster broadcaster.Broadcaster
	stream      chan server.Events

	// 运行时配置项。
	syncEveryWrite   bool
	snapshotEvery    int64
	segmentBytes     int64
	compactMinRetain int64

	// cond 用来让 WaitForSyncTo 等待某个 revision 真正变得可见。
	cond *sync.Cond

	// 下面这一组是文件句柄和派生路径。
	lockFile      *os.File
	metadata      metadata
	lockPath      string
	currentPath   string
	metadataPath  string
	journalDir    string
	snapshotDir   string
	journalFiles  []string
	snapshotFiles []string

	// 当前活跃 journal segment 的运行时状态。
	segmentFile       *os.File
	segmentWriter     *bufio.Writer
	segmentName       string
	segmentSize       int64
	segmentStartRev   int64
	loadedSnapshotRev int64
	replayedRevision  int64
}

// initPaths 只负责把一批常用路径一次性算出来，避免后续到处重复拼接。
func (f *FSLog) initPaths() {
	f.lockPath = filepath.Join(f.rootDir, lockFileName)
	f.currentPath = filepath.Join(f.rootDir, currentFileName)
	f.metadataPath = filepath.Join(f.rootDir, metadataFileName)
	f.journalDir = filepath.Join(f.rootDir, journalDirName)
	f.snapshotDir = filepath.Join(f.rootDir, snapshotDirName)
}
