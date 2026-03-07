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

// fileState 是 rootDir 展开后的文件系统工作集。
//
// 它把 FSLog 需要长期持有的路径、句柄和扫描结果收拢到一起，避免主 struct 里混着一大串
// 文件系统细节。可以把它理解成“这个 FSLog 实例当前绑定到哪个磁盘目录，以及已经认识了
// 哪些文件”。
type fileState struct {
	// lockFile 不是普通缓存，而是必须持续持有的文件句柄；文件锁依赖它对应的 fd 存活。
	lockFile *os.File
	// metadata 是 metadata.json 的内存副本，避免每次都重新读文件。
	metadata metadata
	// lockPath 指向 <root>/LOCK，用来加排他锁，确保同一 rootDir 只有一个写者进程。
	lockPath string
	// currentPath 对应 <root>/CURRENT。当前实现尚未真正写入它，但保留了这个派生路径，
	// 方便后续如果要引入类似“当前活跃 segment 指针文件”的做法时直接复用。
	currentPath string
	// metadataPath 指向 <root>/metadata.json，它保存 currentRevision / compactRevision /
	// activeSegment 这类小型控制面状态。
	metadataPath string
	// journalDir 指向 <root>/journal，真正不断增长的变更日志文件都在这里。
	journalDir string
	// snapshotDir 指向 <root>/snapshots，周期性 snapshot 和 compact 后写出的快照都在这里。
	snapshotDir string
	// journalFiles 是当前已知 journal 文件的绝对路径列表；它来自启动扫描，也会在运行时随着
	// 新 segment 生成和 compact 清理而更新。
	journalFiles []string
	// snapshotFiles 是当前已知 snapshot 文件的绝对路径列表；恢复时通常会取其中最新的一份。
	snapshotFiles []string
}

// segmentState 是当前活跃 journal segment 以及恢复过程的辅助状态。
//
// 它保存“当前正在写哪个 segment”以及“最近一次恢复加载到了哪里”。这些信息都强相关，
// 放在同一个小结构里会比散落在 FSLog 顶层更容易建立心智模型。
type segmentState struct {
	// file / writer 是当前活跃 segment 的打开句柄和带缓冲写入器。
	file   *os.File
	writer *bufio.Writer
	// name / size / startRev 描述当前活跃 segment 本身。
	name     string
	size     int64
	startRev int64
	// loadedSnapshotRev / replayedRevision 则描述最近一次启动恢复加载到了哪里。
	loadedSnapshotRev int64
	replayedRevision  int64
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

	// byKey 适合做按 key 读取 / 前缀扫描。
	//
	// 这里的 value 之所以是 `[]*revOp`，而不是单个值，也不是 `map[int64]*revOp`，
	// 是因为同一个 key 在时间线上会经历多次 create / update / delete / recreate，
	// 我们需要把它保存成“一条按 revision 排序的历史链”。
	//
	// 例如 key `/a` 可能对应：
	//   rev=5  -> create
	//   rev=9  -> update
	//   rev=12 -> delete
	//   rev=20 -> recreate
	// 那么 `byKey["/a"]` 最自然的形态就是 `[]*revOp{...}` 这种有序序列。
	//
	// 这样设计有几个直接好处：
	//   1. 查“某个 key 在指定 revision 时的状态”时，可以沿着这条历史链从后往前找
	//      最后一条 `revision <= targetRevision` 的记录；
	//   2. append 新版本时很自然，就是往这个切片后面继续追加；
	//   3. compact 一个 key 的历史时，也更容易按顺序保留基线和后续记录。
	//
	// value 里用 `*revOp` 而不是 `revOp`，是因为同一条逻辑记录还会同时出现在 `byRev`
	// 这套索引里。用指针可以让两个索引共享同一个对象；只有在 compaction 这类需要构造
	// 新基线的时候，才显式 clone 一份。
	byKey *btree.Map[string, []*revOp]
	// byRev 适合做按 revision 精确定位和顺序回放。
	//
	// 它回答的问题和 byKey 不一样：
	//   - byKey 关注“某个 key 的整条历史链”；
	//   - byRev 关注“全局 revision = X 时，对应的那条记录是什么”。
	//
	// 所以这里直接用 `map[int64]*revOp`：以 revision 为键，可以快速命中单条记录；
	// 在需要顺序回放时，再按 revision 递增去取即可。
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

	// files 收拢 rootDir 派生出来的固定路径、文件句柄和扫描结果。
	files fileState
	// segment 收拢当前活跃 segment 和恢复过程的辅助状态。
	segment segmentState
}

// initPaths 只负责把一批常用路径一次性算出来，避免后续到处重复拼接。
func (f *FSLog) initPaths() {
	f.files.lockPath = filepath.Join(f.rootDir, lockFileName)
	f.files.currentPath = filepath.Join(f.rootDir, currentFileName)
	f.files.metadataPath = filepath.Join(f.rootDir, metadataFileName)
	f.files.journalDir = filepath.Join(f.rootDir, journalDirName)
	f.files.snapshotDir = filepath.Join(f.rootDir, snapshotDirName)
}
