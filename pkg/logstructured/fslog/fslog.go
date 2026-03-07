package fslog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/tidwall/btree"
)

// New 创建一个空的 FSLog，并初始化它后续会长期使用的内存结构。
//
// 这里只做“对象初始化”，还不会真正接触磁盘。真正的启动恢复逻辑发生在
// Start(...) 中。
func New(config Config) *FSLog {
	log := &FSLog{
		rootDir: config.RootDir,
		// 这里的 `btree.NewMap[string, []*revOp](0)` 是 Go 泛型（generics）语法。
		//
		// 可以拆成两层看：
		//   1. `NewMap[...]` 中的 `[...]` 传的是“类型参数”，不是数组或切片；
		//   2. 最后的 `(0)` 才是普通函数参数。
		//
		// 所以这行的意思其实是：
		//   - 创建一个 btree.Map
		//   - 其中 key 的类型是 `string`
		//   - value 的类型是 `[]*revOp`
		//   - 再把运行时参数 `0` 传给构造函数
		//
		// 如果把它翻成更接近自然语言的话，就是：
		// “给我创建一个按 string 排序、value 为 []*revOp 的 B-Tree Map”。
		//
		// 之所以这里不用普通的 Go map，是因为 byKey 不只是做精确查找，还要支持
		// 有序遍历、前缀扫描、从 startKey 继续往后扫等操作；这些都更适合有序的 B-Tree。
		byKey:            btree.NewMap[string, []*revOp](0),
		byRev:            map[int64]*revOp{},
		stream:           make(chan server.Events, 128),
		syncEveryWrite:   config.SyncEveryWrite,
		snapshotEvery:    config.SnapshotEvery,
		segmentBytes:     config.SegmentBytes,
		compactMinRetain: config.CompactMinRetain,
		cond:             sync.NewCond(&sync.Mutex{}),
	}
	log.initPaths()
	return log
}

// Start 打开或创建磁盘状态，并从 snapshot + journal 中恢复出内存索引。
//
// 启动顺序刻意分成几个阶段：
//  1. 确保目录存在
//  2. 获取单写者锁
//  3. 读取 metadata，扫描 snapshot / journal
//  4. 先加载最新 snapshot 作为基线
//  5. 再回放其后的 journal 增量
//  6. 对 brand-new store 应用 Kine 兼容启动基线
func (f *FSLog) Start(ctx context.Context) error {
	if f.rootDir == "" {
		return fmt.Errorf("filesystem backend requires root directory")
	}

	// 第 1 阶段：保证后续会用到的目录结构已经存在。
	if err := f.ensureLayout(); err != nil {
		return err
	}

	// 第 2 阶段：在真正读写共享目录之前先拿独占锁，落实“单写者”假设。
	if err := f.acquireLock(); err != nil {
		return err
	}

	// 第 3 阶段：读取轻量元数据，并找出当前可参与恢复的文件集合。
	if err := f.loadMetadata(); err != nil {
		f.releaseResources()
		return err
	}
	if err := f.scanState(); err != nil {
		f.releaseResources()
		return err
	}

	// 第 4 阶段：先加载最新 snapshot 作为内存基线，再回放之后的 journal。
	if err := f.loadLatestSnapshot(); err != nil {
		f.releaseResources()
		return err
	}
	if err := f.replayJournal(); err != nil {
		f.releaseResources()
		return err
	}

	// currentRev 可能同时受 metadata、snapshot、journal replay 三方影响，
	// 所以这里统一取最大值作为恢复后的可见边界。
	currentRev := maxInt64(maxInt64(f.files.metadata.CurrentRevision, f.segment.replayedRevision), f.segment.loadedSnapshotRev)

	// brand-new store 需要补一条兼容记录，让 fresh store 的启动 revision
	// 语义与其他 Kine 后端保持一致。
	if err := f.bootstrapCompatibilityRevision(currentRev); err != nil {
		f.releaseResources()
		return err
	}

	// 兼容记录本身可能推进了 metadata.CurrentRevision，所以这里再算一次。
	currentRev = maxInt64(maxInt64(f.files.metadata.CurrentRevision, f.segment.replayedRevision), f.segment.loadedSnapshotRev)
	f.currentRev.Store(currentRev)
	f.compactRev.Store(f.files.metadata.CompactRevision)
	f.appliedRev.Store(currentRev)
	f.files.metadata.CurrentRevision = currentRev

	// 后端生命周期跟随外层 context；context 结束时统一释放锁和文件句柄。
	go func() {
		<-ctx.Done()
		f.releaseResources()
	}()

	return nil
}

// ensureLayout 预先创建根目录、journal 目录和 snapshot 目录。
func (f *FSLog) ensureLayout() error {
	for _, dir := range []string{f.rootDir, f.files.journalDir, f.files.snapshotDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("create fs backend directory %q: %w", dir, err)
		}
	}
	return nil
}

// acquireLock 获取 backend 根目录上的 advisory exclusive lock。
// 第一版就是靠它来保证“一个数据目录同一时刻只有一个写者进程”。
func (f *FSLog) acquireLock() error {
	file, err := os.OpenFile(f.files.lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open fs backend lock file: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return ErrLocked
		}
		return fmt.Errorf("lock fs backend directory: %w", err)
	}
	f.files.lockFile = file
	return nil
}

// releaseResources 关闭活跃 segment，并释放目录锁。
// 它是幂等的，所以重复调用也没关系。
func (f *FSLog) releaseResources() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeSegmentLocked()
	if f.files.lockFile == nil {
		return
	}
	_ = syscall.Flock(int(f.files.lockFile.Fd()), syscall.LOCK_UN)
	_ = f.files.lockFile.Close()
	f.files.lockFile = nil
}

// loadMetadata 读取 metadata.json；如果文件不存在，就把它当成 brand-new store。
func (f *FSLog) loadMetadata() error {
	data, err := os.ReadFile(f.files.metadataPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			f.files.metadata = metadata{}
			return nil
		}
		return fmt.Errorf("read fs backend metadata: %w", err)
	}
	var meta metadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return fmt.Errorf("decode fs backend metadata: %w", err)
	}
	f.files.metadata = meta
	return nil
}

// scanState 扫描当前 snapshot / journal 文件集合，并记录为有序路径列表。
func (f *FSLog) scanState() error {
	snapshots, err := collectFileNames(f.files.snapshotDir, snapshotFileSuffix)
	if err != nil {
		return fmt.Errorf("scan snapshot directory: %w", err)
	}
	journals, err := collectFileNames(f.files.journalDir, journalFileSuffix)
	if err != nil {
		return fmt.Errorf("scan journal directory: %w", err)
	}
	f.files.snapshotFiles = snapshots
	f.files.journalFiles = journals

	// 如果 metadata 里还没有 active segment，就默认把扫描到的最新 journal
	// 视为当前活跃文件。
	if f.files.metadata.ActiveSegment == "" && len(journals) > 0 {
		f.files.metadata.ActiveSegment = filepath.Base(journals[len(journals)-1])
	}
	return nil
}

// collectFileNames 扫描一个目录，按后缀过滤文件，并丢掉中间临时文件。
func collectFileNames(dir string, suffix string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, tempFileSuffix) {
			continue
		}
		if suffix != "" && !strings.HasSuffix(name, suffix) {
			continue
		}
		files = append(files, filepath.Join(dir, name))
	}
	sort.Strings(files)
	return files, nil
}

// maxInt64 是启动恢复时的小工具函数，用来合并多个来源上的 revision 边界。
func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// bootstrapRecord 返回 fresh store 兼容逻辑所需的那条内部记录。
func bootstrapRecord() JournalRecord {
	return JournalRecord{
		Revision:       1,
		Key:            "compact_rev_key",
		Create:         true,
		CreateRevision: 1,
		Value:          []byte(""),
	}
}

// bootstrapCompatibilityRevision 在 brand-new store 上补写 Kine 历史兼容记录。
func (f *FSLog) bootstrapCompatibilityRevision(currentRev int64) error {
	if currentRev != 0 {
		return nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if len(f.byRev) != 0 {
		return nil
	}
	if _, ok := f.byKey.Get("compact_rev_key"); ok {
		return nil
	}

	// 现有 Kine 主要后端的 fresh store 启动时，revision 1 会被内部
	// compact revision key 占用；随后 `/registry/health` 会落到 revision 2。
	// fslog 实际上把 compact 状态存进 metadata，但这里仍然补写 legacy
	// compact_rev_key，这样启动基线行为就和其他后端保持一致。
	record := bootstrapRecord()
	if err := f.appendRecordLocked(record); err != nil {
		return err
	}
	f.applyRecordLocked(record)
	f.files.metadata.CurrentRevision = record.Revision
	return f.writeMetadataLocked()
}

// CompactRevision 返回当前 compact 到了哪个 revision。
func (f *FSLog) CompactRevision(context.Context) (int64, error) {
	return f.compactRev.Load(), nil
}

// CurrentRevision 返回当前对外可见的最新 revision。
func (f *FSLog) CurrentRevision(context.Context) (int64, error) {
	return f.currentRev.Load(), nil
}

// DbSize 当前还没有真正统计文件后端占用空间，只是满足接口要求。
func (f *FSLog) DbSize(context.Context) (int64, error) {
	return 0, nil
}
