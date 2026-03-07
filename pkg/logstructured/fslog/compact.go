package fslog

import (
	"context"
	"os"
	"path/filepath"
	"sort"

	"github.com/tidwall/btree"
)

// safeCompactRevision 会把请求的 compact revision 收紧到安全范围内，
// 避免越过 `compact_min_retain` 要求保留的历史窗口。
func safeCompactRevision(targetCompactRev int64, currentRev int64, compactMinRetain int64) int64 {
	safeRev := currentRev - compactMinRetain
	if targetCompactRev < safeRev {
		safeRev = targetCompactRev
	}
	if safeRev < 0 {
		safeRev = 0
	}
	return safeRev
}

// Compact 在新的 compaction 边界上重建内存基线、写出 snapshot，并清理旧 journal。
func (f *FSLog) Compact(ctx context.Context, targetCompactRev int64) (int64, error) {
	_ = ctx

	f.mu.Lock()
	defer f.mu.Unlock()

	currentRev := f.currentRev.Load()
	if currentRev == 0 {
		return 0, nil
	}

	// 先把调用方请求的 compact revision 收紧成一个安全值。
	targetCompactRev = safeCompactRevision(targetCompactRev, currentRev, f.compactMinRetain)
	if targetCompactRev <= f.compactRev.Load() {
		return currentRev, nil
	}

	// 先重建内存基线，再写 snapshot，最后删掉彻底过时的 journal 文件。
	f.compactLocked(targetCompactRev)
	f.compactRev.Store(targetCompactRev)
	f.metadata.CompactRevision = targetCompactRev
	if err := f.writeSnapshotLocked(currentRev); err != nil {
		return currentRev, err
	}
	if err := f.cleanupCompactedJournalLocked(); err != nil {
		return currentRev, err
	}

	return currentRev, nil
}

// compactLocked 根据给定的 compact 边界重建 byKey / byRev 两个索引。
func (f *FSLog) compactLocked(compactRevision int64) {
	compactedByKey := btree.NewMap[string, []*revOp](0)
	compactedByRev := map[int64]*revOp{}

	it := f.byKey.Iter()
	for ok := it.First(); ok; ok = it.Next() {
		kept := compactOps(it.Value(), compactRevision)
		if len(kept) == 0 {
			continue
		}
		compactedByKey.Set(it.Key(), kept)
		for _, op := range kept {
			compactedByRev[op.revision] = op
		}
	}

	f.byKey = compactedByKey
	f.byRev = compactedByRev
}

// compactOps 决定某个 key 在 compaction 后还应保留哪些历史。
func compactOps(ops []*revOp, compactRevision int64) []*revOp {
	if len(ops) == 0 {
		return nil
	}

	// 找到 compact 边界之前最后一条记录，它将成为这个 key 的压缩基线。
	baselineIndex := -1
	for index := len(ops) - 1; index >= 0; index-- {
		if ops[index].revision <= compactRevision {
			baselineIndex = index
			break
		}
	}

	result := make([]*revOp, 0, len(ops))
	if baselineIndex >= 0 {
		baseline := ops[baselineIndex]

		// 如果基线本身是 delete，说明 compact 后这个 key 应该先消失，除非后面
		// 有更新的 recreate 记录。
		if !baseline.delete {
			result = append(result, cloneRevOp(baseline))
		}
		for index := baselineIndex + 1; index < len(ops); index++ {
			if ops[index].revision > compactRevision {
				result = append(result, cloneRevOp(ops[index]))
			}
		}
		return result
	}

	// 如果 compact 边界之前没有任何记录，就只保留边界之后的新历史。
	for _, op := range ops {
		if op.revision > compactRevision {
			result = append(result, cloneRevOp(op))
		}
	}
	return result
}

// cloneRevOp 复制一条内存记录，避免新旧索引共享同一个底层对象。
func cloneRevOp(op *revOp) *revOp {
	if op == nil {
		return nil
	}
	return &revOp{
		revision:       op.revision,
		create:         op.create,
		delete:         op.delete,
		key:            op.key,
		createRevision: op.createRevision,
		prevRevision:   op.prevRevision,
		lease:          op.lease,
		value:          cloneBytes(op.value),
		prevValue:      cloneBytes(op.prevValue),
	}
}

// cleanupCompactedJournalLocked 删除已经完全过时的 journal 文件，只保留活跃 segment。
func (f *FSLog) cleanupCompactedJournalLocked() error {
	activePath := filepath.Join(f.journalDir, f.metadata.ActiveSegment)
	for _, path := range f.journalFiles {
		if path == activePath {
			continue
		}
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	f.journalFiles = nil
	if f.metadata.ActiveSegment != "" {
		f.journalFiles = append(f.journalFiles, activePath)
	}
	sort.Strings(f.journalFiles)
	return syncDir(f.journalDir)
}
