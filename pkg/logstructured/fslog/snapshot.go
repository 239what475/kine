package fslog

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/tidwall/btree"
)

// snapshotNameForRevision 让 snapshot 文件名按 revision 字典序可排序。
func snapshotNameForRevision(revision int64) string {
	return fmt.Sprintf("%020d%s", revision, snapshotFileSuffix)
}

// maybeWriteSnapshotLocked 只在 revision 命中 snapshot_interval 边界时触发快照。
func (f *FSLog) maybeWriteSnapshotLocked(revision int64) {
	if f.snapshotEvery <= 0 || revision == 0 || revision%f.snapshotEvery != 0 {
		return
	}
	_ = f.writeSnapshotLocked(revision)
}

// writeSnapshotLocked 把“当前仍被保留的内存记录集合”整体写成一个 snapshot，
// 然后切换到新的 journal segment。
func (f *FSLog) writeSnapshotLocked(revision int64) error {
	snapshot := SnapshotFile{
		CurrentRevision: revision,
		CompactRevision: f.compactRev.Load(),
		Records:         f.snapshotRecordsLocked(revision),
	}

	finalPath := filepath.Join(f.files.snapshotDir, snapshotNameForRevision(revision))
	tmpPath := finalPath + tempFileSuffix

	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("encode snapshot: %w", err)
	}

	// 先写临时文件，再 rename 成正式文件，避免留下半写入的快照文件。
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open snapshot temp file %q: %w", tmpPath, err)
	}
	if _, err := file.Write(append(data, '\n')); err != nil {
		file.Close()
		return fmt.Errorf("write snapshot temp file %q: %w", tmpPath, err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("sync snapshot temp file %q: %w", tmpPath, err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close snapshot temp file %q: %w", tmpPath, err)
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return fmt.Errorf("rename snapshot temp file %q: %w", tmpPath, err)
	}
	if err := syncDir(f.files.snapshotDir); err != nil {
		return err
	}

	if !containsPath(f.files.snapshotFiles, finalPath) {
		f.files.snapshotFiles = append(f.files.snapshotFiles, finalPath)
		sort.Strings(f.files.snapshotFiles)
	}

	// snapshot 落稳之后，把 journal 切到下一个 revision 开始的新 segment，
	// 这样后续 replay 更容易跳过旧历史。
	f.closeSegmentLocked()
	nextSegment := segmentNameForRevision(revision + 1)
	if err := f.openSegmentLocked(nextSegment, revision+1); err != nil {
		return err
	}
	f.files.metadata.CurrentRevision = revision
	if f.files.metadata.CompactRevision < snapshot.CompactRevision {
		f.files.metadata.CompactRevision = snapshot.CompactRevision
	}
	return f.writeMetadataLocked()
}

// snapshotRecordsLocked 把当前 revision 以内、仍保留在 byRev 中的记录导出到 snapshot。
func (f *FSLog) snapshotRecordsLocked(revision int64) []JournalRecord {
	revisions := make([]int64, 0, len(f.byRev))
	for rev := range f.byRev {
		if rev <= revision {
			revisions = append(revisions, rev)
		}
	}
	sort.Slice(revisions, func(i, j int) bool { return revisions[i] < revisions[j] })
	records := make([]JournalRecord, 0, len(revisions))
	for _, rev := range revisions {
		records = append(records, recordFromOp(f.byRev[rev]))
	}
	return records
}

// loadLatestSnapshot 把最新的 snapshot 加载成当前内存索引的起点状态。
func (f *FSLog) loadLatestSnapshot() error {
	f.segment.loadedSnapshotRev = 0
	if len(f.files.snapshotFiles) == 0 {
		return nil
	}
	path := f.files.snapshotFiles[len(f.files.snapshotFiles)-1]
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read snapshot %q: %w", path, err)
	}
	var snapshot SnapshotFile
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("decode snapshot %q: %w", path, err)
	}

	// snapshot 直接替换当前内存基线；之后 journal replay 再叠加更新的历史。
	f.byKey = btree.NewMap[string, []*revOp](0)
	f.byRev = map[int64]*revOp{}
	for _, record := range snapshot.Records {
		f.applyRecordLocked(record)
	}
	f.segment.loadedSnapshotRev = snapshot.CurrentRevision
	if snapshot.CurrentRevision > f.files.metadata.CurrentRevision {
		f.files.metadata.CurrentRevision = snapshot.CurrentRevision
	}
	if snapshot.CompactRevision > f.files.metadata.CompactRevision {
		f.files.metadata.CompactRevision = snapshot.CompactRevision
	}

	// 如果 metadata 里记录的 active segment 已经完全被 snapshot 覆盖掉，
	// 就清空这个提示，让后续 replay / reopen 自行选择合适的文件。
	if activeStart, ok := parseRevisionPrefix(f.files.metadata.ActiveSegment, journalFileSuffix); ok && activeStart <= snapshot.CurrentRevision {
		f.files.metadata.ActiveSegment = ""
	}
	return nil
}

// recordFromOp 把内存中的 revOp 转回稳定的记录格式，供 snapshot / journal 使用。
func recordFromOp(op *revOp) JournalRecord {
	if op == nil {
		return JournalRecord{}
	}
	return JournalRecord{
		Revision:       op.revision,
		Key:            op.key,
		Create:         op.create,
		Delete:         op.delete,
		CreateRevision: op.createRevision,
		PrevRevision:   op.prevRevision,
		Lease:          op.lease,
		Value:          cloneBytes(op.value),
		PrevValue:      cloneBytes(op.prevValue),
	}
}

// parseRevisionPrefix 从类似 `00000000000000001000.log` 的文件名中解析 revision 前缀。
func parseRevisionPrefix(path string, suffix string) (int64, bool) {
	name := filepath.Base(path)
	if !strings.HasSuffix(name, suffix) {
		return 0, false
	}
	prefix := strings.TrimSuffix(name, suffix)
	rev, err := strconv.ParseInt(prefix, 10, 64)
	if err != nil {
		return 0, false
	}
	return rev, true
}

// syncDir 对目录本身做 fsync，保证 rename / create 这类目录项更新真正落盘。
func syncDir(dir string) error {
	file, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open directory %q for sync: %w", dir, err)
	}
	defer file.Close()
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync directory %q: %w", dir, err)
	}
	return nil
}
