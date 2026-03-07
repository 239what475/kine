package fslog

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// appendRecordLocked 把一条 JournalRecord 追加到当前活跃 segment。
// 调用方必须已经持有写锁。
func (f *FSLog) appendRecordLocked(record JournalRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode journal record: %w", err)
	}
	data = append(data, '\n')

	// 先确保当前有一个可写 segment，必要时会新建或滚动 segment。
	if err := f.ensureWritableSegmentLocked(record.Revision, int64(len(data))); err != nil {
		return err
	}
	written, err := f.segmentWriter.Write(data)
	if err != nil {
		return fmt.Errorf("write journal record: %w", err)
	}
	if err := f.segmentWriter.Flush(); err != nil {
		return fmt.Errorf("flush journal record: %w", err)
	}
	if f.syncEveryWrite {
		if err := f.segmentFile.Sync(); err != nil {
			return fmt.Errorf("sync journal record: %w", err)
		}
	}
	f.segmentSize += int64(written)
	if !containsPath(f.journalFiles, filepath.Join(f.journalDir, f.segmentName)) {
		f.journalFiles = append(f.journalFiles, filepath.Join(f.journalDir, f.segmentName))
	}
	return nil
}

// ensureWritableSegmentLocked 保证当前存在一个可写 segment，必要时会触发滚动。
func (f *FSLog) ensureWritableSegmentLocked(nextRevision int64, recordSize int64) error {
	if f.segmentFile == nil {
		name := f.metadata.ActiveSegment
		if name == "" {
			name = segmentNameForRevision(nextRevision)
		}
		if err := f.openSegmentLocked(name, nextRevision); err != nil {
			return err
		}
	}
	if f.segmentSize > 0 && f.segmentSize+recordSize > f.segmentBytes {
		f.closeSegmentLocked()
		name := segmentNameForRevision(nextRevision)
		if err := f.openSegmentLocked(name, nextRevision); err != nil {
			return err
		}
	}
	return nil
}

// openSegmentLocked 打开或创建一个 journal segment，并把它设为当前活跃文件。
func (f *FSLog) openSegmentLocked(name string, startRevision int64) error {
	path := filepath.Join(f.journalDir, name)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("open journal segment %q: %w", path, err)
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return fmt.Errorf("stat journal segment %q: %w", path, err)
	}
	f.segmentFile = file
	f.segmentWriter = bufio.NewWriter(file)
	f.segmentName = name
	f.segmentSize = info.Size()
	f.segmentStartRev = startRevision
	f.metadata.ActiveSegment = name
	return nil
}

// closeSegmentLocked 关闭当前活跃的 segment 文件和 writer。
func (f *FSLog) closeSegmentLocked() {
	if f.segmentWriter != nil {
		_ = f.segmentWriter.Flush()
	}
	if f.segmentFile != nil {
		_ = f.segmentFile.Close()
	}
	f.segmentWriter = nil
	f.segmentFile = nil
	f.segmentName = ""
	f.segmentSize = 0
	f.segmentStartRev = 0
}

// segmentNameForRevision 让 segment 文件名和 revision 顺序在字典序上保持一致。
func segmentNameForRevision(revision int64) string {
	return fmt.Sprintf("%020d%s", revision, journalFileSuffix)
}

// replayJournal 会按顺序回放需要参与恢复的所有 journal 文件。
func (f *FSLog) replayJournal() error {
	f.replayedRevision = 0
	paths := f.journalFilesForReplay()
	for index, path := range paths {
		if err := f.replayJournalFile(path, index == len(paths)-1); err != nil {
			return err
		}
	}
	return nil
}

// replayJournalFile 回放单个 journal 文件。
//
// `allowTailRepair=true` 只会用于最后一个 journal 文件，目的是在进程崩溃留下
// 半条尾记录时，允许把坏尾巴截掉并继续恢复。
func (f *FSLog) replayJournalFile(path string, allowTailRepair bool) error {
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return fmt.Errorf("open journal file %q: %w", path, err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var offset int64
	for {
		line, err := reader.ReadBytes('\n')
		if err == io.EOF && len(line) == 0 {
			return nil
		}
		nextOffset := offset + int64(len(line))
		trimmed := bytes.TrimSpace(line)

		// 如果最后一条记录没有换行结尾，且当前允许修尾，就把这段坏尾截断。
		if err == io.EOF {
			if allowTailRepair {
				if terr := file.Truncate(offset); terr != nil {
					return fmt.Errorf("truncate partial journal tail %q: %w", path, terr)
				}
				return nil
			}
			return fmt.Errorf("unexpected partial journal tail in %q", path)
		}

		var record JournalRecord
		if uerr := json.Unmarshal(trimmed, &record); uerr != nil {
			// 同样地，如果最后一个文件尾部是坏 JSON，也允许在允许修尾时把它截断。
			if allowTailRepair {
				if terr := file.Truncate(offset); terr != nil {
					return fmt.Errorf("truncate invalid journal tail %q: %w", path, terr)
				}
				return nil
			}
			return fmt.Errorf("decode journal record in %q: %w", path, uerr)
		}

		// snapshot 已经覆盖掉的旧 revision 不需要再应用一次。
		if record.Revision <= f.loadedSnapshotRev {
			offset = nextOffset
			continue
		}

		f.applyRecordLocked(record)
		if record.Revision > f.replayedRevision {
			f.replayedRevision = record.Revision
		}
		offset = nextOffset
	}
}

// journalFilesForReplay 只返回 snapshot 之后仍然需要参与恢复的 journal 文件。
func (f *FSLog) journalFilesForReplay() []string {
	if f.loadedSnapshotRev == 0 {
		return append([]string(nil), f.journalFiles...)
	}
	paths := make([]string, 0, len(f.journalFiles))
	for _, path := range f.journalFiles {
		startRev, ok := parseRevisionPrefix(path, journalFileSuffix)
		if !ok || startRev > f.loadedSnapshotRev {
			paths = append(paths, path)
		}
	}
	return paths
}

// applyRecordLocked 把单条 journal 记录同步映射到 byRev / byKey 两个索引里。
func (f *FSLog) applyRecordLocked(record JournalRecord) {
	op := &revOp{
		revision:       record.Revision,
		create:         record.Create,
		delete:         record.Delete,
		key:            record.Key,
		createRevision: record.CreateRevision,
		prevRevision:   record.PrevRevision,
		lease:          record.Lease,
		value:          cloneBytes(record.Value),
		prevValue:      cloneBytes(record.PrevValue),
	}
	f.byRev[record.Revision] = op
	if values, ok := f.byKey.Get(record.Key); ok {
		values = append(values, op)
		f.byKey.Set(record.Key, values)
	} else {
		f.byKey.Set(record.Key, []*revOp{op})
	}
}

// writeMetadataLocked 重写 metadata.json。
func (f *FSLog) writeMetadataLocked() error {
	data, err := json.MarshalIndent(f.metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("encode metadata: %w", err)
	}
	return os.WriteFile(f.metadataPath, append(data, '\n'), 0o600)
}

// containsPath 是一个小工具函数，用来判断某个路径是否已经在切片中。
func containsPath(paths []string, path string) bool {
	for _, item := range paths {
		if item == path {
			return true
		}
	}
	return false
}
