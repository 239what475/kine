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

func snapshotNameForRevision(revision int64) string {
	return fmt.Sprintf("%020d%s", revision, snapshotFileSuffix)
}

func (f *FSLog) maybeWriteSnapshotLocked(revision int64) {
	if f.snapshotEvery <= 0 || revision == 0 || revision%f.snapshotEvery != 0 {
		return
	}
	_ = f.writeSnapshotLocked(revision)
}

func (f *FSLog) writeSnapshotLocked(revision int64) error {
	snapshot := SnapshotFile{
		CurrentRevision: revision,
		CompactRevision: f.compactRev.Load(),
		Records:         f.snapshotRecordsLocked(revision),
	}

	finalPath := filepath.Join(f.snapshotDir, snapshotNameForRevision(revision))
	tmpPath := finalPath + tempFileSuffix

	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("encode snapshot: %w", err)
	}

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
	if err := syncDir(f.snapshotDir); err != nil {
		return err
	}

	if !containsPath(f.snapshotFiles, finalPath) {
		f.snapshotFiles = append(f.snapshotFiles, finalPath)
		sort.Strings(f.snapshotFiles)
	}

	f.closeSegmentLocked()
	nextSegment := segmentNameForRevision(revision + 1)
	if err := f.openSegmentLocked(nextSegment, revision+1); err != nil {
		return err
	}
	f.metadata.CurrentRevision = revision
	if f.metadata.CompactRevision < snapshot.CompactRevision {
		f.metadata.CompactRevision = snapshot.CompactRevision
	}
	return f.writeMetadataLocked()
}

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

func (f *FSLog) loadLatestSnapshot() error {
	f.loadedSnapshotRev = 0
	if len(f.snapshotFiles) == 0 {
		return nil
	}
	path := f.snapshotFiles[len(f.snapshotFiles)-1]
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read snapshot %q: %w", path, err)
	}
	var snapshot SnapshotFile
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("decode snapshot %q: %w", path, err)
	}

	f.byKey = btree.NewMap[string, []*revOp](0)
	f.byRev = map[int64]*revOp{}
	for _, record := range snapshot.Records {
		f.applyRecordLocked(record)
	}
	f.loadedSnapshotRev = snapshot.CurrentRevision
	if snapshot.CurrentRevision > f.metadata.CurrentRevision {
		f.metadata.CurrentRevision = snapshot.CurrentRevision
	}
	if snapshot.CompactRevision > f.metadata.CompactRevision {
		f.metadata.CompactRevision = snapshot.CompactRevision
	}
	if activeStart, ok := parseRevisionPrefix(f.metadata.ActiveSegment, journalFileSuffix); ok && activeStart <= snapshot.CurrentRevision {
		f.metadata.ActiveSegment = ""
	}
	return nil
}

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
