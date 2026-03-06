package fslog

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/k3s-io/kine/pkg/server"
)

func recordFromEvent(revision int64, event *server.Event) JournalRecord {
	record := JournalRecord{
		Revision: revision,
		Key:      event.KV.Key,
		Create:   event.Create,
		Delete:   event.Delete,
		Lease:    event.KV.Lease,
		Value:    append([]byte(nil), event.KV.Value...),
	}
	if event.Create {
		record.CreateRevision = revision
	} else {
		record.CreateRevision = event.KV.CreateRevision
	}
	if event.PrevKV != nil {
		record.PrevRevision = event.PrevKV.ModRevision
		record.PrevValue = append([]byte(nil), event.PrevKV.Value...)
	}
	return record
}

func (f *FSLog) appendRecordLocked(record JournalRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode journal record: %w", err)
	}
	data = append(data, '\n')

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

func segmentNameForRevision(revision int64) string {
	return fmt.Sprintf("%020d.log", revision)
}

func (f *FSLog) replayJournal() error {
	f.replayedRevision = 0
	for index, path := range f.journalFiles {
		if err := f.replayJournalFile(path, index == len(f.journalFiles)-1); err != nil {
			return err
		}
	}
	return nil
}

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
			if allowTailRepair {
				if terr := file.Truncate(offset); terr != nil {
					return fmt.Errorf("truncate invalid journal tail %q: %w", path, terr)
				}
				return nil
			}
			return fmt.Errorf("decode journal record in %q: %w", path, uerr)
		}

		f.applyRecordLocked(record)
		if record.Revision > f.replayedRevision {
			f.replayedRevision = record.Revision
		}
		offset = nextOffset
	}
}

func (f *FSLog) applyRecordLocked(record JournalRecord) {
	op := &revOp{
		revision:       record.Revision,
		create:         record.Create,
		delete:         record.Delete,
		key:            record.Key,
		createRevision: record.CreateRevision,
		prevRevision:   record.PrevRevision,
		lease:          record.Lease,
		value:          append([]byte(nil), record.Value...),
		prevValue:      append([]byte(nil), record.PrevValue...),
	}
	f.byRev[record.Revision] = op
	if values, ok := f.byKey.Get(record.Key); ok {
		values = append(values, op)
		f.byKey.Set(record.Key, values)
	} else {
		f.byKey.Set(record.Key, []*revOp{op})
	}
}

func (f *FSLog) writeMetadataLocked() error {
	data, err := json.MarshalIndent(f.metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("encode metadata: %w", err)
	}
	return os.WriteFile(f.metadataPath, append(data, '\n'), 0o600)
}

func containsPath(paths []string, path string) bool {
	for _, item := range paths {
		if item == path {
			return true
		}
	}
	return false
}
