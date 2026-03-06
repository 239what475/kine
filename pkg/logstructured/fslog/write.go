package fslog

import (
	"context"
	"fmt"

	"github.com/k3s-io/kine/pkg/server"
)

func (f *FSLog) Append(ctx context.Context, event *server.Event) (int64, error) {
	_ = ctx
	if event == nil || event.KV == nil {
		return 0, fmt.Errorf("filesystem backend append requires event kv")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	current := f.getRevisionOpLocked(event.KV.Key, f.currentRev.Load(), true)
	nextRev := f.currentRev.Load() + 1
	record, err := recordFromEvent(nextRev, event, current)
	if err != nil {
		return 0, err
	}
	if err := f.appendRecordLocked(record); err != nil {
		return 0, err
	}
	f.applyRecordLocked(record)
	f.metadata.CurrentRevision = nextRev
	if err := f.writeMetadataLocked(); err != nil {
		return nextRev, nil
	}
	f.currentRev.Store(nextRev)
	f.appliedRev.Store(nextRev)
	f.emitEvents(server.Events{eventFromOp(f.byRev[nextRev], true, true)})
	f.cond.Broadcast()
	f.maybeWriteSnapshotLocked(nextRev)
	return nextRev, nil
}

func recordFromEvent(revision int64, event *server.Event, current *revOp) (JournalRecord, error) {
	if event == nil || event.KV == nil || event.KV.Key == "" {
		return JournalRecord{}, fmt.Errorf("filesystem backend append requires non-empty key")
	}

	record := JournalRecord{
		Revision: revision,
		Key:      event.KV.Key,
	}

	switch {
	case event.Create:
		if current != nil && !current.delete {
			return JournalRecord{}, server.ErrKeyExists
		}
		record.Create = true
		record.CreateRevision = revision
		record.Lease = event.KV.Lease
		record.Value = cloneBytes(event.KV.Value)
		if current != nil {
			record.PrevRevision = current.revision
			record.PrevValue = cloneBytes(current.value)
		}
		if event.PrevKV != nil {
			if record.PrevRevision == 0 {
				record.PrevRevision = event.PrevKV.ModRevision
			}
			if len(record.PrevValue) == 0 && len(event.PrevKV.Value) > 0 {
				record.PrevValue = cloneBytes(event.PrevKV.Value)
			}
		}
		return record, nil
	case event.Delete:
		if current == nil || current.delete {
			return JournalRecord{}, ErrWriteConflict
		}
		if event.PrevKV != nil && event.PrevKV.ModRevision != 0 && event.PrevKV.ModRevision != current.revision {
			return JournalRecord{}, ErrWriteConflict
		}
		record.Delete = true
		record.CreateRevision = current.effectiveCreateRevision()
		record.PrevRevision = current.revision
		record.Lease = current.lease
		record.Value = cloneBytes(current.value)
		record.PrevValue = cloneBytes(current.value)
		return record, nil
	default:
		if current == nil || current.delete {
			return JournalRecord{}, ErrWriteConflict
		}
		if event.PrevKV != nil && event.PrevKV.ModRevision != 0 && event.PrevKV.ModRevision != current.revision {
			return JournalRecord{}, ErrWriteConflict
		}
		createRevision := current.effectiveCreateRevision()
		if event.KV.CreateRevision != 0 && event.KV.CreateRevision != createRevision {
			return JournalRecord{}, ErrWriteConflict
		}
		record.CreateRevision = createRevision
		record.PrevRevision = current.revision
		record.Lease = event.KV.Lease
		record.Value = cloneBytes(event.KV.Value)
		record.PrevValue = cloneBytes(current.value)
		return record, nil
	}
}

func cloneBytes(value []byte) []byte {
	return append([]byte(nil), value...)
}
