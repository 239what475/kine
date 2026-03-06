package fslog

import (
	"context"
	"os"
	"path/filepath"
	"sort"

	"github.com/tidwall/btree"
)

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

func (f *FSLog) Compact(ctx context.Context, targetCompactRev int64) (int64, error) {
	_ = ctx

	f.mu.Lock()
	defer f.mu.Unlock()

	currentRev := f.currentRev.Load()
	if currentRev == 0 {
		return 0, nil
	}

	targetCompactRev = safeCompactRevision(targetCompactRev, currentRev, f.compactMinRetain)
	if targetCompactRev <= f.compactRev.Load() {
		return currentRev, nil
	}

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

func compactOps(ops []*revOp, compactRevision int64) []*revOp {
	if len(ops) == 0 {
		return nil
	}

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

	for _, op := range ops {
		if op.revision > compactRevision {
			result = append(result, cloneRevOp(op))
		}
	}
	return result
}

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
