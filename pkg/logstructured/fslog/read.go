package fslog

import (
	"context"
	"strings"

	"github.com/k3s-io/kine/pkg/server"
)

func (f *FSLog) List(ctx context.Context, prefix, startKey string, limit, revision int64, includeDeleted, keysOnly bool) (int64, server.Events, error) {
	_ = ctx

	prefix, startKey = normalizeLegacyRootList(prefix, startKey, limit)

	f.mu.RLock()
	defer f.mu.RUnlock()

	currentRev := f.currentRev.Load()
	compactRev := f.compactRev.Load()
	if revision > currentRev {
		return currentRev, nil, server.ErrFutureRev
	}
	if revision > 0 && revision < compactRev {
		return currentRev, nil, server.ErrCompacted
	}

	targetRevision := currentRev
	if revision > 0 {
		targetRevision = revision
	}

	ops := f.listOpsLocked(prefix, startKey, targetRevision, includeDeleted, limit)
	events := make(server.Events, 0, len(ops))
	for _, op := range ops {
		events = append(events, eventFromOp(op, !keysOnly, false))
	}
	return currentRev, events, nil
}

func (f *FSLog) Count(ctx context.Context, prefix, startKey string, revision int64) (int64, int64, error) {
	_ = ctx

	f.mu.RLock()
	defer f.mu.RUnlock()

	currentRev := f.currentRev.Load()
	compactRev := f.compactRev.Load()
	if revision > currentRev {
		return currentRev, 0, server.ErrFutureRev
	}
	if revision > 0 && revision < compactRev {
		return currentRev, 0, server.ErrCompacted
	}

	targetRevision := currentRev
	if revision > 0 {
		targetRevision = revision
	}

	matchPrefix, prefixMode := normalizeCountPattern(prefix)
	ops := f.listOpsForPatternLocked(matchPrefix, prefixMode, startKey, targetRevision, false, 0)
	return currentRev, int64(len(ops)), nil
}

func (f *FSLog) After(ctx context.Context, prefix string, revision, limit int64) (int64, server.Events, error) {
	_ = ctx

	f.mu.RLock()
	defer f.mu.RUnlock()

	currentRev := f.currentRev.Load()
	compactRev := f.compactRev.Load()
	if revision > 0 && revision < compactRev {
		return currentRev, nil, server.ErrCompacted
	}

	matchPrefix, prefixMode := normalizeWatchPattern(prefix)
	events := make(server.Events, 0)
	for nextRevision := revision + 1; nextRevision <= currentRev; nextRevision++ {
		op := f.byRev[nextRevision]
		if op == nil {
			continue
		}
		if !matchesPattern(op.key, matchPrefix, prefixMode) {
			continue
		}
		events = append(events, eventFromOp(op, true, true))
		if limit > 0 && int64(len(events)) >= limit {
			break
		}
	}
	return currentRev, events, nil
}

func (f *FSLog) listOpsLocked(pattern, startKey string, revision int64, includeDeleted bool, limit int64) []*revOp {
	matchPrefix, prefixMode := normalizeListPattern(pattern)
	return f.listOpsForPatternLocked(matchPrefix, prefixMode, startKey, revision, includeDeleted, limit)
}

func (f *FSLog) listOpsForPatternLocked(matchPrefix string, prefixMode bool, startKey string, revision int64, includeDeleted bool, limit int64) []*revOp {
	if !prefixMode {
		op := f.getRevisionOpLocked(matchPrefix, revision, includeDeleted)
		if op == nil {
			return nil
		}
		return []*revOp{op}
	}

	it := f.byKey.Iter()
	seekKey := matchPrefix
	if startKey != "" && startKey > seekKey {
		seekKey = startKey
	}

	var ok bool
	if seekKey != "" {
		ok = it.Seek(seekKey)
	} else {
		ok = it.First()
	}
	if !ok {
		return nil
	}

	results := make([]*revOp, 0)
	for {
		key := it.Key()
		if !strings.HasPrefix(key, matchPrefix) {
			break
		}
		if startKey != "" && key < startKey {
			if !it.Next() {
				break
			}
			continue
		}
		if op := latestOpAtRevision(it.Value(), revision, includeDeleted); op != nil {
			results = append(results, op)
			if limit > 0 && int64(len(results)) >= limit {
				break
			}
		}
		if !it.Next() {
			break
		}
	}

	return results
}

func (f *FSLog) getRevisionOpLocked(key string, revision int64, includeDeleted bool) *revOp {
	ops, ok := f.byKey.Get(key)
	if !ok {
		return nil
	}
	return latestOpAtRevision(ops, revision, includeDeleted)
}

func latestOpAtRevision(ops []*revOp, revision int64, includeDeleted bool) *revOp {
	for index := len(ops) - 1; index >= 0; index-- {
		op := ops[index]
		if revision > 0 && op.revision > revision {
			continue
		}
		if op.delete && !includeDeleted {
			return nil
		}
		return op
	}
	return nil
}

func eventFromOp(op *revOp, includeValue, includePrevValue bool) *server.Event {
	createRevision := op.createRevision
	if createRevision == 0 {
		createRevision = op.revision
	}

	event := &server.Event{
		Create: op.create,
		Delete: op.delete,
		KV: &server.KeyValue{
			Key:            op.key,
			CreateRevision: createRevision,
			ModRevision:    op.revision,
			Lease:          op.lease,
		},
	}
	if includeValue {
		event.KV.Value = append([]byte(nil), op.value...)
	}
	if op.create {
		return event
	}

	event.PrevKV = &server.KeyValue{
		Key:            op.key,
		CreateRevision: createRevision,
		ModRevision:    op.prevRevision,
		Lease:          op.lease,
	}
	if includePrevValue {
		event.PrevKV.Value = append([]byte(nil), op.prevValue...)
	}
	return event
}

func normalizeLegacyRootList(pattern, startKey string, limit int64) (string, string) {
	// Kine's TTL bootstrap path calls Log.List directly with pattern="/" and uses the
	// last returned key as the next page's startKey. Existing backends interpret that as
	// a root-prefix scan with continue-token semantics, even though LogStructured.List is
	// normally the layer that makes those semantics explicit.
	//
	// Keep that compatibility local to fslog so exact single-key reads for "/" (limit=1)
	// stay exact, while the legacy root scan used by TTL initialization still behaves the
	// same as the older SQL/NATS-backed implementations.
	if pattern != "/" || limit <= 1 {
		return pattern, startKey
	}
	if startKey != "" && !strings.HasSuffix(startKey, "\x00") {
		startKey += "\x00"
	}
	return "/%", startKey
}

func normalizeListPattern(pattern string) (string, bool) {
	pattern = strings.ReplaceAll(pattern, "^_", "_")
	if strings.HasSuffix(pattern, "%") {
		return strings.TrimSuffix(pattern, "%"), true
	}
	return pattern, false
}

func normalizeCountPattern(pattern string) (string, bool) {
	pattern = strings.ReplaceAll(pattern, "^_", "_")
	if strings.HasSuffix(pattern, "%") {
		return strings.TrimSuffix(pattern, "%"), true
	}
	if strings.HasSuffix(pattern, "/") {
		return pattern, true
	}
	return pattern, false
}

func normalizeWatchPattern(pattern string) (string, bool) {
	pattern = strings.ReplaceAll(pattern, "^_", "_")
	if strings.HasSuffix(pattern, "%") {
		return strings.TrimSuffix(pattern, "%"), true
	}
	if strings.HasSuffix(pattern, "/") {
		return pattern, true
	}
	return pattern, false
}

func matchesPattern(key, pattern string, prefixMode bool) bool {
	if prefixMode {
		return strings.HasPrefix(key, pattern)
	}
	return key == pattern
}
