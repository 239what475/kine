package fslog

import (
	"context"
	"strings"

	"github.com/k3s-io/kine/pkg/server"
)

// List 返回某个 revision 视图下、匹配指定模式的一组事件。
//
// 这里返回的是 `server.Events`，因为上层 logstructured 语义层本身就是围绕
// event / revision 模型在工作，而不是直接围绕“当前 KV 字典”。
func (f *FSLog) List(ctx context.Context, prefix, startKey string, limit, revision int64, includeDeleted, keysOnly bool) (int64, server.Events, error) {
	_ = ctx

	// 这是一个局部兼容逻辑：Kine 的 TTL 初始化历史上会直接调用
	// Log.List("/", ...)，而不是走更显式的上层 prefix 语义包装。
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

	// revision=0 表示“当前视图”；否则就读指定历史视图。
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

// Count 统计某个 revision 视图下、匹配指定模式的 key 数量。
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

// After 返回某个 revision 之后发生的事件序列。
// 这是 watch 补历史阶段最关键的底层能力之一。
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

// listOpsLocked 是 List 的内部入口：先把 pattern 解释成精确 key 或 prefix，
// 再走统一的扫描逻辑。
func (f *FSLog) listOpsLocked(pattern, startKey string, revision int64, includeDeleted bool, limit int64) []*revOp {
	matchPrefix, prefixMode := normalizeListPattern(pattern)
	return f.listOpsForPatternLocked(matchPrefix, prefixMode, startKey, revision, includeDeleted, limit)
}

// listOpsForPatternLocked 根据模式在 byKey 上做字典序扫描，构造一个 revision
// 视图下的结果集。
func (f *FSLog) listOpsForPatternLocked(matchPrefix string, prefixMode bool, startKey string, revision int64, includeDeleted bool, limit int64) []*revOp {
	// 精确 key 读取时不用扫描整棵树，直接走单点查找即可。
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

		// startKey 语义是“从这个 key 开始继续扫描”；如果当前 key 还在它前面，
		// 就继续往后跳。
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

// getRevisionOpLocked 在指定 revision 视图下取某个 key 的最新状态。
func (f *FSLog) getRevisionOpLocked(key string, revision int64, includeDeleted bool) *revOp {
	ops, ok := f.byKey.Get(key)
	if !ok {
		return nil
	}
	return latestOpAtRevision(ops, revision, includeDeleted)
}

// latestOpAtRevision 从一个 key 的完整历史切片中，挑出某个 revision 视图下
// 真正可见的那一条记录。
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

// eventFromOp 把内存里的 revOp 再转换回上层需要的 server.Event。
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

	// 对 update / delete 这类事件，还要把上一版本的上下文一起带出来。
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

// normalizeLegacyRootList 只负责兼容 Kine 历史上的 TTL 初始化路径。
func normalizeLegacyRootList(pattern, startKey string, limit int64) (string, string) {
	// Kine 的 TTL bootstrap 路径会直接调用 Log.List("/")，并把上一页最后一个
	// key 直接作为下一页 startKey。现有 SQL/NATS 后端都把它解释成：
	//   - root prefix 扫描
	//   - continue-token 语义翻页
	//
	// 这里把兼容逻辑局部留在 fslog 内部，而不去修改共享层语义：
	//   - `List("/", ..., limit=1)` 仍保持精确 key 语义
	//   - TTL 初始化用到的 root 扫描则继续表现得和旧后端一致
	if pattern != "/" || limit <= 1 {
		return pattern, startKey
	}
	if startKey != "" && !strings.HasSuffix(startKey, "\x00") {
		startKey += "\x00"
	}
	return "/%", startKey
}

// normalizeListPattern 把 List 使用的模式翻译成：
//   - 精确 key
//   - 或 prefix 模式
func normalizeListPattern(pattern string) (string, bool) {
	pattern = strings.ReplaceAll(pattern, "^_", "_")
	if strings.HasSuffix(pattern, "%") {
		return strings.TrimSuffix(pattern, "%"), true
	}
	return pattern, false
}

// normalizeCountPattern 复用与 List 类似的规则，但额外把以 `/` 结尾的模式当作 prefix。
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

// normalizeWatchPattern 复用与 Count 相同的 prefix 解释习惯。
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

// matchesPattern 在精确模式和 prefix 模式之间统一做匹配判断。
func matchesPattern(key, pattern string, prefixMode bool) bool {
	if prefixMode {
		return strings.HasPrefix(key, pattern)
	}
	return key == pattern
}
