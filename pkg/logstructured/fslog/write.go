package fslog

import (
	"context"
	"fmt"

	"github.com/k3s-io/kine/pkg/server"
)

// Append 是 logstructured 层要求的唯一写入口。
//
// 整条写路径的顺序刻意保持严格：
//  1. 读取当前 key 状态
//  2. 分配下一个 revision
//  3. 把高层事件归一化成单条 JournalRecord
//  4. 先落 journal
//  5. 再更新内存索引
//  6. 最后广播给 watcher
//
// 这样做的核心目的是：崩溃恢复时，总可以从 journal replay 回来。
func (f *FSLog) Append(ctx context.Context, event *server.Event) (int64, error) {
	_ = ctx
	if event == nil || event.KV == nil {
		return 0, fmt.Errorf("filesystem backend append requires event kv")
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// 先拿到 key 的当前可见状态；后面的 create/update/delete 冲突检查都依赖它。
	current := f.getRevisionOpLocked(event.KV.Key, f.currentRev.Load(), true)
	nextRev := f.currentRev.Load() + 1

	// 把高层 server.Event 收敛成一条稳定的持久化记录格式。
	record, err := recordFromEvent(nextRev, event, current)
	if err != nil {
		return 0, err
	}

	// 先写磁盘，再更新内存索引。这样即使进程在中间崩掉，也能靠 replay 恢复。
	if err := f.appendRecordLocked(record); err != nil {
		return 0, err
	}

	// 一旦 journal 追加成功，就把它同步映射到内存索引。
	f.applyRecordLocked(record)
	f.metadata.CurrentRevision = nextRev

	// metadata 是小型辅助状态；journal 才是核心事实来源。
	// 当前实现延续已有行为：如果 metadata 重写失败，这次 append 仍然返回
	// `nextRev, nil`，但后面的 currentRev / appliedRev / watch 广播不会继续推进。
	// 也就是说，这条记录已经进入 journal，并可在重启后通过 replay 恢复，
	// 但当前进程内不一定会把它完整暴露成“已提交可见”的最新 revision。
	if err := f.writeMetadataLocked(); err != nil {
		return nextRev, nil
	}

	f.currentRev.Store(nextRev)
	f.appliedRev.Store(nextRev)

	// 生成 watcher 所需的事件形式，并在 revision 变得可见后再广播。
	f.emitEvents(server.Events{eventFromOp(f.byRev[nextRev], true, true)})
	f.cond.Broadcast()

	// snapshot 不是每次都写，而是按阈值 opportunistic 触发。
	f.maybeWriteSnapshotLocked(nextRev)
	return nextRev, nil
}

// recordFromEvent 把 `server.Event` 翻译成一条稳定的 journal 记录。
//
// 三种分支分别对应：
//   - create：只有 key 当前不存在或已经删除时才能成功
//   - delete：本质上是 compare-and-delete
//   - update：本质上是 compare-and-swap，并且要保留原 createRevision
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
		// create 只允许发生在“当前没有活 key”的情况下。
		if current != nil && !current.delete {
			return JournalRecord{}, server.ErrKeyExists
		}
		record.Create = true
		record.CreateRevision = revision
		record.Lease = event.KV.Lease
		record.Value = cloneBytes(event.KV.Value)

		// 如果这个 key 以前存在过但后来被删了，把前序上下文也带上，便于后续
		// replay / watch 正确重建事件语义。
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
		// delete 是一种 compare-and-delete：key 必须存在，并且如果调用方带了
		// 前序 revision，也必须和当前最新状态一致。
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

		// delete 事件需要保留当前值，这样 replay 后仍能构造出带 PrevKV 的删除事件。
		record.Value = cloneBytes(current.value)
		record.PrevValue = cloneBytes(current.value)
		return record, nil

	default:
		// update 也是 compare-and-swap：key 必须存在，且任何由调用方提供的版本
		// 条件都必须仍然成立。
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

// cloneBytes 用来打断底层切片别名，避免调用方传入的 buffer 在外部被继续修改。
func cloneBytes(value []byte) []byte {
	return append([]byte(nil), value...)
}
