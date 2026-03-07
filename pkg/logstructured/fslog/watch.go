package fslog

import (
	"context"
	"strings"

	"github.com/k3s-io/kine/pkg/server"
)

// Watch 订阅实时事件流，然后在本地按精确 key / prefix 再过滤一次。
//
// 历史追平并不在这里做，而是由 `logstructured` 上层先调用 `After(...)` 完成；
// fslog 的 Watch 只负责“新的实时事件”。
func (f *FSLog) Watch(ctx context.Context, prefix string) <-chan server.Events {
	result := make(chan server.Events, 100)
	values, err := f.broadcaster.Subscribe(ctx, f.startWatch)
	if err != nil {
		close(result)
		return result
	}

	// 这里沿用 Kine 的历史约定：以 `/` 结尾的模式被视为 prefix watch。
	checkPrefix := strings.HasSuffix(prefix, "/")
	go func() {
		defer close(result)
		for events := range values {
			filtered, ok := filterWatchEvents(events, checkPrefix, prefix)
			if ok {
				result <- filtered
			}
		}
	}()

	return result
}

// WaitForSyncTo 会一直阻塞，直到指定 revision 已经应用到内存索引中。
func (f *FSLog) WaitForSyncTo(revision int64) {
	f.cond.L.Lock()
	defer f.cond.L.Unlock()
	for f.appliedRev.Load() < revision {
		f.cond.Wait()
	}
}

// startWatch 由 broadcaster 在首个 watcher 订阅时调用，用来暴露底层共享流。
func (f *FSLog) startWatch() (chan server.Events, error) {
	f.watchStarted.Store(true)
	return f.stream, nil
}

// emitEvents 把已经提交完成的新事件推到共享流上。
func (f *FSLog) emitEvents(events server.Events) {
	if len(events) == 0 || !f.watchStarted.Load() {
		return
	}
	f.stream <- events
}

// filterWatchEvents 负责把共享事件流裁剪成某个 watcher 真正关心的子集。
func filterWatchEvents(eventList server.Events, checkPrefix bool, prefix string) (server.Events, bool) {
	filtered := make(server.Events, 0, len(eventList))
	for _, event := range eventList {
		if (checkPrefix && strings.HasPrefix(event.KV.Key, prefix)) || event.KV.Key == prefix {
			filtered = append(filtered, event)
		}
	}
	return filtered, len(filtered) > 0
}
