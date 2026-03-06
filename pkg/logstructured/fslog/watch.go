package fslog

import (
	"context"
	"strings"

	"github.com/k3s-io/kine/pkg/server"
)

func (f *FSLog) Watch(ctx context.Context, prefix string) <-chan server.Events {
	result := make(chan server.Events, 100)
	values, err := f.broadcaster.Subscribe(ctx, f.startWatch)
	if err != nil {
		close(result)
		return result
	}

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

func (f *FSLog) WaitForSyncTo(revision int64) {
	f.cond.L.Lock()
	defer f.cond.L.Unlock()
	for f.appliedRev.Load() < revision {
		f.cond.Wait()
	}
}

func (f *FSLog) startWatch() (chan server.Events, error) {
	f.watchStarted.Store(true)
	return f.stream, nil
}

func (f *FSLog) emitEvents(events server.Events) {
	if len(events) == 0 || !f.watchStarted.Load() {
		return
	}
	f.stream <- events
}

func filterWatchEvents(eventList server.Events, checkPrefix bool, prefix string) (server.Events, bool) {
	filtered := make(server.Events, 0, len(eventList))
	for _, event := range eventList {
		if (checkPrefix && strings.HasPrefix(event.KV.Key, prefix)) || event.KV.Key == prefix {
			filtered = append(filtered, event)
		}
	}
	return filtered, len(filtered) > 0
}
