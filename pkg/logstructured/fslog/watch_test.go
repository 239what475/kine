package fslog

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/server"
)

// TestWatchFromCurrentRevision 验证从当前 revision 开始 watch 时，只会收到后续新增事件。
func TestWatchFromCurrentRevision(t *testing.T) {
	log := newStartedFSLogForReadTests(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 不指定历史 revision，watch 应该直接订阅实时广播流。
	watchCh := log.Watch(ctx, "/")
	rev := mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/watch/a", Value: []byte("one")}})

	events := mustReceiveEvents(t, watchCh)
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].KV.Key != "/watch/a" || events[0].KV.ModRevision != rev {
		t.Fatalf("unexpected watch event %+v", events[0].KV)
	}
}

// TestWatchFromHistoricalRevisionUsesAfterPlusWatch 验证历史 watch 会先补历史，再切到实时流。
func TestWatchFromHistoricalRevisionUsesAfterPlusWatch(t *testing.T) {
	ctx := context.Background()
	backend := newBackendForWriteTests(t)
	createRev, err := backend.Create(ctx, "/hist/a", []byte("one"), 0)
	if err != nil {
		t.Fatal(err)
	}
	rev2, _, ok, err := backend.Update(ctx, "/hist/a", []byte("two"), createRev, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected update to succeed")
	}

	// 从 revision=2 开始 watch，此时应该先收到 create/update 两条历史事件。
	watchCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wr := backend.Watch(watchCtx, "/hist/", 2)
	if wr.CompactRevision != 0 {
		t.Fatalf("unexpected compact revision %d", wr.CompactRevision)
	}

	select {
	case events := <-wr.Events:
		if len(events) != 2 || events[0].KV.ModRevision != createRev || events[1].KV.ModRevision != rev2 {
			t.Fatalf("unexpected historical watch events %+v", events)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for historical watch events")
	}

	_, _, ok, err = backend.Update(ctx, "/hist/a", []byte("three"), rev2, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected second update to succeed")
	}

	select {
	case events := <-wr.Events:
		if len(events) != 1 || string(events[0].KV.Value) != "three" {
			t.Fatalf("unexpected follow-up watch events %+v", events)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for live watch events")
	}
}

// TestWatchRespectsPrefix 验证 watch 的前缀过滤只放行命中的 key。
func TestWatchRespectsPrefix(t *testing.T) {
	log := newStartedFSLogForReadTests(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watchCh := log.Watch(ctx, "/prefix/")
	mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/other/a", Value: []byte("skip")}})
	mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/prefix/a", Value: []byte("hit")}})

	events := mustReceiveEvents(t, watchCh)
	if len(events) != 1 || events[0].KV.Key != "/prefix/a" {
		t.Fatalf("unexpected prefix-filtered events %+v", events)
	}
}

// TestWaitForSyncToBlocksUntilRevisionApplied 验证 WaitForSyncTo 会等到目标 revision 真正应用完成。
func TestWaitForSyncToBlocksUntilRevisionApplied(t *testing.T) {
	log := newStartedFSLogForReadTests(t)
	finished := make(chan struct{})

	// 先在后台等待 revision=2，此时 fresh store 只有 bootstrap 记录，因此应该阻塞。
	go func() {
		log.WaitForSyncTo(2)
		close(finished)
	}()

	select {
	case <-finished:
		t.Fatal("wait should block before revision is applied")
	case <-time.After(50 * time.Millisecond):
	}

	mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/wait/a", Value: []byte("one")}})
	mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/wait/b", Value: []byte("two")}})

	select {
	case <-finished:
	case <-time.After(2 * time.Second):
		t.Fatal("wait did not complete after revision applied")
	}
}

// TestWatchSlowConsumerDoesNotBlockWrites 验证慢消费者不会把写路径拖死。
func TestWatchSlowConsumerDoesNotBlockWrites(t *testing.T) {
	log := newStartedFSLogForReadTests(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 这里不消费返回的 channel，模拟广播端遇到一个非常慢的 watcher。
	_ = log.Watch(ctx, "/slow/")
	done := make(chan struct{})
	go func() {
		defer close(done)
		for index := 0; index < 256; index++ {
			mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: fmt.Sprintf("/slow/%03d", index), Value: []byte("v")}})
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("slow consumer caused writes to block")
	}
}

// mustReceiveEvents 在统一超时时间内读取一批 watch 事件。
func mustReceiveEvents(t *testing.T, ch <-chan server.Events) server.Events {
	t.Helper()
	select {
	case events := <-ch:
		return events
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for watch events")
		return nil
	}
}
