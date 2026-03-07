package fslog

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/k3s-io/kine/pkg/server"
)

// TestListCountAndCurrentRevision 验证当前视图下的 list、count 和 current revision 行为。
func TestListCountAndCurrentRevision(t *testing.T) {
	log := newStartedFSLogForReadTests(t)
	// fresh store 启动时已经包含 compact_rev_key 基线，因此后续 revision 都从这个基础上累加。
	baseRev, err := log.CurrentRevision(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	revB := mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/b", Value: []byte("b1")}})
	revA := mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/a", Value: []byte("a1")}})
	mustAppendEvent(t, log, &server.Event{KV: &server.KeyValue{Key: "/a", Value: []byte("a2"), CreateRevision: revA}, PrevKV: &server.KeyValue{Key: "/a", Value: []byte("a1"), CreateRevision: revA, ModRevision: revA}})
	mustAppendEvent(t, log, &server.Event{Delete: true, KV: &server.KeyValue{Key: "/b", Value: []byte("b1"), CreateRevision: revB, ModRevision: revB}, PrevKV: &server.KeyValue{Key: "/b", Value: []byte("b1"), CreateRevision: revB, ModRevision: revB}})

	currentRev, err := log.CurrentRevision(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if currentRev != baseRev+4 {
		t.Fatalf("expected current revision %d, got %d", baseRev+4, currentRev)
	}

	rev, events, err := log.List(context.Background(), "/%", "", 0, 0, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if rev != baseRev+4 {
		t.Fatalf("expected list rev %d, got %d", baseRev+4, rev)
	}
	assertEventKeys(t, events, "/a")
	if got := string(events[0].KV.Value); got != "a2" {
		t.Fatalf("expected latest value a2, got %q", got)
	}

	rev, historical, err := log.List(context.Background(), "/%", "", 0, baseRev+2, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if rev != baseRev+4 {
		t.Fatalf("expected historical list to report current rev %d, got %d", baseRev+4, rev)
	}
	assertEventKeys(t, historical, "/a", "/b")
	if got := string(historical[0].KV.Value); got != "a1" {
		t.Fatalf("expected historical value a1, got %q", got)
	}

	rev, keysOnly, err := log.List(context.Background(), "/%", "", 0, 0, false, true)
	if err != nil {
		t.Fatal(err)
	}
	if rev != baseRev+4 {
		t.Fatalf("expected keysOnly list rev %d, got %d", baseRev+4, rev)
	}
	if len(keysOnly) != 1 || len(keysOnly[0].KV.Value) != 0 {
		t.Fatalf("expected keysOnly list to omit values, got %+v", keysOnly)
	}

	rev, count, err := log.Count(context.Background(), "/", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if rev != baseRev+4 || count != 1 {
		t.Fatalf("expected count rev=%d count=1, got rev=%d count=%d", baseRev+4, rev, count)
	}

	rev, historicalCount, err := log.Count(context.Background(), "/", "", baseRev+2)
	if err != nil {
		t.Fatal(err)
	}
	if rev != baseRev+4 || historicalCount != 2 {
		t.Fatalf("expected historical count rev=%d count=2, got rev=%d count=%d", baseRev+4, rev, historicalCount)
	}
}

// TestAfterReturnsOrderedEvents 验证 After 会按 revision 顺序回放事件，并保留 create/update/delete 语义。
func TestAfterReturnsOrderedEvents(t *testing.T) {
	log := newStartedFSLogForReadTests(t)
	baseRev, err := log.CurrentRevision(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	revB := mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/b", Value: []byte("b1")}})
	revA := mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/a", Value: []byte("a1")}})
	mustAppendEvent(t, log, &server.Event{KV: &server.KeyValue{Key: "/a", Value: []byte("a2"), CreateRevision: revA}, PrevKV: &server.KeyValue{Key: "/a", Value: []byte("a1"), CreateRevision: revA, ModRevision: revA}})
	mustAppendEvent(t, log, &server.Event{Delete: true, KV: &server.KeyValue{Key: "/b", Value: []byte("b1"), CreateRevision: revB, ModRevision: revB}, PrevKV: &server.KeyValue{Key: "/b", Value: []byte("b1"), CreateRevision: revB, ModRevision: revB}})

	rev, events, err := log.After(context.Background(), "/", baseRev, 0)
	if err != nil {
		t.Fatal(err)
	}
	if rev != baseRev+4 {
		t.Fatalf("expected after rev %d, got %d", baseRev+4, rev)
	}
	if len(events) != 4 {
		t.Fatalf("expected 4 events after revision %d, got %d", baseRev, len(events))
	}
	assertEventKeys(t, events, "/b", "/a", "/a", "/b")
	if !events[0].Create {
		t.Fatal("expected first event to be create")
	}
	if !events[1].Create {
		t.Fatal("expected second event to be create")
	}
	if events[2].Create || events[2].Delete {
		t.Fatal("expected third event to be update")
	}
	if !events[3].Delete {
		t.Fatal("expected fourth event to be delete")
	}
	if events[2].PrevKV == nil || string(events[2].PrevKV.Value) != "a1" {
		t.Fatalf("expected update event PrevKV value a1, got %+v", events[2].PrevKV)
	}
}

// TestListHandlesEscapedExactKeyAndRevisionErrors 验证精确 key 转义、future rev 和 compacted rev 的返回值。
func TestListHandlesEscapedExactKeyAndRevisionErrors(t *testing.T) {
	log := newStartedFSLogForReadTests(t)
	baseRev, err := log.CurrentRevision(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/a_b", Value: []byte("v1")}})
	mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/c", Value: []byte("v2")}})
	// 人工推进 compactRev，用来验证旧 revision 会返回 ErrCompacted。
	log.compactRev.Store(baseRev + 1)

	_, events, err := log.List(context.Background(), "/a^_b", "", 1, 0, false, false)
	if err != nil {
		t.Fatal(err)
	}
	assertEventKeys(t, events, "/a_b")

	rev, _, err := log.List(context.Background(), "/%", "", 0, baseRev+9, false, false)
	if !errors.Is(err, server.ErrFutureRev) {
		t.Fatalf("expected ErrFutureRev, got rev=%d err=%v", rev, err)
	}

	rev, _, err = log.List(context.Background(), "/%", "", 0, baseRev, false, false)
	if !errors.Is(err, server.ErrCompacted) {
		t.Fatalf("expected ErrCompacted, got rev=%d err=%v", rev, err)
	}

	rev, _, err = log.Count(context.Background(), "/", "", baseRev)
	if !errors.Is(err, server.ErrCompacted) {
		t.Fatalf("expected count ErrCompacted, got rev=%d err=%v", rev, err)
	}

	rev, _, err = log.After(context.Background(), "/", baseRev, 0)
	if !errors.Is(err, server.ErrCompacted) {
		t.Fatalf("expected after ErrCompacted, got rev=%d err=%v", rev, err)
	}
}

// newStartedFSLogForReadTests 创建一个已启动的 FSLog，供读路径和 watch 测试共用。
func newStartedFSLogForReadTests(t *testing.T) *FSLog {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	log := New(Config{RootDir: t.TempDir(), SegmentBytes: 1 << 20, SyncEveryWrite: true})
	if err := log.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return log
}

// mustAppendEvent 包装 Append，测试里直接拿返回 revision 做断言。
func mustAppendEvent(t *testing.T, log *FSLog, event *server.Event) int64 {
	t.Helper()
	rev, err := log.Append(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	return rev
}

// assertEventKeys 把事件序列转成 key 列表，便于验证顺序和过滤结果。
func assertEventKeys(t *testing.T, events server.Events, expected ...string) {
	t.Helper()
	keys := make([]string, 0, len(events))
	for _, event := range events {
		keys = append(keys, event.KV.Key)
	}
	if !reflect.DeepEqual(keys, expected) {
		t.Fatalf("expected keys %v, got %v", expected, keys)
	}
}

// TestListAndCountUseContinueTokenStartKey 验证带 \x00 的 startKey 会按 continue token 语义跳过当前 key。
func TestListAndCountUseContinueTokenStartKey(t *testing.T) {
	log := newStartedFSLogForReadTests(t)
	baseRev, err := log.CurrentRevision(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/a", Value: []byte("a1")}})
	mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/b", Value: []byte("b1")}})
	mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/c", Value: []byte("c1")}})

	rev, events, err := log.List(context.Background(), "/%", "/a\x00", 0, 0, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if rev != baseRev+3 {
		t.Fatalf("expected list rev %d, got %d", baseRev+3, rev)
	}
	assertEventKeys(t, events, "/b", "/c")

	rev, count, err := log.Count(context.Background(), "/", "/a\x00", 0)
	if err != nil {
		t.Fatal(err)
	}
	if rev != baseRev+3 || count != 2 {
		t.Fatalf("expected count rev=%d count=2, got rev=%d count=%d", baseRev+3, rev, count)
	}
}

// TestListAndCountKeepInclusiveStartKey 验证普通 startKey 仍保持包含边界的历史兼容语义。
func TestListAndCountKeepInclusiveStartKey(t *testing.T) {
	log := newStartedFSLogForReadTests(t)
	baseRev, err := log.CurrentRevision(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/a", Value: []byte("a1")}})
	mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/b", Value: []byte("b1")}})
	mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/c", Value: []byte("c1")}})

	rev, events, err := log.List(context.Background(), "/%", "/a", 0, 0, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if rev != baseRev+3 {
		t.Fatalf("expected list rev %d, got %d", baseRev+3, rev)
	}
	assertEventKeys(t, events, "/a", "/b", "/c")

	rev, count, err := log.Count(context.Background(), "/", "/a", 0)
	if err != nil {
		t.Fatal(err)
	}
	if rev != baseRev+3 || count != 3 {
		t.Fatalf("expected count rev=%d count=3, got rev=%d count=%d", baseRev+3, rev, count)
	}
}

// TestLegacyRootListCompatibilityForTTL 验证 legacy root list 兼容逻辑，确保 TTL bootstrap 仍能分页扫描。
func TestLegacyRootListCompatibilityForTTL(t *testing.T) {
	log := newStartedFSLogForReadTests(t)
	baseRev, err := log.CurrentRevision(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/registry/health", Value: []byte("ok")}})
	mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/pods/a", Value: []byte("a1")}})
	mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/pods/b", Value: []byte("b1")}})

	// limit=1 时仍保持精确 key 语义，不能把 "/" 直接当成全前缀扫描。
	rev, exact, err := log.List(context.Background(), "/", "", 1, 0, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if rev != baseRev+3 {
		t.Fatalf("expected exact root list rev %d, got %d", baseRev+3, rev)
	}
	if len(exact) != 0 {
		t.Fatalf("expected exact list for / to stay empty, got %d events", len(exact))
	}

	// limit 足够大时，legacy root list 会切到兼容模式，把 "/" 当作 root 前缀。
	rev, page, err := log.List(context.Background(), "/", "", 1000, 0, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if rev != baseRev+3 {
		t.Fatalf("expected root compatibility list rev %d, got %d", baseRev+3, rev)
	}
	assertEventKeys(t, page, "/pods/a", "/pods/b", "/registry/health")

	rev, nextPage, err := log.List(context.Background(), "/", page[0].KV.Key, 1000, rev, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if rev != baseRev+3 {
		t.Fatalf("expected paged root compatibility list rev %d, got %d", baseRev+3, rev)
	}
	assertEventKeys(t, nextPage, "/pods/b", "/registry/health")
}
