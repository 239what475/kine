package fslog

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/k3s-io/kine/pkg/server"
)

func TestListCountAndCurrentRevision(t *testing.T) {
	log := newStartedFSLogForReadTests(t)

	revB := mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/b", Value: []byte("b1")}})
	revA := mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/a", Value: []byte("a1")}})
	mustAppendEvent(t, log, &server.Event{KV: &server.KeyValue{Key: "/a", Value: []byte("a2"), CreateRevision: revA}, PrevKV: &server.KeyValue{Key: "/a", Value: []byte("a1"), CreateRevision: revA, ModRevision: revA}})
	mustAppendEvent(t, log, &server.Event{Delete: true, KV: &server.KeyValue{Key: "/b", Value: []byte("b1"), CreateRevision: revB, ModRevision: revB}, PrevKV: &server.KeyValue{Key: "/b", Value: []byte("b1"), CreateRevision: revB, ModRevision: revB}})

	currentRev, err := log.CurrentRevision(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if currentRev != 4 {
		t.Fatalf("expected current revision 4, got %d", currentRev)
	}

	rev, events, err := log.List(context.Background(), "/%", "", 0, 0, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if rev != 4 {
		t.Fatalf("expected list rev 4, got %d", rev)
	}
	assertEventKeys(t, events, "/a")
	if got := string(events[0].KV.Value); got != "a2" {
		t.Fatalf("expected latest value a2, got %q", got)
	}

	rev, historical, err := log.List(context.Background(), "/%", "", 0, 2, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if rev != 4 {
		t.Fatalf("expected historical list to report current rev 4, got %d", rev)
	}
	assertEventKeys(t, historical, "/a", "/b")
	if got := string(historical[0].KV.Value); got != "a1" {
		t.Fatalf("expected historical value a1, got %q", got)
	}

	rev, keysOnly, err := log.List(context.Background(), "/%", "", 0, 0, false, true)
	if err != nil {
		t.Fatal(err)
	}
	if rev != 4 {
		t.Fatalf("expected keysOnly list rev 4, got %d", rev)
	}
	if len(keysOnly) != 1 || len(keysOnly[0].KV.Value) != 0 {
		t.Fatalf("expected keysOnly list to omit values, got %+v", keysOnly)
	}

	rev, count, err := log.Count(context.Background(), "/", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if rev != 4 || count != 1 {
		t.Fatalf("expected count rev=4 count=1, got rev=%d count=%d", rev, count)
	}

	rev, historicalCount, err := log.Count(context.Background(), "/", "", 2)
	if err != nil {
		t.Fatal(err)
	}
	if rev != 4 || historicalCount != 2 {
		t.Fatalf("expected historical count rev=4 count=2, got rev=%d count=%d", rev, historicalCount)
	}
}

func TestAfterReturnsOrderedEvents(t *testing.T) {
	log := newStartedFSLogForReadTests(t)

	revB := mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/b", Value: []byte("b1")}})
	revA := mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/a", Value: []byte("a1")}})
	mustAppendEvent(t, log, &server.Event{KV: &server.KeyValue{Key: "/a", Value: []byte("a2"), CreateRevision: revA}, PrevKV: &server.KeyValue{Key: "/a", Value: []byte("a1"), CreateRevision: revA, ModRevision: revA}})
	mustAppendEvent(t, log, &server.Event{Delete: true, KV: &server.KeyValue{Key: "/b", Value: []byte("b1"), CreateRevision: revB, ModRevision: revB}, PrevKV: &server.KeyValue{Key: "/b", Value: []byte("b1"), CreateRevision: revB, ModRevision: revB}})

	rev, events, err := log.After(context.Background(), "/", 1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if rev != 4 {
		t.Fatalf("expected after rev 4, got %d", rev)
	}
	if len(events) != 3 {
		t.Fatalf("expected 3 events after revision 1, got %d", len(events))
	}
	assertEventKeys(t, events, "/a", "/a", "/b")
	if !events[0].Create {
		t.Fatal("expected first event to be create")
	}
	if events[1].Create || events[1].Delete {
		t.Fatal("expected second event to be update")
	}
	if !events[2].Delete {
		t.Fatal("expected third event to be delete")
	}
	if events[1].PrevKV == nil || string(events[1].PrevKV.Value) != "a1" {
		t.Fatalf("expected update event PrevKV value a1, got %+v", events[1].PrevKV)
	}
}

func TestListHandlesEscapedExactKeyAndRevisionErrors(t *testing.T) {
	log := newStartedFSLogForReadTests(t)
	mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/a_b", Value: []byte("v1")}})
	mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/c", Value: []byte("v2")}})
	log.compactRev.Store(2)

	_, events, err := log.List(context.Background(), "/a^_b", "", 1, 0, false, false)
	if err != nil {
		t.Fatal(err)
	}
	assertEventKeys(t, events, "/a_b")

	rev, _, err := log.List(context.Background(), "/%", "", 0, 10, false, false)
	if !errors.Is(err, server.ErrFutureRev) {
		t.Fatalf("expected ErrFutureRev, got rev=%d err=%v", rev, err)
	}

	rev, _, err = log.List(context.Background(), "/%", "", 0, 1, false, false)
	if !errors.Is(err, server.ErrCompacted) {
		t.Fatalf("expected ErrCompacted, got rev=%d err=%v", rev, err)
	}

	rev, _, err = log.Count(context.Background(), "/", "", 1)
	if !errors.Is(err, server.ErrCompacted) {
		t.Fatalf("expected count ErrCompacted, got rev=%d err=%v", rev, err)
	}

	rev, _, err = log.After(context.Background(), "/", 1, 0)
	if !errors.Is(err, server.ErrCompacted) {
		t.Fatalf("expected after ErrCompacted, got rev=%d err=%v", rev, err)
	}
}

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

func mustAppendEvent(t *testing.T, log *FSLog, event *server.Event) int64 {
	t.Helper()
	rev, err := log.Append(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	return rev
}

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
