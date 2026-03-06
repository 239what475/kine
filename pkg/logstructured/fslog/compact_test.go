package fslog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/k3s-io/kine/pkg/server"
)

func TestCompactReturnsErrCompactedForOldRevisionAndPreservesCurrentView(t *testing.T) {
	log := newStartedFSLogWithConfig(t, Config{RootDir: t.TempDir(), SegmentBytes: 1 << 20, SyncEveryWrite: true, SnapshotEvery: 100, CompactMinRetain: 1})

	createA := mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/c/a", Value: []byte("a1")}})
	mustAppendEvent(t, log, &server.Event{KV: &server.KeyValue{Key: "/c/a", Value: []byte("a2"), CreateRevision: createA}, PrevKV: &server.KeyValue{Key: "/c/a", Value: []byte("a1"), CreateRevision: createA, ModRevision: createA}})
	createB := mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/c/b", Value: []byte("b1")}})
	currentRev := mustAppendEvent(t, log, &server.Event{KV: &server.KeyValue{Key: "/c/b", Value: []byte("b2"), CreateRevision: createB}, PrevKV: &server.KeyValue{Key: "/c/b", Value: []byte("b1"), CreateRevision: createB, ModRevision: createB}})

	rev, err := log.Compact(context.Background(), createB)
	if err != nil {
		t.Fatal(err)
	}
	if rev != currentRev {
		t.Fatalf("expected compact to return current revision %d, got %d", currentRev, rev)
	}
	if got := log.compactRev.Load(); got != createB {
		t.Fatalf("expected compact revision %d, got %d", createB, got)
	}

	rev, _, err = log.List(context.Background(), "/%", "", 0, createB-1, false, false)
	if !errors.Is(err, server.ErrCompacted) {
		t.Fatalf("expected ErrCompacted for old revision, got rev=%d err=%v", rev, err)
	}

	rev, events, err := log.List(context.Background(), "/%", "", 0, createB, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if rev != currentRev {
		t.Fatalf("expected list to report current rev %d, got %d", currentRev, rev)
	}
	assertEventKeys(t, events, "/c/a", "/c/b")
	if got := string(events[0].KV.Value); got != "a2" {
		t.Fatalf("expected compact baseline value a2, got %q", got)
	}
	if got := string(events[1].KV.Value); got != "b1" {
		t.Fatalf("expected compact baseline value b1 at rev %d, got %q", createB, got)
	}

	rev, currentEvents, err := log.List(context.Background(), "/%", "", 0, 0, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if rev != currentRev {
		t.Fatalf("expected current list rev %d, got %d", currentRev, rev)
	}
	assertEventKeys(t, currentEvents, "/c/a", "/c/b")
	if got := string(currentEvents[1].KV.Value); got != "b2" {
		t.Fatalf("expected current value b2, got %q", got)
	}
}

func TestCompactDropsFullyDeletedKeys(t *testing.T) {
	log := newStartedFSLogWithConfig(t, Config{RootDir: t.TempDir(), SegmentBytes: 1 << 20, SyncEveryWrite: true, SnapshotEvery: 100, CompactMinRetain: 1})

	createA := mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/drop/a", Value: []byte("a1")}})
	deleteA := mustAppendEvent(t, log, &server.Event{Delete: true, KV: &server.KeyValue{Key: "/drop/a", Value: []byte("a1"), CreateRevision: createA, ModRevision: createA}, PrevKV: &server.KeyValue{Key: "/drop/a", Value: []byte("a1"), CreateRevision: createA, ModRevision: createA}})
	currentRev := mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/drop/b", Value: []byte("b1")}})

	if _, err := log.Compact(context.Background(), deleteA); err != nil {
		t.Fatal(err)
	}
	if got := log.compactRev.Load(); got != deleteA {
		t.Fatalf("expected compact revision %d, got %d", deleteA, got)
	}

	_, events, err := log.List(context.Background(), "/%", "", 0, deleteA, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("expected fully deleted key to be dropped from compact baseline, got %+v", events)
	}

	rev, currentEvents, err := log.List(context.Background(), "/%", "", 0, 0, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if rev != currentRev {
		t.Fatalf("expected current revision %d, got %d", currentRev, rev)
	}
	assertEventKeys(t, currentEvents, "/drop/b")
}

func TestCompactWritesSnapshotAndCleansOldJournals(t *testing.T) {
	rootDir := t.TempDir()
	log := newStartedFSLogWithConfig(t, Config{RootDir: rootDir, SegmentBytes: 1, SyncEveryWrite: true, SnapshotEvery: 100, CompactMinRetain: 1})

	createA := mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/keep/a", Value: []byte("a1")}})
	updateA := mustAppendEvent(t, log, &server.Event{KV: &server.KeyValue{Key: "/keep/a", Value: []byte("a2"), CreateRevision: createA}, PrevKV: &server.KeyValue{Key: "/keep/a", Value: []byte("a1"), CreateRevision: createA, ModRevision: createA}})
	currentRev := mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/keep/b", Value: []byte("b1")}})

	if _, err := log.Compact(context.Background(), updateA); err != nil {
		t.Fatal(err)
	}
	if len(log.snapshotFiles) == 0 {
		t.Fatal("expected compaction to write snapshot")
	}
	if log.metadata.ActiveSegment != segmentNameForRevision(currentRev+1) {
		t.Fatalf("expected active segment %q, got %q", segmentNameForRevision(currentRev+1), log.metadata.ActiveSegment)
	}
	entries, err := os.ReadDir(filepath.Join(rootDir, journalDirName))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != segmentNameForRevision(currentRev+1) {
		t.Fatalf("expected only compacted active segment to remain, got %+v", entries)
	}

	log.releaseResources()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	restarted := New(Config{RootDir: rootDir, SegmentBytes: 1, SyncEveryWrite: true, SnapshotEvery: 100, CompactMinRetain: 1})
	if err := restarted.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if got := restarted.compactRev.Load(); got != updateA {
		t.Fatalf("expected recovered compact revision %d, got %d", updateA, got)
	}

	rev, _, err := restarted.List(context.Background(), "/%", "", 0, updateA-1, false, false)
	if !errors.Is(err, server.ErrCompacted) {
		t.Fatalf("expected ErrCompacted after restart, got rev=%d err=%v", rev, err)
	}
	_, events, err := restarted.List(context.Background(), "/%", "", 0, updateA, false, false)
	if err != nil {
		t.Fatal(err)
	}
	assertEventKeys(t, events, "/keep/a")
	if got := string(events[0].KV.Value); got != "a2" {
		t.Fatalf("expected compacted snapshot baseline value a2, got %q", got)
	}
}
