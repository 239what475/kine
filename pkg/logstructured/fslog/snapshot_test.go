package fslog

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/k3s-io/kine/pkg/server"
)

func TestSnapshotWrittenAtConfiguredInterval(t *testing.T) {
	log := newStartedFSLogWithConfig(t, Config{RootDir: t.TempDir(), SegmentBytes: 1 << 20, SyncEveryWrite: true, SnapshotEvery: 2})

	createRev := mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/snap/a", Value: []byte("one")}})
	mustAppendEvent(t, log, &server.Event{KV: &server.KeyValue{Key: "/snap/a", Value: []byte("two"), CreateRevision: createRev}, PrevKV: &server.KeyValue{Key: "/snap/a", Value: []byte("one"), CreateRevision: createRev, ModRevision: createRev}})

	if len(log.snapshotFiles) != 1 {
		t.Fatalf("expected 1 snapshot file, got %d", len(log.snapshotFiles))
	}
	snapshotPath := log.snapshotFiles[0]
	if _, err := os.Stat(snapshotPath); err != nil {
		t.Fatal(err)
	}
	if log.metadata.ActiveSegment != segmentNameForRevision(3) {
		t.Fatalf("expected active segment %q after snapshot, got %q", segmentNameForRevision(3), log.metadata.ActiveSegment)
	}
	if _, err := os.Stat(filepath.Join(log.journalDir, log.metadata.ActiveSegment)); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(snapshotPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"currentRevision": 2`) {
		t.Fatalf("expected snapshot to record current revision 2, got %s", data)
	}
}

func TestSnapshotAndJournalRecovery(t *testing.T) {
	rootDir := t.TempDir()
	log := newStartedFSLogWithConfig(t, Config{RootDir: rootDir, SegmentBytes: 1 << 20, SyncEveryWrite: true, SnapshotEvery: 2})

	createRev := mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/snap/a", Value: []byte("one")}})
	mustAppendEvent(t, log, &server.Event{KV: &server.KeyValue{Key: "/snap/a", Value: []byte("two"), CreateRevision: createRev}, PrevKV: &server.KeyValue{Key: "/snap/a", Value: []byte("one"), CreateRevision: createRev, ModRevision: createRev}})
	mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/snap/b", Value: []byte("three")}})

	oldSegment := filepath.Join(log.journalDir, segmentNameForRevision(1))
	if err := os.WriteFile(oldSegment, []byte("corrupted old segment should be skipped"), 0o600); err != nil {
		t.Fatal(err)
	}
	log.releaseResources()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	restarted := New(Config{RootDir: rootDir, SegmentBytes: 1 << 20, SyncEveryWrite: true, SnapshotEvery: 2})
	if err := restarted.Start(ctx); err != nil {
		t.Fatal(err)
	}

	rev, events, err := restarted.List(context.Background(), "/%", "", 0, 0, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if rev != 3 {
		t.Fatalf("expected recovered revision 3, got %d", rev)
	}
	assertEventKeys(t, events, "/snap/a", "/snap/b")

	rev, historical, err := restarted.List(context.Background(), "/%", "", 0, 2, false, false)
	if err != nil {
		t.Fatal(err)
	}
	if rev != 3 {
		t.Fatalf("expected historical list to report current revision 3, got %d", rev)
	}
	assertEventKeys(t, historical, "/snap/a")
	if got := string(historical[0].KV.Value); got != "two" {
		t.Fatalf("expected historical value two, got %q", got)
	}
}

func TestStartupIgnoresInterruptedSnapshotTempFile(t *testing.T) {
	rootDir := t.TempDir()
	log := newStartedFSLogWithConfig(t, Config{RootDir: rootDir, SegmentBytes: 1 << 20, SyncEveryWrite: true, SnapshotEvery: 2})

	mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/snap/a", Value: []byte("one")}})
	mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/snap/b", Value: []byte("two")}})

	brokenTmp := filepath.Join(log.snapshotDir, snapshotNameForRevision(99)+tempFileSuffix)
	if err := os.WriteFile(brokenTmp, []byte(`{not-json`), 0o600); err != nil {
		t.Fatal(err)
	}
	log.releaseResources()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	restarted := New(Config{RootDir: rootDir, SegmentBytes: 1 << 20, SyncEveryWrite: true, SnapshotEvery: 2})
	if err := restarted.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if got := restarted.currentRev.Load(); got != 2 {
		t.Fatalf("expected revision 2 after restart with temp snapshot file, got %d", got)
	}
}

func newStartedFSLogWithConfig(t *testing.T, config Config) *FSLog {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	log := New(config)
	if err := log.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return log
}
