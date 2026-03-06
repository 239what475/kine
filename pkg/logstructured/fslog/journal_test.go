package fslog

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/k3s-io/kine/pkg/server"
)

func TestAppendCreatesJournalAndMetadata(t *testing.T) {
	rootDir := t.TempDir()
	log := New(Config{RootDir: rootDir, SegmentBytes: 1 << 20, SyncEveryWrite: true})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := log.Start(ctx); err != nil {
		t.Fatal(err)
	}

	rev, err := log.Append(ctx, &server.Event{Create: true, KV: &server.KeyValue{Key: "/a", Value: []byte("value")}})
	if err != nil {
		t.Fatal(err)
	}
	if rev != 1 {
		t.Fatalf("expected revision 1, got %d", rev)
	}
	if log.metadata.ActiveSegment != segmentNameForRevision(1) {
		t.Fatalf("expected active segment %q, got %q", segmentNameForRevision(1), log.metadata.ActiveSegment)
	}
	if _, err := os.Stat(filepath.Join(log.journalDir, log.metadata.ActiveSegment)); err != nil {
		t.Fatal(err)
	}
	metadataBytes, err := os.ReadFile(log.metadataPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(metadataBytes), `"currentRevision": 1`) {
		t.Fatalf("expected metadata to include currentRevision 1, got %s", metadataBytes)
	}
}

func TestAppendReplayOnRestart(t *testing.T) {
	rootDir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	log := New(Config{RootDir: rootDir, SegmentBytes: 1 << 20, SyncEveryWrite: true})
	if err := log.Start(ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := log.Append(ctx, &server.Event{KV: &server.KeyValue{Key: "/a", Value: []byte("value")}}); err != nil {
			t.Fatal(err)
		}
	}
	cancel()
	log.releaseResources()

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	log2 := New(Config{RootDir: rootDir, SegmentBytes: 1 << 20, SyncEveryWrite: true})
	if err := log2.Start(ctx2); err != nil {
		t.Fatal(err)
	}
	if got := log2.currentRev.Load(); got != 2 {
		t.Fatalf("expected current revision 2 after replay, got %d", got)
	}
	if len(log2.byRev) != 2 {
		t.Fatalf("expected 2 replayed revisions, got %d", len(log2.byRev))
	}
}

func TestAppendRotatesSegment(t *testing.T) {
	rootDir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := New(Config{RootDir: rootDir, SegmentBytes: 1, SyncEveryWrite: true})
	if err := log.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(ctx, &server.Event{KV: &server.KeyValue{Key: "/a", Value: []byte("value")}}); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(ctx, &server.Event{KV: &server.KeyValue{Key: "/b", Value: []byte("value")}}); err != nil {
		t.Fatal(err)
	}
	if len(log.journalFiles) != 2 {
		t.Fatalf("expected 2 journal files after rotation, got %d", len(log.journalFiles))
	}
}

func TestReplayTruncatesPartialFinalLine(t *testing.T) {
	rootDir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	log := New(Config{RootDir: rootDir, SegmentBytes: 1 << 20, SyncEveryWrite: true})
	if err := log.Start(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := log.Append(ctx, &server.Event{KV: &server.KeyValue{Key: "/a", Value: []byte("value")}}); err != nil {
		t.Fatal(err)
	}
	segmentPath := filepath.Join(log.journalDir, log.metadata.ActiveSegment)
	if file, err := os.OpenFile(segmentPath, os.O_WRONLY|os.O_APPEND, 0); err != nil {
		t.Fatal(err)
	} else {
		if _, err := file.Write([]byte(`{"revision":2,"key":"/broken"`)); err != nil {
			file.Close()
			t.Fatal(err)
		}
		file.Close()
	}
	cancel()
	log.releaseResources()

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	log2 := New(Config{RootDir: rootDir, SegmentBytes: 1 << 20, SyncEveryWrite: true})
	if err := log2.Start(ctx2); err != nil {
		t.Fatal(err)
	}
	if got := log2.currentRev.Load(); got != 1 {
		t.Fatalf("expected replayed current revision 1, got %d", got)
	}
	data, err := os.ReadFile(segmentPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "/broken") {
		t.Fatalf("expected broken tail to be truncated, got %s", data)
	}
}
