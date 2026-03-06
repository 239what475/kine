package fslog

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStartCreatesDirectoryLayout(t *testing.T) {
	rootDir := filepath.Join(t.TempDir(), "store")
	log := New(Config{RootDir: rootDir})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := log.Start(ctx); err != nil {
		t.Fatal(err)
	}

	for _, path := range []string{rootDir, log.journalDir, log.snapshotDir, log.lockPath} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected %s to exist: %v", path, err)
		}
	}
	if log.lockFile == nil {
		t.Fatal("expected lock file to be held")
	}
}

func TestStartLoadsExistingMetadataAndScansDirectories(t *testing.T) {
	rootDir := t.TempDir()
	journalDir := filepath.Join(rootDir, journalDirName)
	snapshotDir := filepath.Join(rootDir, snapshotDirName)
	if err := os.MkdirAll(journalDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(snapshotDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rootDir, metadataFileName), []byte(`{"currentRevision":12,"compactRevision":4,"activeSegment":"0001.log"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(journalDir, "0002.log"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(snapshotDir, "0001.snapshot.json"), []byte(`{"currentRevision":0,"compactRevision":0,"records":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := New(Config{RootDir: rootDir})
	if err := log.Start(ctx); err != nil {
		t.Fatal(err)
	}

	if got := log.currentRev.Load(); got != 12 {
		t.Fatalf("expected current revision 12, got %d", got)
	}
	if got := log.compactRev.Load(); got != 4 {
		t.Fatalf("expected compact revision 4, got %d", got)
	}
	if got := log.appliedRev.Load(); got != 12 {
		t.Fatalf("expected applied revision 12, got %d", got)
	}
	if len(log.journalFiles) != 1 {
		t.Fatalf("expected 1 journal file, got %d", len(log.journalFiles))
	}
	if len(log.snapshotFiles) != 1 {
		t.Fatalf("expected 1 snapshot file, got %d", len(log.snapshotFiles))
	}
}

func TestStartRejectsLockedDirectory(t *testing.T) {
	rootDir := t.TempDir()

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	log1 := New(Config{RootDir: rootDir})
	if err := log1.Start(ctx1); err != nil {
		t.Fatal(err)
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	log2 := New(Config{RootDir: rootDir})
	if err := log2.Start(ctx2); err != ErrLocked {
		t.Fatalf("expected ErrLocked, got %v", err)
	}
}

func TestStartRejectsInvalidMetadata(t *testing.T) {
	rootDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(rootDir, metadataFileName), []byte(`{not-json}`), 0o600); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := New(Config{RootDir: rootDir})
	if err := log.Start(ctx); err == nil {
		t.Fatal("expected metadata decode error")
	}
}

func TestStartReleasesLockOnContextCancel(t *testing.T) {
	rootDir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	log := New(Config{RootDir: rootDir})
	if err := log.Start(ctx); err != nil {
		t.Fatal(err)
	}
	cancel()

	deadline := time.Now().Add(2 * time.Second)
	for {
		ctx2, cancel2 := context.WithCancel(context.Background())
		log2 := New(Config{RootDir: rootDir})
		err := log2.Start(ctx2)
		if err == nil {
			cancel2()
			return
		}
		cancel2()
		if err != ErrLocked {
			t.Fatalf("expected ErrLocked or success while waiting for release, got %v", err)
		}
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for lock release")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
