package fslog

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestStartCreatesDirectoryLayout 验证第一次启动会创建目录布局并持有锁文件。
func TestStartCreatesDirectoryLayout(t *testing.T) {
	rootDir := filepath.Join(t.TempDir(), "store")
	log := New(Config{RootDir: rootDir})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := log.Start(ctx); err != nil {
		t.Fatal(err)
	}

	for _, path := range []string{rootDir, log.files.journalDir, log.files.snapshotDir, log.files.lockPath} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("expected %s to exist: %v", path, err)
		}
	}
	if log.files.lockFile == nil {
		t.Fatal("expected lock file to be held")
	}
}

// TestStartLoadsExistingMetadataAndScansDirectories 验证重启时会从 metadata 和目录扫描恢复内存状态。
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
	if len(log.files.journalFiles) != 1 {
		t.Fatalf("expected 1 journal file, got %d", len(log.files.journalFiles))
	}
	if len(log.files.snapshotFiles) != 1 {
		t.Fatalf("expected 1 snapshot file, got %d", len(log.files.snapshotFiles))
	}
}

// TestStartRejectsLockedDirectory 验证同一个数据目录只能被一个进程持有。
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

// TestStartRejectsInvalidMetadata 验证损坏的 metadata 不会被静默接受。
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

// TestStartReleasesLockOnContextCancel 验证上下文结束后锁会被释放，后续实例可以重新启动。
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

// TestStartBootstrapsCompactRevisionKeyOnFreshStore 验证 fresh store 启动时会补一条 compact_rev_key 基线记录。
func TestStartBootstrapsCompactRevisionKeyOnFreshStore(t *testing.T) {
	rootDir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := New(Config{RootDir: rootDir})
	if err := log.Start(ctx); err != nil {
		t.Fatal(err)
	}

	if got := log.currentRev.Load(); got != 1 {
		t.Fatalf("expected fresh fslog start to bootstrap revision 1, got %d", got)
	}
	if got := log.appliedRev.Load(); got != 1 {
		t.Fatalf("expected applied revision 1 after bootstrap, got %d", got)
	}
	kv := log.getRevisionOpLocked("compact_rev_key", 1, true)
	if kv == nil || !kv.create {
		t.Fatalf("expected compact_rev_key bootstrap record, got %+v", kv)
	}
}
