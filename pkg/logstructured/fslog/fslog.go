package fslog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"

	"github.com/k3s-io/kine/pkg/server"
	"github.com/tidwall/btree"
)

func New(config Config) *FSLog {
	log := &FSLog{
		rootDir:        config.RootDir,
		byKey:          btree.NewMap[string, []*revOp](0),
		byRev:          map[int64]*revOp{},
		stream:         make(chan server.Events, 128),
		syncEveryWrite: config.SyncEveryWrite,
		snapshotEvery:  config.SnapshotEvery,
		segmentBytes:   config.SegmentBytes,
		cond:           sync.NewCond(&sync.Mutex{}),
	}
	log.initPaths()
	return log
}

func (f *FSLog) Start(ctx context.Context) error {
	if f.rootDir == "" {
		return fmt.Errorf("filesystem backend requires root directory")
	}
	if err := f.ensureLayout(); err != nil {
		return err
	}
	if err := f.acquireLock(); err != nil {
		return err
	}
	if err := f.loadMetadata(); err != nil {
		f.releaseResources()
		return err
	}
	if err := f.scanState(); err != nil {
		f.releaseResources()
		return err
	}
	if err := f.loadLatestSnapshot(); err != nil {
		f.releaseResources()
		return err
	}
	if err := f.replayJournal(); err != nil {
		f.releaseResources()
		return err
	}

	currentRev := maxInt64(maxInt64(f.metadata.CurrentRevision, f.replayedRevision), f.loadedSnapshotRev)
	f.currentRev.Store(currentRev)
	f.compactRev.Store(f.metadata.CompactRevision)
	f.appliedRev.Store(currentRev)
	f.metadata.CurrentRevision = currentRev

	go func() {
		<-ctx.Done()
		f.releaseResources()
	}()

	return nil
}

func (f *FSLog) ensureLayout() error {
	for _, dir := range []string{f.rootDir, f.journalDir, f.snapshotDir} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("create fs backend directory %q: %w", dir, err)
		}
	}
	return nil
}

func (f *FSLog) acquireLock() error {
	file, err := os.OpenFile(f.lockPath, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open fs backend lock file: %w", err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return ErrLocked
		}
		return fmt.Errorf("lock fs backend directory: %w", err)
	}
	f.lockFile = file
	return nil
}

func (f *FSLog) releaseResources() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closeSegmentLocked()
	if f.lockFile == nil {
		return
	}
	_ = syscall.Flock(int(f.lockFile.Fd()), syscall.LOCK_UN)
	_ = f.lockFile.Close()
	f.lockFile = nil
}

func (f *FSLog) loadMetadata() error {
	data, err := os.ReadFile(f.metadataPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			f.metadata = metadata{}
			return nil
		}
		return fmt.Errorf("read fs backend metadata: %w", err)
	}
	var meta metadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return fmt.Errorf("decode fs backend metadata: %w", err)
	}
	f.metadata = meta
	return nil
}

func (f *FSLog) scanState() error {
	snapshots, err := collectFileNames(f.snapshotDir, snapshotFileSuffix)
	if err != nil {
		return fmt.Errorf("scan snapshot directory: %w", err)
	}
	journals, err := collectFileNames(f.journalDir, journalFileSuffix)
	if err != nil {
		return fmt.Errorf("scan journal directory: %w", err)
	}
	f.snapshotFiles = snapshots
	f.journalFiles = journals
	if f.metadata.ActiveSegment == "" && len(journals) > 0 {
		f.metadata.ActiveSegment = filepath.Base(journals[len(journals)-1])
	}
	return nil
}

func collectFileNames(dir string, suffix string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	files := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, tempFileSuffix) {
			continue
		}
		if suffix != "" && !strings.HasSuffix(name, suffix) {
			continue
		}
		files = append(files, filepath.Join(dir, name))
	}
	sort.Strings(files)
	return files, nil
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (f *FSLog) CompactRevision(context.Context) (int64, error) {
	return f.compactRev.Load(), nil
}

func (f *FSLog) CurrentRevision(context.Context) (int64, error) {
	return f.currentRev.Load(), nil
}

func (f *FSLog) DbSize(context.Context) (int64, error) {
	return 0, nil
}

func (f *FSLog) Compact(context.Context, int64) (int64, error) {
	return 0, ErrNotImplemented
}
