package fslog

import (
	"context"
	"errors"
	"testing"

	logstructuredbackend "github.com/k3s-io/kine/pkg/logstructured"
	"github.com/k3s-io/kine/pkg/server"
)

func TestBackendCRUDSemantics(t *testing.T) {
	ctx := context.Background()
	backend := newBackendForWriteTests(t)

	baseRev, err := backend.CurrentRevision(ctx)
	if err != nil {
		t.Fatal(err)
	}

	rev, err := backend.Create(ctx, "/test/a", []byte("one"), 1)
	if err != nil {
		t.Fatal(err)
	}
	if rev != baseRev+1 {
		t.Fatalf("expected create rev %d, got %d", baseRev+1, rev)
	}

	_, err = backend.Create(ctx, "/test/a", []byte("again"), 0)
	if !errors.Is(err, server.ErrKeyExists) {
		t.Fatalf("expected duplicate create to return ErrKeyExists, got %v", err)
	}

	updateRev, kv, ok, err := backend.Update(ctx, "/test/a", []byte("two"), rev, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected update to succeed")
	}
	if updateRev != baseRev+2 {
		t.Fatalf("expected update rev %d, got %d", baseRev+2, updateRev)
	}
	if kv.CreateRevision != rev {
		t.Fatalf("expected create revision %d, got %d", rev, kv.CreateRevision)
	}
	if kv.ModRevision != updateRev {
		t.Fatalf("expected mod revision %d, got %d", updateRev, kv.ModRevision)
	}
	if string(kv.Value) != "two" {
		t.Fatalf("expected updated value two, got %q", kv.Value)
	}
	if kv.Lease != 0 {
		t.Fatalf("expected lease 0 after update, got %d", kv.Lease)
	}

	staleRev, staleKV, ok, err := backend.Update(ctx, "/test/a", []byte("stale"), rev, 1)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected stale update to fail")
	}
	if staleRev != updateRev {
		t.Fatalf("expected stale update to return current rev %d, got %d", updateRev, staleRev)
	}
	if staleKV == nil || staleKV.ModRevision != updateRev {
		t.Fatalf("expected stale update to return latest kv, got %+v", staleKV)
	}

	deleteRev, deletedKV, ok, err := backend.Delete(ctx, "/test/a", rev)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected delete with stale revision to fail")
	}
	if deleteRev != updateRev {
		t.Fatalf("expected stale delete rev %d, got %d", updateRev, deleteRev)
	}
	if deletedKV == nil || deletedKV.ModRevision != updateRev {
		t.Fatalf("expected stale delete to return current kv, got %+v", deletedKV)
	}

	deleteRev, deletedKV, ok, err = backend.Delete(ctx, "/test/a", updateRev)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected delete to succeed")
	}
	if deleteRev != baseRev+3 {
		t.Fatalf("expected delete rev %d, got %d", baseRev+3, deleteRev)
	}
	if deletedKV == nil || deletedKV.CreateRevision != rev || deletedKV.ModRevision != updateRev {
		t.Fatalf("unexpected deleted kv: %+v", deletedKV)
	}

	currentRev, kvOut, err := backend.Get(ctx, "/test/a", "", 1, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if currentRev != deleteRev {
		t.Fatalf("expected current rev %d after delete, got %d", deleteRev, currentRev)
	}
	if kvOut != nil {
		t.Fatalf("expected deleted key to be hidden from Get, got %+v", kvOut)
	}

	recreateRev, err := backend.Create(ctx, "/test/a", []byte("three"), 0)
	if err != nil {
		t.Fatal(err)
	}
	if recreateRev != baseRev+4 {
		t.Fatalf("expected recreate rev %d, got %d", baseRev+4, recreateRev)
	}

	_, recreatedKV, err := backend.Get(ctx, "/test/a", "", 1, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if recreatedKV == nil {
		t.Fatal("expected recreated key to exist")
	}
	if recreatedKV.CreateRevision != recreateRev {
		t.Fatalf("expected recreated create revision %d, got %d", recreateRev, recreatedKV.CreateRevision)
	}
}

func TestAppendRejectsInvalidWriteState(t *testing.T) {
	log := newStartedFSLogForReadTests(t)
	ctx := context.Background()

	if _, err := log.Append(ctx, &server.Event{KV: &server.KeyValue{Key: "/missing", Value: []byte("x")}}); !errors.Is(err, ErrWriteConflict) {
		t.Fatalf("expected update on missing key to fail with ErrWriteConflict, got %v", err)
	}
	if _, err := log.Append(ctx, &server.Event{Delete: true, KV: &server.KeyValue{Key: "/missing"}}); !errors.Is(err, ErrWriteConflict) {
		t.Fatalf("expected delete on missing key to fail with ErrWriteConflict, got %v", err)
	}

	createRev := mustAppendEvent(t, log, &server.Event{Create: true, KV: &server.KeyValue{Key: "/a", Value: []byte("one")}})
	if _, err := log.Append(ctx, &server.Event{Create: true, KV: &server.KeyValue{Key: "/a", Value: []byte("two")}}); !errors.Is(err, server.ErrKeyExists) {
		t.Fatalf("expected duplicate append create to fail with ErrKeyExists, got %v", err)
	}

	if _, err := log.Append(ctx, &server.Event{KV: &server.KeyValue{Key: "/a", Value: []byte("two"), CreateRevision: createRev}, PrevKV: &server.KeyValue{Key: "/a", ModRevision: 999}}); !errors.Is(err, ErrWriteConflict) {
		t.Fatalf("expected stale append update to fail with ErrWriteConflict, got %v", err)
	}
}

func newBackendForWriteTests(t *testing.T) server.Backend {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	backend := logstructuredbackend.New(New(Config{RootDir: t.TempDir(), SegmentBytes: 1 << 20, SyncEveryWrite: true}))
	if err := backend.Start(ctx); err != nil {
		t.Fatal(err)
	}
	return backend
}
