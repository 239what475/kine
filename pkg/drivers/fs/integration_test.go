package fs_test

import (
	"context"
	"errors"
	"net/url"
	"path/filepath"
	"testing"
	"time"

	"github.com/k3s-io/kine/pkg/testserver"
	mvccpb "go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestFilesystemBackendIntegration(t *testing.T) {
	rootDir := filepath.Join(t.TempDir(), "fs-data")
	t.Setenv("KINE_ENDPOINT", (&url.URL{Scheme: "fs", Path: rootDir}).String())
	t.Setenv("KINE_COMPACT_INTERVAL", "0")
	t.Setenv("KINE_COMPACT_MIN_RETAIN", "0")

	cfg := testserver.NewTestConfig(t)
	cfg.ListenClientUrls = []url.URL{{Scheme: "unix", Path: filepath.Join(t.TempDir(), "kine.sock")}}

	client := testserver.RunEtcd(t, cfg)
	t.Cleanup(func() {
		_ = client.Close()
	})

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	createResp, err := client.Put(ctx, "/stage10/foo", "one")
	if err != nil {
		t.Fatalf("put initial key: %v", err)
	}

	getResp, err := client.Client.Get(ctx, "/stage10/foo")
	if err != nil {
		t.Fatalf("get initial key: %v", err)
	}
	if len(getResp.Kvs) != 1 {
		t.Fatalf("expected 1 key from get, got %d", len(getResp.Kvs))
	}
	if value := string(getResp.Kvs[0].Value); value != "one" {
		t.Fatalf("expected initial value %q, got %q", "one", value)
	}
	if getResp.Kvs[0].ModRevision != createResp.Header.Revision {
		t.Fatalf("expected mod revision %d, got %d", createResp.Header.Revision, getResp.Kvs[0].ModRevision)
	}

	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	watchCh := client.Watch(watchCtx, "/stage10/", clientv3.WithPrefix(), clientv3.WithRev(createResp.Header.Revision+1))

	updateResp, err := client.Put(ctx, "/stage10/foo", "two")
	if err != nil {
		t.Fatalf("update watched key: %v", err)
	}

	watchResp := nextWatchResponse(t, watchCh)
	if len(watchResp.Events) != 1 {
		t.Fatalf("expected 1 watch event, got %d", len(watchResp.Events))
	}
	watchEvent := watchResp.Events[0]
	if watchEvent.Type != mvccpb.PUT {
		t.Fatalf("expected PUT watch event, got %s", watchEvent.Type)
	}
	if key := string(watchEvent.Kv.Key); key != "/stage10/foo" {
		t.Fatalf("expected watch key %q, got %q", "/stage10/foo", key)
	}
	if value := string(watchEvent.Kv.Value); value != "two" {
		t.Fatalf("expected watched value %q, got %q", "two", value)
	}
	if watchEvent.Kv.ModRevision != updateResp.Header.Revision {
		t.Fatalf("expected watched revision %d, got %d", updateResp.Header.Revision, watchEvent.Kv.ModRevision)
	}

	if _, err := client.Put(ctx, "/stage10/bar", "three"); err != nil {
		t.Fatalf("put second prefixed key: %v", err)
	}

	listResp, err := client.Client.Get(ctx, "/stage10/", clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("list prefixed keys: %v", err)
	}
	if len(listResp.Kvs) != 2 {
		t.Fatalf("expected 2 prefixed keys, got %d", len(listResp.Kvs))
	}

	if _, err := client.Compact(ctx, updateResp.Header.Revision); err != nil {
		t.Fatalf("compact backend at revision %d: %v", updateResp.Header.Revision, err)
	}

	if _, err := client.Client.Get(ctx, "/stage10/foo", clientv3.WithRev(createResp.Header.Revision)); !errors.Is(err, rpctypes.ErrCompacted) {
		t.Fatalf("expected compacted error for historical get, got %v", err)
	}
}

func nextWatchResponse(t *testing.T, watchCh clientv3.WatchChan) clientv3.WatchResponse {
	t.Helper()

	select {
	case response, ok := <-watchCh:
		if !ok {
			t.Fatal("watch channel closed before delivering an event")
		}
		if err := response.Err(); err != nil {
			t.Fatalf("watch response returned error: %v", err)
		}
		if response.Canceled {
			t.Fatalf("watch response was canceled: %v", response.Err())
		}
		return response
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for watch response")
		return clientv3.WatchResponse{}
	}
}

func TestFilesystemBackendTTLIntegration(t *testing.T) {
	rootDir := filepath.Join(t.TempDir(), "fs-data")
	t.Setenv("KINE_ENDPOINT", (&url.URL{Scheme: "fs", Path: rootDir}).String())
	t.Setenv("KINE_COMPACT_INTERVAL", "0")
	t.Setenv("KINE_COMPACT_MIN_RETAIN", "0")

	cfg := testserver.NewTestConfig(t)
	cfg.ListenClientUrls = []url.URL{{Scheme: "unix", Path: filepath.Join(t.TempDir(), "kine.sock")}}

	client := testserver.RunEtcd(t, cfg)
	t.Cleanup(func() {
		_ = client.Close()
	})

	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()

	leaseResp, err := client.Lease.Grant(ctx, 1)
	if err != nil {
		t.Fatalf("grant lease: %v", err)
	}

	putResp, err := client.Client.Put(ctx, "/stage10/ttl", "value", clientv3.WithLease(leaseResp.ID))
	if err != nil {
		t.Fatalf("put key with lease: %v", err)
	}

	watchCtx, watchCancel := context.WithCancel(ctx)
	defer watchCancel()
	watchCh := client.Client.Watch(watchCtx, "/stage10/ttl", clientv3.WithRev(putResp.Header.Revision+1))

	watchResp := nextWatchResponse(t, watchCh)
	if len(watchResp.Events) != 1 {
		t.Fatalf("expected 1 ttl watch event, got %d", len(watchResp.Events))
	}
	watchEvent := watchResp.Events[0]
	if watchEvent.Type != mvccpb.DELETE {
		t.Fatalf("expected DELETE ttl watch event, got %s", watchEvent.Type)
	}
	if key := string(watchEvent.Kv.Key); key != "/stage10/ttl" {
		t.Fatalf("expected ttl watch key %q, got %q", "/stage10/ttl", key)
	}

	getResp, err := client.Client.Get(ctx, "/stage10/ttl")
	if err != nil {
		t.Fatalf("get ttl key after delete: %v", err)
	}
	if len(getResp.Kvs) != 0 {
		t.Fatalf("expected ttl key to be deleted, got %d remaining kvs", len(getResp.Kvs))
	}
}

func TestFilesystemBackendExactSlashKeyIntegration(t *testing.T) {
	rootDir := filepath.Join(t.TempDir(), "fs-data")
	t.Setenv("KINE_ENDPOINT", (&url.URL{Scheme: "fs", Path: rootDir}).String())
	t.Setenv("KINE_COMPACT_INTERVAL", "0")
	t.Setenv("KINE_COMPACT_MIN_RETAIN", "0")

	cfg := testserver.NewTestConfig(t)
	cfg.ListenClientUrls = []url.URL{{Scheme: "unix", Path: filepath.Join(t.TempDir(), "kine.sock")}}

	client := testserver.RunEtcd(t, cfg)
	t.Cleanup(func() {
		_ = client.Close()
	})

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	if _, err := client.Client.Put(ctx, "/pods/test-ns/foo", "foo"); err != nil {
		t.Fatalf("put child key: %v", err)
	}
	if _, err := client.Client.Put(ctx, "/pods/test-ns/foobar", "foobar"); err != nil {
		t.Fatalf("put sibling key: %v", err)
	}

	getResp, err := client.Client.Get(ctx, "/pods/")
	if err != nil {
		t.Fatalf("exact get on /pods/: %v", err)
	}
	if len(getResp.Kvs) != 0 {
		t.Fatalf("expected exact key /pods/ to return 0 kvs, got %d (first key %q)", len(getResp.Kvs), string(getResp.Kvs[0].Key))
	}
}

func TestFilesystemBackendHistoricalLimitedPrefixCount(t *testing.T) {
	rootDir := filepath.Join(t.TempDir(), "fs-data")
	t.Setenv("KINE_ENDPOINT", (&url.URL{Scheme: "fs", Path: rootDir}).String())
	t.Setenv("KINE_COMPACT_INTERVAL", "0")
	t.Setenv("KINE_COMPACT_MIN_RETAIN", "0")

	cfg := testserver.NewTestConfig(t)
	cfg.ListenClientUrls = []url.URL{{Scheme: "unix", Path: filepath.Join(t.TempDir(), "kine.sock")}}

	client := testserver.RunEtcd(t, cfg)
	t.Cleanup(func() {
		_ = client.Close()
	})

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()

	if _, err := client.Client.Put(ctx, "/pods/a", "a"); err != nil {
		t.Fatalf("put first key: %v", err)
	}
	if _, err := client.Client.Put(ctx, "/pods/b", "b"); err != nil {
		t.Fatalf("put second key: %v", err)
	}
	thirdResp, err := client.Client.Put(ctx, "/pods/c", "c")
	if err != nil {
		t.Fatalf("put third key: %v", err)
	}

	historicalRev := thirdResp.Header.Revision

	if _, err := client.Client.Put(ctx, "/pods/d", "d"); err != nil {
		t.Fatalf("put fourth key: %v", err)
	}
	if _, err := client.Client.Put(ctx, "/pods/e", "e"); err != nil {
		t.Fatalf("put fifth key: %v", err)
	}

	resp, err := client.Client.Get(ctx, "/pods/", clientv3.WithPrefix(), clientv3.WithRev(historicalRev), clientv3.WithLimit(2))
	if err != nil {
		t.Fatalf("historical limited prefix get: %v", err)
	}
	if !resp.More {
		t.Fatal("expected historical limited list to indicate more results")
	}
	if resp.Count != 3 {
		t.Fatalf("expected historical limited list count 3, got %d", resp.Count)
	}
	if len(resp.Kvs) != 2 {
		t.Fatalf("expected 2 kvs from limited list, got %d", len(resp.Kvs))
	}
	if key := string(resp.Kvs[0].Key); key != "/pods/a" {
		t.Fatalf("expected first listed key %q, got %q", "/pods/a", key)
	}
	if key := string(resp.Kvs[1].Key); key != "/pods/b" {
		t.Fatalf("expected second listed key %q, got %q", "/pods/b", key)
	}
}
