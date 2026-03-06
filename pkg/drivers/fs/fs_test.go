package fs

import (
	"testing"

	"github.com/k3s-io/kine/pkg/drivers"
)

func TestParseConfigDefaults(t *testing.T) {
	config, err := ParseConfig(&drivers.Config{Endpoint: "fs:///var/lib/kine"})
	if err != nil {
		t.Fatal(err)
	}
	if config.RootDir != "/var/lib/kine" {
		t.Fatalf("expected root dir /var/lib/kine, got %q", config.RootDir)
	}
	if !config.SyncEveryWrite {
		t.Fatal("expected syncEveryWrite to default to true")
	}
	if config.SnapshotEvery != defaultSnapshotEvery {
		t.Fatalf("expected default snapshotEvery %d, got %d", defaultSnapshotEvery, config.SnapshotEvery)
	}
	if config.SegmentBytes != defaultSegmentBytes {
		t.Fatalf("expected default segmentBytes %d, got %d", defaultSegmentBytes, config.SegmentBytes)
	}
}

func TestParseConfigQueryOverrides(t *testing.T) {
	config, err := ParseConfig(&drivers.Config{Endpoint: "fs:///tmp/kine-dev?sync=false&snapshot_interval=42&segment_bytes=8192"})
	if err != nil {
		t.Fatal(err)
	}
	if config.RootDir != "/tmp/kine-dev" {
		t.Fatalf("expected root dir /tmp/kine-dev, got %q", config.RootDir)
	}
	if config.SyncEveryWrite {
		t.Fatal("expected syncEveryWrite to be false")
	}
	if config.SnapshotEvery != 42 {
		t.Fatalf("expected snapshotEvery 42, got %d", config.SnapshotEvery)
	}
	if config.SegmentBytes != 8192 {
		t.Fatalf("expected segmentBytes 8192, got %d", config.SegmentBytes)
	}
}

func TestParseConfigRejectsEmptyEndpoint(t *testing.T) {
	if _, err := ParseConfig(&drivers.Config{}); err == nil {
		t.Fatal("expected error for empty endpoint")
	}
}

func TestParseConfigRejectsRelativeLikePath(t *testing.T) {
	if _, err := ParseConfig(&drivers.Config{Endpoint: "fs://./data/kine"}); err == nil {
		t.Fatal("expected error for relative-like path")
	}
}

func TestParseConfigRejectsUnknownQueryParameter(t *testing.T) {
	if _, err := ParseConfig(&drivers.Config{Endpoint: "fs:///var/lib/kine?unknown=1"}); err == nil {
		t.Fatal("expected error for unknown query parameter")
	}
}

func TestParseConfigRejectsInvalidValues(t *testing.T) {
	tests := []string{
		"fs:///var/lib/kine?sync=maybe",
		"fs:///var/lib/kine?snapshot_interval=0",
		"fs:///var/lib/kine?segment_bytes=-1",
	}

	for _, endpoint := range tests {
		if _, err := ParseConfig(&drivers.Config{Endpoint: endpoint}); err == nil {
			t.Fatalf("expected error for endpoint %q", endpoint)
		}
	}
}
