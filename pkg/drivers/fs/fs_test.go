package fs

import (
	"testing"

	"github.com/k3s-io/kine/pkg/drivers"
)

// TestParseConfigDefaults 验证最简单的绝对路径 DSN 会落到默认配置。
func TestParseConfigDefaults(t *testing.T) {
	// 只提供 rootDir 时，驱动层应该补齐默认的写盘、快照和 segment 配置。
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

// TestParseConfigQueryOverrides 验证 query 参数能覆盖默认值。
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

// TestParseConfigRejectsEmptyEndpoint 验证没有 endpoint 时直接报错。
func TestParseConfigRejectsEmptyEndpoint(t *testing.T) {
	if _, err := ParseConfig(&drivers.Config{}); err == nil {
		t.Fatal("expected error for empty endpoint")
	}
}

// TestParseConfigRejectsRelativeLikePath 验证第一版不接受类似相对路径的写法。
func TestParseConfigRejectsRelativeLikePath(t *testing.T) {
	if _, err := ParseConfig(&drivers.Config{Endpoint: "fs://./data/kine"}); err == nil {
		t.Fatal("expected error for relative-like path")
	}
}

// TestParseConfigRejectsUnknownQueryParameter 验证未知 query 参数不会被静默忽略。
func TestParseConfigRejectsUnknownQueryParameter(t *testing.T) {
	if _, err := ParseConfig(&drivers.Config{Endpoint: "fs:///var/lib/kine?unknown=1"}); err == nil {
		t.Fatal("expected error for unknown query parameter")
	}
}

// TestParseConfigRejectsInvalidValues 验证各类非法参数值都会被拒绝。
func TestParseConfigRejectsInvalidValues(t *testing.T) {
	// 这里分别覆盖三类非法输入：
	// 1. `sync=maybe`：`sync` 只接受布尔值，`maybe` 不是合法布尔字面量。
	// 2. `snapshot_interval=0`：快照间隔必须大于 0，0 没有意义。
	// 3. `segment_bytes=-1`：segment 大小不能为负数。
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
