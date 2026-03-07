package fs

import (
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/logstructured/fslog"
	"github.com/k3s-io/kine/pkg/util"
)

const (
	// querySync 控制每次 append 返回前是否执行 fsync。
	querySync = "sync"
	// querySnapshotInterval 控制多少个 revision 之后生成一次 snapshot。
	querySnapshotInterval = "snapshot_interval"
	// querySegmentBytes 控制当前 journal segment 的滚动阈值。
	querySegmentBytes = "segment_bytes"
)

// ParseConfig 校验 `fs://` DSN，并把它转换成 `fslog.Config`。
//
// 第一版故意把配置面收得很小：
//   - 只接受绝对路径
//   - host 必须为空
//   - query 参数只允许白名单中的几个键
func ParseConfig(cfg *drivers.Config) (fslog.Config, error) {
	if cfg == nil {
		return fslog.Config{}, fmt.Errorf("fs backend requires driver config")
	}
	if cfg.Endpoint == "" {
		return fslog.Config{}, fmt.Errorf("fs backend requires explicit endpoint")
	}

	// 先按 URL 解析 DSN，这样可以尽早拒绝含糊不清的输入，例如
	// `fs://./data` 这种看起来像相对路径、但 URL 语义其实并不清晰的写法。
	u, err := util.ParseURL(cfg.Endpoint)
	if err != nil {
		return fslog.Config{}, err
	}
	if u.Scheme != "fs" {
		return fslog.Config{}, fmt.Errorf("fs backend requires scheme fs, got %q", u.Scheme)
	}
	if u.Host != "" {
		return fslog.Config{}, fmt.Errorf("fs backend requires absolute path DSN like fs:///var/lib/kine")
	}

	// DSN 的 path 部分就是后端真正使用的数据根目录。
	// 第一版强制要求它是绝对路径，这样 endpoint 和磁盘目录之间是一一对应的。
	rootDir := filepath.Clean(u.Path)
	if rootDir == "." || rootDir == "" || !filepath.IsAbs(rootDir) {
		return fslog.Config{}, fmt.Errorf("fs backend requires absolute path DSN like fs:///var/lib/kine")
	}

	// 对 query 参数做白名单校验，避免拼错参数时被静默忽略。
	for key := range u.Query() {
		switch key {
		case querySync, querySnapshotInterval, querySegmentBytes:
		default:
			return fslog.Config{}, fmt.Errorf("fs backend does not support query parameter %q", key)
		}
	}

	// 先从保守默认值出发，再叠加 query 参数中的显式覆盖值。
	result := fslog.Config{
		RootDir:          rootDir,
		SyncEveryWrite:   true,
		SnapshotEvery:    defaultSnapshotEvery,
		SegmentBytes:     defaultSegmentBytes,
		CompactMinRetain: cfg.CompactMinRetain,
	}

	if value := u.Query().Get(querySync); value != "" {
		syncEveryWrite, err := strconv.ParseBool(value)
		if err != nil {
			return fslog.Config{}, fmt.Errorf("invalid fs backend %q value %q: %w", querySync, value, err)
		}
		result.SyncEveryWrite = syncEveryWrite
	}

	if value := u.Query().Get(querySnapshotInterval); value != "" {
		snapshotEvery, err := strconv.ParseInt(value, 10, 64)
		if err != nil || snapshotEvery <= 0 {
			return fslog.Config{}, fmt.Errorf("invalid fs backend %q value %q", querySnapshotInterval, value)
		}
		result.SnapshotEvery = snapshotEvery
	}

	if value := u.Query().Get(querySegmentBytes); value != "" {
		segmentBytes, err := strconv.ParseInt(value, 10, 64)
		if err != nil || segmentBytes <= 0 {
			return fslog.Config{}, fmt.Errorf("invalid fs backend %q value %q", querySegmentBytes, value)
		}
		result.SegmentBytes = segmentBytes
	}

	return result, nil
}
