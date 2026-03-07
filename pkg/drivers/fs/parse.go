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
	//
	// 一个容易误解的点是：标准库并不会把 `fs://./data` 解析成“path=./data”，
	// 而是会更接近于：
	//   - scheme = fs
	//   - host   = .
	//   - path   = /data
	// 也就是说，它在 URL 语义里更像“带 host 的地址”，而不是我们想要的
	// “path 就是本地数据目录”的 DSN。
	//
	// 所以下面先用 `u.Host != ""` 把这类写法挡掉，后面再继续检查 path
	// 是否真的是一个绝对路径。我们最终只接受 `fs:///var/lib/kine` 这种
	// host 为空、path 明确等于绝对路径的形式。
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
	//
	// 当前第一版只支持 3 个 query 参数：
	//   1. `sync`
	//      - 控制每次 append 返回前是否执行 fsync。
	//      - `true` 表示每次写入都尽量把数据刷到磁盘，崩溃后更稳，但写入更重。
	//      - `false` 表示允许更多数据先留在页缓存里，性能更轻，但掉电时最近写入更容易丢。
	//
	//   2. `snapshot_interval`
	//      - 控制累计多少个 revision 之后生成一次 snapshot。
	//      - 值越小，快照越频繁，重启恢复通常越快，但平时写 snapshot 的额外开销更高。
	//      - 值越大，平时更省，但积累的 journal 更多，恢复时要回放的日志也更多。
	//
	//   3. `segment_bytes`
	//      - 控制单个 journal segment 文件在多大时滚动到下一个新文件。
	//      - 值越小，文件切得越碎，单文件更容易观察和清理，但滚动会更频繁。
	//      - 值越大，滚动次数更少，但单个日志文件会更臃肿。
	//
	// 之所以只允许白名单，而不是“有参数就先收下”，是因为学习型项目里最怕出现
	// “拼错参数但系统悄悄忽略了”的情况。这里宁可报错，也不想让配置看起来生效、
	// 实际却在用默认值。
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
