package fs

import (
	"context"
	"sync"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/logstructured"
	"github.com/k3s-io/kine/pkg/logstructured/fslog"
	"github.com/k3s-io/kine/pkg/server"
)

const (
	// defaultSnapshotEvery 表示默认每累计多少个 revision 触发一次快照。
	defaultSnapshotEvery = int64(1000)
	// defaultSegmentBytes 表示单个 journal segment 的默认滚动阈值。
	defaultSegmentBytes = int64(64 << 20)
)

// New 是注册到 `fs://` scheme 下的驱动构造入口。
//
// 这一层故意保持很薄，只负责三件事：
//  1. 解析 Kine 驱动层的配置 / DSN
//  2. 构造真正的文件后端 `fslog.FSLog`
//  3. 再用 `logstructured.New(...)` 包成 Kine 需要的后端对象
//
// 也就是说，这个包负责“接入”，真正的存储语义都在
// `pkg/logstructured/fslog` 中实现。
func New(ctx context.Context, wg *sync.WaitGroup, cfg *drivers.Config) (bool, server.Backend, error) {
	// fslog 在对象构造阶段本身不需要外层的 context / waitgroup；
	// 真正的生命周期管理发生在后续 Backend.Start(...) 中。
	_ = ctx
	_ = wg

	// 先把 driver 层的 DSN 配置翻译成 fslog 自己的配置结构。
	backendConfig, err := ParseConfig(cfg)
	if err != nil {
		return false, nil, err
	}

	// fslog 只实现“追加式日志后端”；
	// logstructured.New(...) 会在它之上补齐 Kine 需要的 MVCC 语义层。
	backend := logstructured.New(fslog.New(backendConfig))

	// 文件后端不参与 leader election，因此第一个返回值恒为 false。
	return false, backend, nil
}

func init() {
	// 把 `fs://...` 注册到统一驱动注册表里，这样 drivers.New(...) 就能
	// 像处理其他后端一样，通过 scheme 找到这个构造函数。
	drivers.Register("fs", New)
}
