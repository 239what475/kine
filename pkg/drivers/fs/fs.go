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
	//
	// 这里的 revision 可以把它理解成“整个存储系统的全局版本号”或“全局时间线”。
	// 每次成功的创建、更新、删除，都会让它递增 1。它不是某个 key 自己的版本号，
	// 而是整个存储共享的一条提交序列。
	//
	// 例如：
	//   rev=1: 创建 /a = x
	//   rev=2: 更新 /a = y
	//   rev=3: 创建 /b = z
	// 那么这里的 revision 说的就是 1、2、3 这种全局递增编号。
	//
	// etcd 自己也有 revision，而且这是它 MVCC 模型里的核心概念。像 Kubernetes 里
	// 常见的 resourceVersion，本质上就是在消费这条“全局历史时间线”。我们这个
	// 文件后端现在做的事情，就是用一个学习版实现去模仿这套语义。
	//
	// 我们这个文件后端底层是 journal 追加日志模型：每次变更都会先追加成一条 journal
	// 记录。如果永远只依赖 journal，那么数据一多，下一次启动时就必须从第一条日志
	// 开始一直回放到最后一条，恢复成本会越来越高。
	//
	// 所谓快照（snapshot），不是调试信息，也不是简单的“打印当前值”，而是把
	// “这一时刻当前仍然有效的状态”整体保存成一个 snapshot 文件。这样重启时就可以：
	//   1. 先直接加载 snapshot
	//   2. 再只回放 snapshot 之后新增的少量 journal
	// 从而把恢复时间控制住。
	//
	// etcd 自己也有快照，但 etcd 里的快照比我们这里更完整：
	//   - 一方面，它同样承担“减少恢复成本”的作用；
	//   - 另一方面，它还和 Raft 日志裁剪、follower 追赶状态、集群恢复有关。
	//
	// 我们这里的 snapshot 更适合先把它理解成一个学习版模型：核心目标是让我们明白
	// “为什么不能每次都从第一条历史日志开始重放”，以及“为什么要定期把当前状态
	// 拍成一张可以直接加载的快照”。
	//
	// 因此，这个阈值的意思不是“每隔多少秒做一次快照”，而是“自上次快照以来，
	// 全局 revision 又增加了多少次”。我们按 revision 数量触发，而不是按时间触发，
	// 是因为真正决定恢复成本的是“积累了多少条变更”，而不是“过去了多久”。
	//
	// 这里默认取 1000，意思是：每累计 1000 次成功写入，就把当前状态重新拍一张照。
	// 这个值并不是 etcd 官方规定的特殊数字，只是我们这个学习型文件后端里一个折中
	// 默认值：既避免写一次就做一次快照太重，也避免长时间完全不做快照导致恢复过慢。
	defaultSnapshotEvery = int64(1000)
	// defaultSegmentBytes 表示单个 journal segment 的默认滚动阈值。
	//
	// 可以把 segment 理解成“把一整条无限增长的 journal 按大小切成多个文件”。
	// 如果我们永远只写一个 journal 文件，那么随着写入越来越多，这个文件会变得很大：
	//   - 单文件查看、排错会越来越痛苦
	//   - 启动恢复时，回放一个巨大的日志文件也会更慢
	//   - compact / snapshot 之后，清理旧日志时粒度也会很粗
	//
	// 所以更常见的做法不是维护一个永远追加的超大文件，而是把 journal 切成多个
	// segment，例如：
	//   00000000000000000001.log
	//   00000000000000001001.log
	//   00000000000000002001.log
	// 每个文件各自保存一段连续的 revision 区间。
	//
	// 所谓“滚动阈值”，就是当前正在写的这个 segment 文件，累计到多大以后，
	// 就关闭它并切换到下一个新文件继续写。
	//
	// 这里按“文件大小”而不是“revision 个数”来滚动，是因为单条 journal 记录的长度
	// 并不固定：有的 key/value 很短，有的很长，还可能带 PrevKV、lease 等额外字段。
	// 用字节大小做阈值，能更直接地控制单文件体积。
	//
	// 这和 etcd / Raft / WAL 里常见的“日志分段”思想是相通的：核心目标都是不要让
	// 单个日志文件无限膨胀，而是让日志天然按段组织，便于恢复、清理和排查。
	//
	// 这里默认取 64 MiB，含义不是“到这个大小一定有特殊语义”，而只是一个偏保守的
	// 默认值：既不会小到频繁切文件，也不会大到把单个 segment 养得过于臃肿。
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
	// 这里之所以能收到 ctx 和 wg，是因为 Kine 的驱动构造函数签名是统一的：
	// 所有后端都要实现同一个 `drivers.Constructor` 接口，所以参数列表必须一致。
	//
	// 但对 fs backend 来说，`New(...)` 这一层只是“把配置翻译成 backend 对象”，
	// 还没有真正开始运行后台逻辑；它此时不会去起 goroutine，也不会在这里做需要
	// 外部 context 托管的长生命周期工作。
	//
	// 真正的生命周期管理发生在后续 `Backend.Start(ctx)`：
	//   - 到那时才会创建目录、加锁、加载 metadata、回放 snapshot / journal；
	//   - 也是在那里才会绑定上下文，处理后续的关闭与资源释放。
	//
	// 所以这里的 `ctx` 和 `wg` 在 fs backend 的构造阶段暂时用不上，只是为了满足
	// 统一接口而保留下来，并用 `_ = ...` 明确告诉读代码的人：这不是忘了用，
	// 而是这个后端在这一层本来就不需要它们。
	//
	// 更具体地说，fs backend 和其他后端的区别在于：
	//   - NATS 那类后端可能在 `New(...)` 里就要启动 embedded server、建立连接，
	//     甚至起后台 goroutine，因此它们需要立刻用到 `ctx` 和 `wg`；
	//   - SQL 后端通常也会在构造阶段打开数据库连接、初始化 dialect/generic 层；
	//   - 但 fs backend 这里只是 new 出一个内存对象，本身既不连网络，也不启动额外
	//     服务，更不会在这一层提前起后台线程。
	//
	// 也正因为如此，fs backend 把“真正有副作用的动作”都延后到了 `Start(ctx)`：
	// 创建目录、文件锁、加载 metadata、回放 snapshot/journal，都是在那里发生的。
	//
	// 一个很自然的追问是：既然别的后端有时会在 `New(...)` 里做更多事情，
	// 那 fs backend 要不要也在这里提前创建目录，好让流程看起来更像？
	//
	// 答案是：技术上当然可以，例如这里就能先 `MkdirAll(rootDir)`；但我们当前刻意
	// 不这么做，因为那会把“构造对象”和“真正启动后端”的边界搅在一起。
	//
	// 更具体地说，如果把目录初始化提前到 `New(...)`：
	//   - `New(...)` 就开始对外部文件系统产生副作用；
	//   - 即使后面 `Start(ctx)` 根本没被调用，磁盘上也已经留下了目录；
	//   - 更关键的是，真正重要的启动步骤并不只有 mkdir，还包括加锁、读取 metadata、
	//     加载 snapshot、回放 journal、重建内存索引。若只把 mkdir 挪过来，初始化流程
	//     就会被拆成两半，反而更不好理解。
	//
	// 所以这里的设计选择是：
	//   - `New(...)` 只负责“把配置翻译成一个 backend 对象”；
	//   - `Start(ctx)` 才负责“真正接触文件系统并把后端跑起来”。
	//
	// 这样读代码时，心智模型会更清楚：看到 `New(...)`，知道它只是构造；
	// 看到 `Start(ctx)`，才知道从这里开始才会真的创建目录、持有锁、恢复状态。
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
