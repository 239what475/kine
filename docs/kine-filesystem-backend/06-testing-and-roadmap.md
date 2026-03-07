# 测试、验证路径与后续收尾

## 测试文件放置

Kine 当前并没有统一的 `tests/` 目录，测试主要与实现代码同目录放置。

filesystem backend 当前测试布局如下：

- `pkg/drivers/fs/fs_test.go`
  - DSN 解析与配置校验
- `pkg/drivers/fs/integration_test.go`
  - 无 Docker 的端到端集成验证
- `pkg/logstructured/fslog/fslog_test.go`
  - 启动、目录布局、锁、metadata、startup revision 兼容
- `pkg/logstructured/fslog/journal_test.go`
  - journal 追加、回放、segment 旋转、尾部损坏恢复
- `pkg/logstructured/fslog/read_test.go`
  - `List` / `Count` / `After` / root list 兼容
- `pkg/logstructured/fslog/write_test.go`
  - create / update / delete 写语义
- `pkg/logstructured/fslog/watch_test.go`
  - watch 与 `WaitForSyncTo`
- `pkg/logstructured/fslog/snapshot_test.go`
  - snapshot 写入与恢复
- `pkg/logstructured/fslog/compact_test.go`
  - compaction 与 snapshot 联动

## 当前可用验证路径

### 1. 包级 Go 测试

可以直接运行：

```bash
cd /home/what/myproject/etcd-mock/kine
go test ./pkg/logstructured/fslog ./pkg/drivers/fs
```

### 2. 完整本地 fs 测试链路

当前约定的本地完整测试命令已经记录在仓库根目录 `AGENTS.md`：

```bash
cd /home/what/myproject/etcd-mock/kine
make build package
PATH="/home/what/myproject/etcd-mock/.venv/bin:$PATH" ./scripts/test fs
```

这条路径已经在本地验证通过，覆盖：

- `apiserver` 测试二进制
- 本地构建的 Kine 镜像
- K3s 测试集群
- `scripts/test-load`
- `scripts/test-conformance`（当前输出为 `Skipping conformance`）

## 本地 Python 依赖约定

当前不修改 `kine/scripts/test-load` 本身。

本地运行完整测试时，通过根目录 `.venv` 提供 Python 依赖，当前依赖为：

- `kubernetes`
- `termplotlib==0.3.4`

## 剩余收尾方向

当前第一版实现已经完成到集成验证阶段。剩余工作主要是：

- 清理和补齐设计文档
- 继续整理已知限制
- 视需要再决定是否处理 `pkg/testserver` 异步日志 race
- 视需要再决定是否为 `test-load` 增加更正式的本地环境包装
