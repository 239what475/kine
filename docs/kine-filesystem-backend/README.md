# Kine 文件系统后端设计目录

本文档目录用于承载 Kine 文件系统后端的分阶段设计。当前这些文档描述的是**已经落地的第一版实现**，而不再只是最初的概念草案。

## 文档索引

- `01-overview.md`
  - 项目定位、接入层次、当前已实现能力、目标与非目标
- `02-integration-and-dsn.md`
  - 包结构、驱动装配、后端 DSN 规则、支持的 query 参数
- `03-storage-model.md`
  - 存储模型、磁盘布局、记录格式、内存索引、startup revision 兼容
- `04-runtime-paths.md`
  - 启动恢复、写路径、读路径、watch、`WaitForSyncTo`
- `05-maintenance.md`
  - snapshot、compaction、当前取舍与已知限制
- `06-testing-and-roadmap.md`
  - 测试布局、当前验证路径、本地 Python 依赖约定、后续收尾方向
- `07-test-run-fs-flow.md`
  - `scripts/test fs` 到 `test-run-fs` 的完整执行链路、关键脚本职责与实际测试内容

## 当前约束

- 第一版仅面向单机、单写者、学习用途
- 接入点放在 `logstructured.Log` 之下，而不是 `server.Dialect`
- 文件系统后端的 `rootDir` 来自 `fs:///absolute/path` 形式 DSN 的 `path` 部分
- 本地完整 fs 测试通过根目录 `AGENTS.md` 记录的命令执行

## 最小使用示例

本地执行完整 fs 测试链路时，当前约定命令为：

```bash
cd /home/what/myproject/etcd-mock/kine
make build package
PATH="/home/what/myproject/etcd-mock/.venv/bin:$PATH" ./scripts/test fs
```

## 后续维护建议

- 新增设计时，优先放入最贴近主题的子文档
- 如果某个主题继续膨胀，可在该主题下再拆更细的子文档
- 如需记录最终决策，可后续增加 `decisions/` 或 `adr/` 目录
