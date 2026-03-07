# 接入方式与后端 DSN

## 当前包结构

```text
pkg/
  drivers/
    fs/
      fs.go
      fs_test.go
      parse.go
      integration_test.go
  logstructured/
    fslog/
      compact.go
      errors.go
      fslog.go
      journal.go
      read.go
      snapshot.go
      types.go
      watch.go
      write.go
```

## 驱动装配

`pkg/drivers/fs/fs.go` 当前负责：

- 解析并校验 `fs://` 形式的后端 DSN，并从其中提取 `rootDir`
- 构造 `fslog.FSLog`
- 返回 `logstructured.New(fslog.New(...))`
- 通过 `drivers.Register("fs", New)` 注册自己

主程序通过 `pkg/endpoint/init.go` 的空导入识别 `fs` 驱动。

当前实现上的职责划分如下：

- `fslog.go`：启动、目录布局、锁、元数据加载、启动基线兼容逻辑
- `read.go`：`List`、`Count`、`After` 以及 Kine 历史 root list 兼容逻辑
- `write.go`：`Append`、写前状态检查、事件归一化为 `JournalRecord`
- `journal.go`：journal 编码、segment 管理、日志回放、metadata 落盘
- `snapshot.go`：snapshot 写入与恢复
- `compact.go`：compaction、基线保留、旧 journal 清理
- `watch.go`：`Watch` 与 `WaitForSyncTo`
- `errors.go`：后端内部错误定义
- `types.go`：配置、元数据、记录结构、内存状态定义

## 后端 DSN 形式

这里的 DSN 指的是 **Kine 进程连接底层存储后端所使用的地址**，不是 apiserver 或 `etcdctl` 直接访问的 etcd 服务地址。

第一版规则已经落地为：**只支持绝对路径形式的 DSN**。

也就是说：

- `scheme` 必须为 `fs`
- `rootDir` 明确定义为 DSN 的 `path` 部分
- `query` 部分只用于传递后端行为配置，不用于表达路径
- 第一版拒绝空路径和相对路径
- 如果 `rootDir` 对应目录不存在，则在启动阶段自动创建
- 第一版还要求 `host` 为空，也就是形如 `fs:///var/lib/kine`

推荐的 DSN 形式：

```text
fs:///var/lib/kine
fs:///tmp/kine-dev
fs:///var/lib/kine?sync=true&snapshot_interval=1000&segment_bytes=67108864&compact_min_retain=1
```

对应关系示例：

- `fs:///var/lib/kine` -> `rootDir = /var/lib/kine`
- `fs:///tmp/kine-dev?sync=false` -> `rootDir = /tmp/kine-dev`

第一版不支持如下相对路径形式：

```text
fs://./data/kine
```

## 当前支持的查询参数

当前实现支持下列 query 参数：

- `sync=true|false`
  - 每次 append 返回前是否执行 `fsync`
  - 默认值：`true`
- `snapshot_interval=<revisions>`
  - 每累计多少个 revision 生成一次 snapshot
  - 默认值：`1000`
- `segment_bytes=<bytes>`
  - journal segment 的滚动大小
  - 默认值：`64MiB`
- `compact_min_retain=<revisions>`
  - compaction 时至少保留的 revision 窗口
  - 默认值：沿用 Kine 传入的 `CompactMinRetain`

对于未知 query 参数，驱动会直接返回清晰错误，而不是忽略。
