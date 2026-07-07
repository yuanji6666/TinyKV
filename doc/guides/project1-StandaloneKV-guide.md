# Project 1 详细指导：StandaloneKV

> 基础文档：[project1-StandaloneKV.md](../project1-StandaloneKV.md)

## 目标

完成 Project 1 后，你将拥有一个**单机版** KV 服务：通过 gRPC 暴露 RawGet / RawPut / RawDelete / RawScan，底层用 Badger 持久化，并支持 Column Family（列族）。

这是整个 TinyKV 课程的地基。后续 Raft、Multi-Raft、事务层都会复用这里的 `Storage` 接口、`engine_util` 工具和 Raw API 的处理模式。

## 前置知识

- Go 基础：interface、error 处理、defer
- 了解 LSM-Tree 基本概念（Badger 是 LSM 存储引擎）
- 阅读 [reading_list.md](../reading_list.md) 中「LSM-Tree」和「gRPC」部分（可选但推荐）

## 代码地图

| 路径 | 职责 | 你需要改吗 |
|------|------|-----------|
| `kv/main.go` | 启动 gRPC 服务 | 否 |
| `proto/proto/tinykvpb.proto` | 服务定义 | 一般否 |
| `proto/proto/kvrpcpb.proto` | 请求/响应结构 | 一般否 |
| `kv/storage/storage.go` | `Storage` / `StorageReader` 接口 | 否（读接口） |
| `kv/storage/modify.go` | `Modify` 写操作抽象 | 否（读用法） |
| `kv/storage/standalone_storage/standalone_storage.go` | 单机存储引擎 | **是** |
| `kv/server/raw_api.go` | Raw API handler | **是** |
| `kv/util/engine_util/` | CF 前缀、WriteBatch、迭代器 | 否（调用） |

## 实现路线

建议按以下顺序推进，每步都能单独验证：

```
Step 1: StandAloneStorage 生命周期 (New / Start / Stop)
   ↓
Step 2: Reader — 基于 badger.Txn 的快照读
   ↓
Step 3: Write — 批量写入 Modify
   ↓
Step 4: RawPut / RawDelete
   ↓
Step 5: RawGet
   ↓
Step 6: RawScan
   ↓
make project1
```

---

### Step 1：初始化 StandAloneStorage

**文件：** `kv/storage/standalone_storage/standalone_storage.go`

**要做的事：**

1. 在结构体中保存 `*engine_util.Engines` 或至少一个 Badger DB 实例（参考 `engine_util.Engines` 的构造方式）。
2. `NewStandAloneStorage(conf)`：根据 `config.Config` 打开 Badger，路径通常在 `conf.DBPath`。
3. `Start()`：若引擎已打开可返回 nil；若有懒加载逻辑在此完成。
4. `Stop()`：关闭 Badger，释放资源。

**注意：**

- 使用 `github.com/Connor1996/badger`（课程 fork 版），不要用官方 dgraph-io/badger。
- 参考项目中其他 storage 实现（如 `raft_storage`）看 Badger 打开选项，但 Project 1 只需单库。

---

### Step 2：实现 Reader

**接口：**

```go
type StorageReader interface {
    GetCF(cf string, key []byte) ([]byte, error)
    IterCF(cf string) engine_util.DBIterator
    Close()
}
```

**实现要点：**

1. `Reader(ctx)` 开启一个 **Badger 只读事务**（`db.NewTransaction(false)`），得到一致性快照。
2. 返回一个包装该 txn 的 reader 实现。
3. `GetCF`：通过 `engine_util.GetCF(txn, cf, key)` 读取；key 不存在时返回 `nil, nil`（不是 error）。
4. `IterCF`：通过 `engine_util.NewCFIterator(cf, txn)` 创建迭代器。
5. `Close`：对 txn 调用 `Discard()`，并关闭所有迭代器。

**常见错误：**

- 忘记 `Discard()` txn → 内存泄漏、测试卡住。
- 迭代器未关闭就 Discard txn → Badger 报错。

---

### Step 3：实现 Write

**接口：**

```go
Write(ctx *kvrpcpb.Context, batch []Modify) error
```

**实现要点：**

1. 开启 Badger 写事务。
2. 遍历 `batch`，根据 `Modify` 类型（Put / Delete）调用 `engine_util` 对应方法。
3. 提交事务；任一步失败则回滚。

`Modify` 结构在 `kv/storage/modify.go`，通常包含 CF、Key、Value、Type。

**本阶段可忽略 `kvrpcpb.Context`**，它在 Project 2 才携带 Region 信息。

---

### Step 4–6：Raw API Handlers

**文件：** `kv/server/raw_api.go`

| RPC | 逻辑概要 |
|-----|---------|
| `RawPut` | 构造 `Modify{Type: Put, CF, Key, Value}` → `storage.Write` |
| `RawDelete` | 构造 `Modify{Type: Delete, ...}` → `storage.Write` |
| `RawGet` | `storage.Reader` → `GetCF` → 填入 response |
| `RawScan` | `Reader.IterCF` 从 `StartKey` 起扫描至多 `Limit` 条 |

**RawScan 细节：**

- 使用 `req.StartKey` 作为下界；注意 CF 内 key 的字典序。
- `Limit` 为 0 时按 proto 语义处理（看测试期望，通常为不限制或返回空）。
- 迭代时注意 `Valid()`、`Next()`、`Key()`、`Value()` 的调用顺序。
- Scan 结束后 `Close()` reader。

**错误处理：**

- 存储层 error 应向上返回，gRPC 层包装为 RPC error。
- Get 不到 key 不是 error，而是 response 中 value 为空 / `NotFound` 字段（以测试为准）。

---

## Column Family 机制

Badger 本身不支持 CF。TinyKV 用 **key 前缀** 模拟：

```
实际存储 key = "${cf}_${userKey}"
```

所有读写必须走 `engine_util`，不要直接操作裸 key。详见 `kv/util/engine_util/doc.go`。

Project 4 会用到 `default` / `lock` / `write` 三个 CF，现在先养成通过 CF 读写的习惯。

---

## 测试与验收

```bash
# 通过 Project 1 全部测试
make project1

# 等价于
go test -v --count=1 ./kv/server -run 1
```

**验收标准：**

- [ ] `make project1` 全部通过
- [ ] Put 后 Get 能读到；Delete 后 Get 为空
- [ ] Scan 顺序正确、Limit 生效
- [ ] 不同 CF 下同名 key 互不影响

**调试建议：**

```bash
LOG_LEVEL=debug make project1
```

---

## 常见陷阱清单

1. **直接用 dgraph-io/badger** → 编译或行为不一致。
2. **Get 不到 key 返回 error** → 应返回 nil value。
3. **Write 未原子提交** → 部分 Modify 生效会导致测试随机失败。
4. **Scan 未限制在 CF 内** → 扫到其他 CF 的数据。
5. **忘记 Close reader** → 文件句柄泄漏。

---

## 与后续 Project 的关系

| 本 Project 产出 | 后续用途 |
|----------------|---------|
| `Storage` 接口实现模式 | `RaftStorage` 复用同一接口 |
| `engine_util` 使用经验 | peer storage、MVCC 编码都依赖它 |
| Raw API handler 模式 | Project 2B 起改为走 Raft  propose |
| CF 概念 | Project 4 MVCC 三 CF 的基础 |

---

## 推荐阅读顺序

1. [project1-StandaloneKV.md](../project1-StandaloneKV.md) — 官方任务说明
2. `kv/util/engine_util/doc.go` — CF 与引擎工具
3. Badger Txn 文档：https://godoc.org/github.com/Connor1996/badger#Txn
4. [reading_list.md](../reading_list.md) — LSM-Tree 章节
