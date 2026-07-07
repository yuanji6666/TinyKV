# Project 4 详细指导：Transaction

> 基础文档：[project4-Transaction.md](../project4-Transaction.md)

## 目标

在分布式 KV 之上实现 **Percolator 风格的两阶段提交（2PC）** 和 **MVCC**，提供快照隔离（Snapshot Isolation）级别的事务语义。

客户端为 TinySQL；服务端 TinyKV 与客户端协同才能保证 ACID。Raw API 与事务 API **不可混用**。

## 事务协议概览

```
TinySQL                          TinyScheduler              TinyKV
   |  1. 获取 start_ts  ──────────────────────────────►  TSO
   |  2. KvGet/KvScan (带 start_ts)
   |─────────────────────────────────────────────────────►
   |  3. 本地构建写集，选 primary key
   |  4. KvPrewrite (锁 + 写 default CF)  ─────────────►  各 Region Leader
   |  5. 获取 commit_ts  ──────────────────────────────►  TSO
   |  6. KvCommit (primary)  ───────────────────────────►  primary Region
   |  7. KvCommit (secondaries)  ───────────────────────►  其他 Region
```

**失败路径：**

- Prewrite 失败 → `KvBatchRollback` 解锁
- 遇到他人锁 → `KvCheckTxnStatus` → `KvResolveLock`
- Primary commit 失败 → 整体 rollback

## 整体结构

| 部分 | 内容 | 测试 |
|------|------|------|
| **4A** | MVCC 层：`MvccTxn` | `make project4a` |
| **4B** | KvGet / KvPrewrite / KvCommit + Latches | `make project4b` |
| **4C** | KvScan / KvCheckTxnStatus / KvBatchRollback / KvResolveLock | `make project4c` |

---

## Part A：MVCC 编码与 MvccTxn

### 三个 Column Family

| CF | Key 格式 | Value |
|----|---------|-------|
| `default` | `encode(userKey, start_ts)` | 用户 value |
| `lock` | `userKey` | 序列化 `Lock` |
| `write` | `encode(userKey, commit_ts)` | 序列化 `Write` |

**编码规则：** 先按 userKey 升序，再按 timestamp **降序**（这样迭代时最新版本在前）。

相关代码：

- `kv/transaction/mvcc/transaction.go` — 编码函数 + `MvccTxn`
- `kv/transaction/mvcc/lock.go` — `Lock` 结构
- `kv/transaction/mvcc/write.go` — `Write` 结构

### MvccTxn 职责

- 持有 `StartTS` 和 `StorageReader`
- 提供逻辑读写 API（GetValue、PutLock、PutWrite 等）
- 修改收集在 `writes []Modify`，最后一次性落盘（原子性）

**注意：** `MvccTxn` 是**单条命令**的 MVCC 包装，不是 TinySQL 会话级事务。

### 要实现的方法

| 方法 | 要点 |
|------|------|
| `GetLock` | 读 lock CF |
| `PutLock` / `DeleteLock` | 写/删锁 |
| `GetValue` | 在 start_ts 时点找最新已提交值；需扫描 write CF 判断可见性 |
| `PutValue` / `DeleteValue` | 写 default CF（带 start_ts） |
| `PutWrite` | 写 write CF（带 commit_ts） |
| `CurrentWrite` / `MostRecentWrite` | 按 start_ts 查找 write 记录 |

**GetValue 难点：**

- 迭代 encoded key，找 ≤ start_ts 的最新已提交版本
- 判断有效性看 **commit_ts**，不是 start_ts
- 可能需结合 write 记录中的 `WriteKind`（Put / Delete / Rollback）

**测试：**

```bash
make project4a
# go test ./kv/transaction/... -run 4A
```

建议先读 `transaction_test.go` 理解期望行为。

---

## Part B：核心事务 API

### 代码地图

| 文件 | 改动 |
|------|------|
| `kv/server/server.go` | KvGet / KvPrewrite / KvCommit |
| `kv/transaction/latches/latches.go` |  per-key 互斥 |

### KvGet

1. 创建 `MvccTxn(reader, req.Version)` — Version 即 start_ts
2. 检查 key 是否被**其他事务**锁住 → 返回 `KeyError`（锁信息）
3. `GetValue` → 填入 response
4. 处理 Region 错误（同 Raw API）

### KvPrewrite

对每个 key 独立处理：

1. 检查是否已有锁或其他事务的 write
2. 冲突 → 返回锁错误 / write conflict
3. 成功 → `PutLock`（含 primary、ttl）+ `PutValue`（default CF）
4. 收集 `txn.Writes()` 一次性 `storage.Write`

**锁结构：** 记录 primary key、start_ts、ttl 等（见 `lock.go`）。

### KvCommit

1. 验证 key 被**本事务**锁住（start_ts 匹配）
2. `DeleteLock` + `PutWrite`（记录 commit_ts）
3. 不改变 default CF 中的 value（Prewrite 时已写入）

**失败条件：** 未锁定 / 被其他事务锁定。

### Latches（并发控制）

多客户端并发时，同一 key 可能同时 commit 和 rollback。

`kv/transaction/latches/latches.go` 提供 per-key 锁（覆盖所有 CF）：

```
acquire(keys) → 处理 → release(keys)
```

在 handler 入口 acquire 请求涉及的所有 key。

---

## Part C：扫描与冲突解决

### KvScan

比 RawScan 复杂得多：

- 不能直接在底层迭代「逻辑 key」
- 需跳过多版本、过滤不可见版本、处理锁

**建议：** 实现 `kv/transaction/mvcc/scanner.go` 中的 scanner 抽象，迭代**逻辑** key-value。

行为：

- 在 start_ts 时点的一致性快照读
- 某个 key 遇到锁错误 → 记录到 response 的 `Errors` 字段，**继续扫描其他 key**
- 其他致命错误 → 终止整个 scan

### KvCheckTxnStatus

客户端传入 primary key、start_ts、**当前物理时间**。

1. 查找锁
2. 锁不存在 → 返回已提交/已回滚状态
3. 锁存在 → 用 **PhysicalTime** 比较 TTL（只用时间戳的物理部分）
4. 超时 → 回滚锁，返回状态

### KvBatchRollback

对每个 key：

1. 验证锁属于本事务（start_ts 匹配）
2. `DeleteLock`
3. 删除 default CF 中 prewrite 的 value
4. `PutWrite` 写 Rollback 记录（防止后续读看到脏数据）

### KvResolveLock

批量处理锁列表：

- `commit_ts == 0` → rollback
- 否则 → commit（可复用 KvCommit / KvBatchRollback 逻辑）

---

## 时间戳

时间戳 = **物理时间 + 逻辑计数**（TSO 由 Scheduler 分配）。

- 比较相等性：用完整 timestamp
- 计算 TTL 超时：只用 `PhysicalTime(ts)`

见 `scheduler/pkg/tsoutil` 和 `transaction.go` 中的 `PhysicalTime`。

---

## 测试与验收

```bash
make project4       # 全部
make project4a      # MVCC 单元测试
make project4b      # Get / Prewrite / Commit
make project4c      # Scan / 冲突解决
```

**验收标准：**

- [ ] MVCC 编码顺序正确（迭代测试）
- [ ] 并发 prewrite 冲突检测正确
- [ ] Commit 后其他事务能读到新值
- [ ] Rollback 后读不到 prewrite 的值
- [ ] Scan 部分 key 锁错误不影响其他 key
- [ ] Region 错误正确传播

---

## 常见陷阱

1. **GetValue 用 start_ts 判断可见性** → 应该用 commit_ts
2. **编码 key 字节序错误** → Scan/Get 结果错乱
3. **未使用 Latches** → 并发测试随机失败
4. **Commit 修改了 default CF** → 违反 Percolator 协议
5. **TTL 比较用了完整 timestamp** → 超时判断错误
6. **Scan 遇锁错误就整体失败** → 与协议不符
7. **Rollback 未写 Rollback write 记录** → 幻读 / 脏读

---

## 端到端验证

完成四个 Project 后，可按 README 启动完整栈：

```bash
./tinyscheduler-server
./tinykv-server -path=data
./tinysql-server --store=tikv --path="127.0.0.1:2379"
mysql -u root -h 127.0.0.1 -P 4000
```

在 MySQL 客户端执行事务 SQL，验证跨 Region 事务。

---

## 推荐阅读

1. [Percolator 论文](https://storage.googleapis.com/pub-tools-public-publication-data/pdf/36726.pdf)
2. [TiKV Percolator 实现](https://tikv.org/docs/deep-dive/distributed-transaction/percolator/)
3. [Snapshot Isolation](https://en.wikipedia.org/wiki/Snapshot_isolation)
4. [reading_list.md](../reading_list.md) — Distributed Transactions 章节
