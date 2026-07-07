# Project 2 详细指导：RaftKV

> 基础文档：[project2-RaftKV.md](../project2-RaftKV.md)

## 目标

在 Project 1 单机 KV 之上，实现 **Raft 共识算法** 和 **基于 Raft 的容错 KV 服务**，并支持 **日志压缩（GC）与 Snapshot**。

完成后，集群在少数节点故障或网络分区时，只要多数派存活，读写仍可继续。

## 整体结构

Project 2 分三大块，建议严格按顺序：

| 部分 | 内容 | 测试命令 |
|------|------|---------|
| **2A** | Raft 算法（选举、复制、RawNode） | `make project2a` |
| **2B** | raftstore + PeerStorage + 容错 KV | `make project2b` |
| **2C** | 日志 GC + Snapshot | `make project2c` |

2A 可再拆：

| 子阶段 | 内容 | 测试 |
|--------|------|------|
| 2AA | Leader 选举 | `make project2aa` |
| 2AB | 日志复制 | `make project2ab` |
| 2AC | RawNode / Ready | `make project2ac` |

---

## Part A：Raft 算法

### 代码地图

| 文件 | 职责 |
|------|------|
| `raft/raft.go` | 核心状态机：tick、Step、选举、复制 |
| `raft/log.go` | `RaftLog`：日志索引、term、提交 |
| `raft/rawnode.go` | 对上层的 `RawNode` 封装 |
| `raft/storage.go` | Raft 持久化接口 |
| `proto/proto/eraftpb.proto` | Raft 消息与 Entry 定义 |
| `raft/doc.go` | **必读**：Ready 处理流程、MessageType 说明 |

### 核心设计约束

1. **逻辑时钟**：不要在本模块内设真实 timer；由上层周期性调用 `RawNode.Tick()`。
2. **异步消息**：需要发送的消息放入 `raft.msgs`，由上层取出并发出；收到的消息调用 `Step()`。
3. **Ready 批处理**：状态变更不立即落盘/发送，而是打包进 `Ready`，由上层统一处理后再 `Advance()`。

### 2AA：Leader 选举

**入口函数：** `raft.Raft.tick()`、`raft.Raft.Step()`

**要实现：**

- 选举超时 → 转 Candidate → 发 `MsgRequestVote`
- 收到投票 → 统计票数 → 过半则 `becomeLeader`
- Follower 收到合法 `MsgRequestVote` → 投票
- Leader 心跳超时 → 发 `MsgHeartbeat`（本课程与 AppendEntries 分离）
- 实现 `becomeFollower` / `becomeCandidate` / `becomeLeader` 等状态切换

**测试假设（来自官方 hints）：**

- 首次启动 term 为 0
- 新当选 Leader 在本 term 追加一条 **noop** entry
- 各 peer 选举超时应不同（随机化）
- 本地消息 `MsgHup` / `MsgBeat` / `MsgPropose` 不设 term

### 2AB：日志复制

**处理消息：** `MsgAppend`、`MsgAppendResponse`

**`RaftLog` 职责：**

- 追加、匹配、截断日志
- 更新 `commitIndex`、`lastIndex`、`term`
- 通过 `Storage` 接口读写持久化日志

**Leader vs Follower 追加逻辑不同**——仔细区分日志来源与一致性检查。

**测试假设：**

- Leader 推进 commitIndex 后，通过 `MsgAppend` 广播 commit 信息

### 2AC：RawNode 接口

**关键 API：**

- `RawNode.Tick()` → 驱动内部 tick
- `RawNode.Step(m)` → 处理消息
- `RawNode.Propose(data)` → 提议新日志
- `RawNode.Ready()` → 取出待处理批次
- `RawNode.Advance(rd)` → 通知 Raft 已处理完 Ready

**Ready 可能包含：**

- `Messages`：待发送
- `Entries`：待持久化
- `HardState`：term、vote、commit
- `CommittedEntries`：待应用到状态机
- `Snapshot`：快照元数据

**启动时：** 从 `Storage` 恢复 HardState 和最后日志，初始化 `Raft` 和 `RaftLog`。

---

## Part B：容错 KV 服务

### 核心概念

| 术语 | 含义 |
|------|------|
| **Store** | 一个 `tinykv-server` 实例 |
| **Peer** | Store 上运行的 Raft 节点 |
| **Region** | Peer 集合（Raft Group）；Project 2 仅单 Region |

### 代码地图

| 文件 | 职责 | 你要改 |
|------|------|--------|
| `kv/storage/raft_storage/raft_server.go` | `RaftStorage`，把读写转为 Raft 命令 | 读流程 |
| `kv/raftstore/raftstore.go` | raftstore 入口 | 读 |
| `kv/raftstore/raft_worker.go` | 轮询 raftCh、驱动 Raft | 读 |
| `kv/raftstore/peer_msg_handler.go` | 消息与 Ready 处理 | **是** |
| `kv/raftstore/peer.go` | Peer 状态 | **是** |
| `kv/raftstore/peer_storage.go` | 持久化 Raft 状态 | **是** |
| `proto/proto/raft_cmdpb.proto` | Get/Put/Delete/Snap 命令 | 读 |

### 请求全链路

```
Client RawGet/Put/...
  → Server handler
  → RaftStorage.Write/Reader
  → raftCh (MsgTypeRaftCmd)
  → proposeRaftCommand → Raft Propose
  → 日志 commit
  → HandleRaftReady apply
  → callback 返回客户端
```

### 任务 1：PeerStorage.SaveReadyState

**文件：** `kv/raftstore/peer_storage.go`

把 `raft.Ready` 中的内容写入 Badger：

- **raftdb**：追加 `Ready.Entries`；更新 `RaftLocalState`；删除不会被提交的旧日志
- 使用 `WriteBatch` 保证原子性

**元数据 key 格式**（见 project2 文档表格）：

| 数据 | DB |
|------|-----|
| raft log、RaftLocalState | raftdb |
| KV 数据、RaftApplyState、RegionLocalState | kvdb |

**初始化注意：** `RAFT_INIT_LOG_TERM` 和 `RAFT_INIT_LOG_INDEX` 为 **5**（不是 0），用于区分 conf change 后被动创建的 peer。

### 任务 2：proposeRaftCommand + HandleRaftReady

**文件：** `kv/raftstore/peer_msg_handler.go`、`peer.go`

**HandleMsg 处理：**

- `MsgTypeTick` → `RawNode.Tick()`
- `MsgTypeRaftCmd` → propose 客户端命令
- `MsgTypeRaftMessage` → 网络来的 Raft 消息 → `Step()`

**HandleRaftReady 伪代码：**

```go
rd := peer.RawNode.Ready()
peer.Storage.SaveReadyState(&rd)
sendMessages(rd.Messages)
for _, entry := range rd.CommittedEntries {
    apply(entry)  // Get/Put/Delete/Snap
}
peer.RawNode.Advance(rd)
```

**apply 时要：**

- 用 `WriteBatch` 原子更新 KV + `RaftApplyState`
- 记录 propose 时的 callback，apply 后回调
- Snap 命令需在 callback 中设置 badger Txn

### 错误处理（本阶段）

| 错误 | 场景 | 客户端行为 |
|------|------|-----------|
| `ErrNotLeader` | 在 Follower 上 propose | 重试其他 peer |
| `ErrStaleCommand` | Leader 变更导致旧命令失效 | 客户端重试 |

使用 `kv/raftstore/cmd_resp.go` 的 `BindRespError` 转换到 `errorpb.proto`。

### 读路径

测试要求：**非 Leader 或无最新数据时不能完成 Get**。

可选方案：

1. 把 Get 也走 Raft 日志（简单，推荐先实现）
2. Read Index / Lease Read 优化（Raft 论文 §8，非必须）

---

## Part C：日志 GC 与 Snapshot

### Raft 层

- Leader 需要发 Snapshot 时：调用 `Storage.Snapshot()` 得到 `eraftpb.Snapshot`
- Follower：`handleSnapshot` 从 `SnapshotMetadata` 恢复 term、commit、成员信息
- `eraftpb.Snapshot.data` 只是元数据，真实 KV 数据由 raftstore 生成

### raftstore 层

**新增 worker：**

| Worker | 作用 |
|--------|------|
| raftlog-gc | 异步删除已截断的 raft log |
| region | 生成 / 应用 snapshot |

**CompactLog 流程：**

1. `onRaftGcLogTick` 检查日志条数 > `RaftLogGcCountLimit`
2. propose `CompactLogRequest` admin 命令
3. apply 时更新 `RaftApplyState.RaftTruncatedState`
4. `ScheduleCompactLog` 通知 gc worker 删物理日志

**Snapshot 生成：**

1. `PeerStorage.Snapshot()` 投递 `RegionTaskGen` 到 region worker
2. region worker 扫描引擎生成 snapshot
3. 完成后 Raft 发送 snapshot 消息；接收走 `snap_runner.go`

**Snapshot apply（Ready 中）：**

1. 更新内存：`RaftLocalState`、`RaftApplyState`、`RegionLocalState`
2. 持久化到 kvdb + raftdb，删除过期数据
3. `snapState` → `Applying`，投递 `RegionTaskApply`，等待完成

---

## 测试与验收

```bash
make project2      # 全部
make project2a     # Raft 算法
make project2b     # 容错 KV（多测试，较慢）
make project2c     # Snapshot
```

**2B 测试覆盖：** 基础读写、并发、不可靠网络、分区、持久化恢复等。

**调试：**

```bash
LOG_LEVEL=debug make project2b
```

2A 部分测试建议**多次运行**以暴露竞态 bug。

---

## 常见陷阱

1. **Advance 前未持久化 HardState** → 违反 Raft 安全性
2. **apply index 未更新** → 重启后重复 apply 或跳过 entry
3. **callback 丢失** → 客户端永久阻塞
4. **Leader 选举后未追加 noop** → 2AA 测试失败
5. **Snapshot apply 未清理旧 key** → 数据不一致
6. **Get 在 Follower 直接读本地** → 违反线性一致性测试

---

## 与 Project 3 的衔接

Project 2 假设 **单 Region、单 Peer per Store**。Project 3 将在此基础上增加：

- ConfChange（增删 Peer）
- Region Split（多 Raft Group）
- Scheduler 调度

你在 2B 实现的 admin 命令框架和错误处理，是 3B 的直接基础。

---

## 推荐阅读

1. [Raft 论文](https://raft.github.io/raft.pdf) + [可视化](https://raft.github.io/)
2. `raft/doc.go` — Ready 处理契约
3. [TiKV Multi-Raft / Raftstore 设计（英文）](https://pingcap.com/blog/design-and-implementation-of-multi-raft/#raftstore)
4. [reading_list.md](../reading_list.md) — Consensus 章节
