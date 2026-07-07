# Raft 算法讲解（结合 TinyKV 项目）

> 本文面向 TinyKV 学习者：先建立 Raft 的直觉与术语，再对照本仓库代码结构，帮助你在做 Project 2/3 时「知道每段代码在解决什么问题」。
>
> 官方论文：[In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)  
> 交互可视化：[Raft Visualization](https://raft.github.io/)  
> 项目实现指导：[project2-RaftKV-guide.md](./project2-RaftKV-guide.md)

---

## 1. 要解决什么问题？

分布式系统里，多台机器各自有一份状态（例如 KV 数据）。网络会丢包、分区，机器会宕机——**怎样让多数派节点对同一份状态达成一致？**

Raft 的做法是：

1. 选出一个 **Leader**，所有写请求先交给 Leader；
2. Leader 把操作写入 **复制日志（replicated log）**；
3. 日志被 **多数派（quorum）** 复制并提交后，再 **应用到状态机**（在 TinyKV 里就是 KV 存储引擎）。

因此 Raft 保证：**只要过半节点存活且能互通，集群就能继续服务，且不会丢已提交的写。**

---

## 2. 核心抽象：复制状态机

```
客户端 ──写请求──▶ Leader ──追加日志──▶ Follower 们
                      │                    │
                      └──── 多数派提交 ─────┘
                               │
                               ▼
                         应用到 KV 状态机
```

三个关键对象：

| 概念 | 含义 | TinyKV 中的对应 |
|------|------|----------------|
| **Log Entry** | 一条带 `(index, term)` 的操作记录 | `eraftpb.Entry` |
| **Commit Index** | 已被多数派确认、可安全应用的日志位置 | `RaftLog.committed` |
| **Term** | 逻辑「任期」，每次新选举 term+1，用于识别过期 Leader | `Raft.Term` |

**Term 是理解 Raft 的钥匙：** 收到消息时先比 term——term 更小的一律拒绝或退位；term 更大则更新本地 term 并转 Follower。

---

## 3. 节点角色与状态转换

Raft 节点只有三种角色（见 `raft/raft.go` 的 `StateType`）：

```
                    选举超时 / 收不到 Leader 心跳
         ┌──────────────────────────────────────────┐
         ▼                                          │
    Follower ──选举超时──▶ Candidate ──获得多数票──▶ Leader
         ▲                    │                      │
         │                    │ 发现更高 term         │
         └────────────────────┴──────────────────────┘
                    发现更高 term 的 Leader / 消息
```

| 角色 | 职责 |
|------|------|
| **Follower** | 被动响应；收到合法 `AppendEntries` / `Heartbeat` 则重置选举计时 |
| **Candidate** | 发起选举，向其他节点请求投票（`MsgRequestVote`） |
| **Leader** | 处理客户端提议、复制日志、维护心跳、推进 commitIndex |

TinyKV 中状态切换函数（你需要在 Project 2A 实现）：

- `becomeFollower(term, lead)`
- `becomeCandidate()`
- `becomeLeader()` — 新 Leader 需在本 term **追加一条 noop 空日志**（测试要求）

---

## 4. 子问题一：Leader 选举

### 4.1 何时触发？

Follower 在 **选举超时（election timeout）** 内没收到当前 Leader 的心跳 → 自增 term → 转 Candidate → 给自己投票 → 向集群广播 `MsgRequestVote`。

### 4.2 投票规则（安全性）

Candidate 请求投票时会带上 `(lastLogIndex, lastLogTerm)`。Follower 只在以下情况投票：

- 该 term 还没投过票；且
- Candidate 的日志 **至少和自己一样新**（term 更大，或 term 相同但 index ≥ 自己）

这保证：**只有拥有最新已提交日志的节点才能当选**，避免 committed 条目被覆盖。

### 4.3 何时成为 Leader？

Candidate 收到 **过半数** 的 `MsgRequestVoteResponse`（且未 reject）→ `becomeLeader()`。

### 4.4 与 TinyKV 代码的对应

| 机制 | 代码位置 |
|------|----------|
| 逻辑时钟 tick | `Raft.tick()` — 上层周期性调用 `RawNode.Tick()` |
| 选举超时 | `electionElapsed` vs `electionTimeout` |
| 心跳超时 | `heartbeatElapsed` vs `heartbeatTimeout`（仅 Leader） |
| 消息入口 | `Raft.Step(m)` |
| 待发送消息 | `Raft.msgs` — 由上层取出并 RPC 发出 |
| 消息定义 | `proto/proto/eraftpb.proto` |

**TinyKV 与论文的差异：** 本课程把 **Heartbeat** 和 **AppendEntries** 拆成两种消息（`MsgHeartbeat` / `MsgAppend`），逻辑更清晰。本地触发消息 `MsgHup`（选举）、`MsgBeat`（Leader 发心跳）不走网络。

---

## 5. 子问题二：日志复制

### 5.1 正常流程

1. 客户端（或上层 KV）调用 `RawNode.Propose(data)`；
2. Leader 把 `data` 封装为 `EntryNormal` 追加到本地日志；
3. Leader 向每个 Follower 发 `MsgAppend`（含 entries + leaderCommit）；
4. Follower 检查 **prevLogIndex / prevLogTerm** 是否匹配：
   - 匹配 → 追加新 entries，返回 success；
   - 不匹配 → reject，Leader **回退 nextIndex 并重试**（找共同前缀）；
5. Leader 统计每个 Follower 的 `Match` 索引，当某 index 被 **多数派** 复制 → 更新 `commitIndex`；
6. 已 commit 的 entries 通过 `Ready.CommittedEntries` 交给上层 **apply** 到 KV。

### 5.2 日志结构（RaftLog）

`raft/log.go` 中的 `RaftLog` 管理内存中的日志窗口：

```
 snapshot/first ..... applied .... committed .... stabled ..... last
 --------|--------------------------------------------------------|
                         未压缩的 log entries
```

| 字段 | 含义 |
|------|------|
| `entries` | 内存中的日志条目 |
| `stabled` | 已持久化到 Storage 的最高 index |
| `committed` | 已被 quorum 确认的最高 index |
| `applied` | 已应用到状态机的最高 index（不变量：`applied ≤ committed`） |

持久化通过 `raft/storage.go` 的 `Storage` 接口完成；Project 2B 里 `PeerStorage` 用 Badger 实现。

### 5.3 Leader 的复制进度

`Raft.Prs map[uint64]*Progress` 记录每个 Follower 的：

- `Match`：已知已复制的最高 index
- `Next`：下次 `MsgAppend` 从哪条开始发

Leader 根据 `MsgAppendResponse` 更新这两个值，实现 **按需回退、流水线复制**。

---

## 6. 子问题三：安全性（为什么这样设计就够？）

Raft 通过以下机制保证线性一致性的核心性质：

1. **Leader 完备性：** 只有日志够新的节点能当选；
2. **Leader 只追加：** Leader 从不删除或覆盖自己的 entries，只追加；
3. **提交规则：** Leader 只能提交 **当前 term** 的条目（或间接通过当前 term 的 noop 推进），避免「假提交」；
4. **Term 比较：** 所有 RPC 都带 term，过期 Leader 的请求会被拒绝。

读 TinyKV 测试失败日志时，常见根因是：term 没更新、commitIndex 推进不对、或 Leader/Follower 追加逻辑混用。

---

## 7. TinyKV 的分层：Raft 模块 vs 上层应用

Raft 本身 **不直接存 KV**，它只产出 **已提交的日志**。TinyKV 把「应用层」拆成清晰边界：

```
┌─────────────────────────────────────────────────────────┐
│  kv/raftstore（Project 2B/3）                            │
│  Peer、Region、apply 日志到 BadgerEngine                  │
└───────────────────────────┬─────────────────────────────┘
                            │ RawNode.Propose / Ready
┌───────────────────────────▼─────────────────────────────┐
│  raft/（Project 2A）                                       │
│  Raft 状态机、RaftLog、选举与复制                          │
└───────────────────────────┬─────────────────────────────┘
                            │ Storage 接口
┌───────────────────────────▼─────────────────────────────┐
│  持久化（MemoryStorage 测试 / PeerStorage + Badger 生产）   │
└─────────────────────────────────────────────────────────┘
```

### 7.1 RawNode 与 Ready（必读 `raft/doc.go`）

Raft 与上层 **异步协作**：状态变更不会立刻落盘或发网络包，而是打包进 `Ready`：

```go
// 典型事件循环（摘自 raft/doc.go，简化）
for {
    select {
    case <-ticker:
        rawNode.Tick()
    case rd := <-rawNode.Ready():
        // 1. 先持久化 HardState、Entries、Snapshot
        saveToStorage(rd.HardState, rd.Entries, rd.Snapshot)
        // 2. 再发送 Messages
        send(rd.Messages)
        // 3. 应用 CommittedEntries 到状态机
        for _, e := range rd.CommittedEntries {
            apply(e)
        }
        // 4. 通知 Raft 已处理完毕
        rawNode.Advance(rd)
    }
}
```

**顺序很重要：** 必须先持久化 HardState/Entries，再发 Messages——否则宕机后可能丢日志却以为已复制。

### 7.2 主要消息类型速查

| MessageType | 方向 | 作用 |
|-------------|------|------|
| `MsgHup` | 本地 | 触发选举 |
| `MsgBeat` | 本地 | Leader 触发心跳 |
| `MsgPropose` | 本地 | 上层提议新日志 |
| `MsgRequestVote` | RPC | 请求投票 |
| `MsgRequestVoteResponse` | RPC | 投票结果 |
| `MsgAppend` | RPC | 复制日志条目 |
| `MsgAppendResponse` | RPC | 复制应答 |
| `MsgHeartbeat` | RPC | Leader 维持权威 |
| `MsgSnapshot` | RPC | 日志过长时发快照（Project 2C） |

完整说明见 `raft/doc.go` 末尾的 MessageType 章节。

---

## 8. Project 2/3 中 Raft 的演进

| 阶段 | 新增能力 | 关键文件 |
|------|----------|----------|
| **2A** | 基础 Raft：选举 + 复制 + RawNode | `raft/*.go` |
| **2B** | 日志应用到 KV；多 Peer 组集群 | `kv/raftstore/` |
| **2C** | 日志 GC + Snapshot 压缩 | `RaftLog.maybeCompact`, `handleSnapshot` |
| **3A** | 成员变更（ConfChange）、Leader 转移 | `addNode`/`removeNode`, `EntryConfChange` |
| **3B** | Multi-Raft：按 Region 分片，每 Region 一个 Raft 组 | `kv/raftstore/`, `scheduler/` |

**Multi-Raft 直觉：** 一个 TiKV/TinyKV 节点上跑很多个 Raft 组（每个 Region 一组），而不是全局只有一个 Raft 集群——这是水平扩展的关键。

---

## 9. 动手学习建议

### 9.1 推荐阅读顺序

1. 本文（建立地图）
2. [Raft 可视化网站](https://raft.github.io/) — 拖拽看选举与复制
3. `raft/doc.go` — TinyKV 对 Ready/Message 的约定
4. [project2-RaftKV-guide.md](./project2-RaftKV-guide.md) — 逐步实现清单
5. 论文 Figure 2（RequestVote / AppendEntries RPC 伪代码）

### 9.2 实现与调试顺序（2A）

```
make project2aa   # 选举
make project2ab   # 日志复制
make project2ac   # RawNode / Ready
make project2a    # 2A 全部
```

每步失败时，对照：

- term / vote / commit 是否正确持久化；
- Leader 是否在当选后 append noop；
- `MsgAppend` 的 reject 回退逻辑；
- Ready 处理顺序是否先 store 再 send。

### 9.3 自测问题（检验是否真懂）

1. 为什么 Follower 收到更高 term 的 `MsgAppend` 要退位？
2. 为什么新 Leader 要提交一条 noop entry？
3. `committed` 和 `applied` 为什么可能不相等？中间状态代表什么？
4. 网络分区后，少数派一侧会发生什么？客户端写会怎样？
5. TinyKV 为什么用 tick 而不是 Raft 内部 sleep？

---

## 10. 本仓库关键文件索引

| 路径 | 说明 |
|------|------|
| `raft/raft.go` | Raft 核心状态与 Step/tick |
| `raft/log.go` | 日志索引与 committed/applied |
| `raft/rawnode.go` | 上层 API：Propose、Ready、Advance |
| `raft/storage.go` | 持久化接口 |
| `raft/doc.go` | 官方级设计文档（英文） |
| `proto/proto/eraftpb.proto` | Entry、Message、HardState 定义 |
| `kv/raftstore/` | Raft 与 KV 的结合（2B 起） |
| `doc/project2-RaftKV.md` | 课程英文说明 |
| `doc/guides/project2-RaftKV-guide.md` | 中文实现指导 |

---

## 11. 小结

| 你需要的直觉 | 一句话 |
|--------------|--------|
| Raft 是什么 | 通过 **Leader + 复制日志 + 多数派提交** 实现容错一致性的共识算法 |
| 日志是什么 | 有序、带 term 的操作记录；commit 之后才可 apply |
| TinyKV 怎么用 Raft | 2A 实现算法；2B 把 committed entry apply 到 KV；3 扩展为多 Region |
| 代码从哪读起 | `eraftpb.proto` → `raft.go` / `log.go` → `doc.go` → `rawnode.go` |

把 Raft 想成 **「带规则的多副本写日志系统」**，KV 只是日志的应用目标。先让选举和复制在测试里跑通，再去看 `raftstore` 如何把 `Put/Get` 变成 `Propose` 的数据——整个 TinyKV 的故事就串起来了。
