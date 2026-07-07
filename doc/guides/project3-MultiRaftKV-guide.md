# Project 3 详细指导：MultiRaftKV

> 基础文档：[project3-MultiRaftKV.md](../project3-MultiRaftKV.md)

## 目标

把单 Raft Group 的 KV 扩展为 **Multi-Raft** 架构：多个 Region 并行服务不同 key range，由 **Scheduler（PD）** 收集心跳并生成调度任务（balance region）。

完成后，系统将具备水平扩展的基础能力。

## 为什么需要 Multi-Raft

Project 2 的瓶颈：

- 所有数据在一个 Raft Group → 写入串行 commit
- 单 Region → 无法利用多节点并行

Multi-Raft 把 key space 切成多个 **Region**（range 分片），每个 Region 是独立 Raft Group，可并行处理不同 key 的请求。

```
         Scheduler (PD)
        /    |    \
   Store1  Store2  Store3
   [R1]    [R1,R2]  [R2]
```

## 整体结构

| 部分 | 内容 | 测试 |
|------|------|------|
| **3A** | Raft：Leader Transfer + ConfChange | `make project3a` |
| **3B** | raftstore：TransferLeader / ChangePeer / Split | `make project3b` |
| **3C** | Scheduler：心跳处理 + Region 均衡 | `make project3c` |

---

## Part A：Raft 成员变更与 Leader 转移

### 代码地图

| 文件 | 改动 |
|------|------|
| `raft/raft.go` | 处理新消息类型 |
| `raft/rawnode.go` | `ProposeConfChange`、`TransferLeader` |
| `proto/proto/eraftpb.proto` | 新 Message / Entry 类型 |

### Leader Transfer

**新消息：**

- `MsgTransferLeader`（本地消息，from = 目标 peer id）
- `MsgTimeoutNow`（发给目标，触发立即选举）

**流程：**

1. 当前 Leader 收到 `MsgTransferLeader`
2. 检查目标日志是否足够新；否则先发 `MsgAppend` 帮助追赶，并暂停接受新 propose
3. 目标合格后发送 `MsgTimeoutNow`
4. 目标收到后立即选举（可用 `Step(MsgHup)`），高 term + 新日志 → 成为 Leader

### ConfChange

**算法：** 一次只增或删一个 peer（非 joint consensus）。

**流程：**

1. Leader 调用 `RawNode.ProposeConfChange(cc)`
2. 生成 `EntryConfChange` 类型日志
3. commit 后上层调用 `RawNode.ApplyConfChange(cc)`
4. 再调用 `addNode` / `removeNode` 更新本节点 peer 集合

**Hints：**

- `ConfChange` 序列化：`pb.ConfChange.Marshal` → 写入 `Entry.Data`
- `MsgTransferLeader` 是本地消息，不经网络

---

## Part B：raftstore 管理命令

### 代码地图

| 文件 | 改动 |
|------|------|
| `kv/raftstore/peer_msg_handler.go` | admin 命令 propose / apply |
| `kv/raftstore/peer.go` | split、conf change 逻辑 |

### Admin 命令类型

| 命令 | 状态 | 说明 |
|------|------|------|
| `CompactLog` | Project 2C 已实现 | 日志截断 |
| `TransferLeader` | **本阶段** | 调用 `RawNode.TransferLeader`，不走 `Propose` |
| `ChangePeer` | **本阶段** | AddNode / RemoveNode |
| `Split` | **本阶段** | Region 分裂 |

### RegionEpoch

`metapb.Region` 的元数据版本控制：

| 字段 | 何时递增 |
|------|---------|
| `conf_ver` | ConfChange（增删 peer） |
| `version` | Split |

用于检测过期 Region 信息，防止网络隔离下双 Leader。

### TransferLeader

- 作为 Raft 命令进入流程，但执行时调用 `TransferLeader()` 而非 `Propose()`

### ChangePeer

**AddNode：**

1. `ProposeConfChange`
2. commit 后更新 `RegionLocalState`（epoch、peers）
3. `ApplyConfChange`
4. 新 peer 由 leader 心跳触发 `maybeCreatePeer()` 创建（初始 log index/term = 0）
5. Leader 发现 log gap → 直接发 Snapshot

**RemoveNode：**

1. 同上 propose + apply
2. 显式 `destroyPeer()` 停止被移除的 Raft 模块
3. 更新 `storeMeta` 中的 region 信息
4. 测试会重复调度同一 conf change → 需处理重复命令

### Split Region

**背景：** 初始 Region range 为 `["", "")` 表示整个 key space（循环空间）。

**触发：**

1. `split_checker` 定期检查 Region 大小
2. 生成 split key → `onPrepareSplitRegion`
3. pd worker 向 Scheduler 申请新 Region / Peer id
4. `onAskSplit` 收到 id 后 propose Split admin 命令

**你的任务：** apply Split 命令

- 一个 Region 继承原 id，缩小 `end_key`（或调整 start）
- 另一个新建 Region + Peer，注册到 `router.regions` 和 `storeMeta.regionRanges`
- 更新双方 `RegionEpoch.version`

**边界情况：**

- 网络隔离下 snapshot 可能与已有 range 重叠 → `checkSnapshot()` 有过检逻辑
- 用 `engine_util.ExceedEndKey()` 比较 end key（`""` 表示正无穷）

**新错误：**

- `ErrRegionNotFound`
- `ErrKeyNotInRegion`
- `ErrEpochNotMatch`

---

## Part C：Scheduler

### 代码地图

| 文件 | 改动 |
|------|------|
| `scheduler/server/cluster.go` | `processRegionHeartbeat` |
| `scheduler/server/schedulers/balance_region.go` | `Schedule` |

### 心跳处理：processRegionHeartbeat

**目标：** 维护集群 Region 拓扑 view，并判断心跳是否可信。

**为什么不能信任每个心跳：**

网络分区时，不同节点可能对同一 Region 报告矛盾的 leader 信息。用 **RegionEpoch** 判断新旧：

1. 若本地已有同 id Region，且心跳的 `conf_ver` 或 `version` **任一**小于本地 → **stale，丢弃**
2. 若本地无此 id，扫描所有与其 range 重叠的 Region；心跳 epoch 必须 ≥ 全部重叠 Region，否则 stale

**何时不能跳过更新（需写入）：**

- `version` 或 `conf_ver` 变大
- Leader 变更
- 存在 pending peer
- `ApproximateSize` 变化
- （不必找充要条件，多更新不影响正确性）

**写入操作：**

- `RaftCluster.core.PutRegion` — 更新 region tree
- `RaftCluster.core.UpdateStoreStatus` — 更新 store 统计（leader 数、region 数等）

### Region 均衡：Schedule

**目标：** 避免单个 Store 承载过多 Region。

**算法概要：**

1. 筛选 **up** 且宕机时间 < `MaxStoreDownTime` 的 stores
2. 按 region 总大小排序
3. 从最大的 store 尝试选出要迁移的 region：
   - 优先 **pending** region（可能磁盘压力大）
   - 其次 **follower** region
   - 再次 **leader** region
4. 目标 store 选 region 总大小最小的
5. 若 `源大小 - 目标大小 > 2 * region近似大小`，才认为值得迁移
6. 用 `CreateMovePeerOperator` 生成 `AddPeer → (可选 TransferLeader) → RemovePeer`

**选 region 的辅助方法：**

- `GetPendingRegionsWithLock`
- `GetFollowersWithLock`
- `GetLeadersWithLock`

随机选一个即可。

---

## 测试与验收

```bash
make project3       # 全部
make project3a      # Raft conf change + transfer
make project3b      # raftstore（测试多且含不可靠网络）
make project3c      # Scheduler
```

**3B 重点测试：**

- TransferLeader、ConfChange（含 remove leader、恢复）
- Split（含不可靠网络、与 conf change 组合）
- Snapshot + 分区恢复

**3C：**

```bash
go test ./scheduler/server ./scheduler/server/schedulers -check.f="3C"
```

---

## 常见陷阱

1. **ConfChange 在 apply 前调用 addNode** → 违反 Raft 安全性
2. **Split 后未注册新 peer 到 router** → 请求路由失败
3. **忽略重复 conf change 命令** → 3B 测试 hang 或 panic
4. **心跳 stale 检查不完整** → Scheduler 拓扑混乱
5. **均衡阈值太小** → region 来回迁移（乒乓效应）
6. **RemoveNode 未 destroyPeer** → 幽灵 peer 继续跑 Raft

---

## 与 Project 4 的衔接

- Multi-Region 意味着一个事务的 key 可能跨多个 Region → 客户端发多个 `KvPrewrite`
- Scheduler 在 Project 4 还负责 **分配全局时间戳**（TSO），与事务 commit 密切相关
- Region 错误（`ErrEpochNotMatch` 等）在事务 API 中同样要正确处理

---

## 推荐阅读

1. [TiKV Multi-Raft](https://tikv.org/deep-dive/scalability/multi-raft/)
2. [TiKV 调度设计](https://pingcap.com/blog/tidb-internal-scheduling/)
3. [Range vs Hash 分片](https://tikv.org/docs/deep-dive/scalability/data-sharding/)
4. project3 文档中的 balance 示意图（`doc/imgs/balance1.png`、`balance2.png`）
