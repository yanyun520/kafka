# Kafka 消费者组与重平衡机制深度解析

本文档详细分析 Apache Kafka 中消费者组（Consumer Group）的交互机制，以及新消费者加入时触发的重平衡（Rebalance）全过程。

---

## 一、消费者组架构概览

### 1.1 核心组件

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              Kafka Cluster                                       │
│  ┌───────────────────────────────────────────────────────────────────────────┐  │
│  │                     GroupCoordinator (Broker)                              │  │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────┐ │  │
│  │  │  GroupMetadata  │  │  MemberMetadata │  │  __consumer_offsets Topic   │ │  │
│  │  │  - groupId      │  │  - memberId     │  │  (Offset Storage)           │ │  │
│  │  │  - generationId │  │  - subscription │  └─────────────────────────────┘ │  │
│  │  │  - leaderId     │  │  - assignment   │                                    │  │
│  │  │  - state        │  │  - heartbeat    │                                    │  │
│  │  └─────────────────┘  └─────────────────┘                                    │  │
│  └───────────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────┘
                                       ▲
                                       │ JoinGroup / SyncGroup / Heartbeat / LeaveGroup
                                       │
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           Consumer Group Members                                 │
│                                                                                  │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐              │
│  │   Consumer-1     │  │   Consumer-2     │  │   Consumer-N     │              │
│  │   (Leader)       │  │   (Follower)     │  │   (Follower)     │              │
│  │                  │  │                  │  │                  │              │
│  │  ┌────────────┐  │  │  ┌────────────┐  │  │  ┌────────────┐  │              │
│  │  │ Consumer   │  │  │  │ Consumer   │  │  │  │ Consumer   │  │              │
│  │  │Coordinator │  │  │  │Coordinator │  │  │  │Coordinator │  │              │
│  │  │            │  │  │  │            │  │  │  │            │  │              │
│  │  │ - Assignor │  │  │  │ - metadata │  │  │  │ - metadata │  │              │
│  │  │ - listener │  │  │  │ - commit   │  │  │  │ - commit   │  │              │
│  │  └────────────┘  │  │  └────────────┘  │  │  └────────────┘  │              │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘              │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 1.2 消费者组状态机

```
                    ┌─────────────────────────────────────────────────────────────┐
                    │                                                             │
                    ▼                                                             │
┌───────────┐   ┌─────────────┐   ┌─────────────────┐   ┌───────────────┐        │
│   Empty   │──▶│Preparing    │──▶│ Completing      │──▶│    Stable     │        │
│           │   │Rebalance    │   │ Rebalance       │   │               │        │
│           │◀──│             │◀──│                 │◀──│               │        │
└───────────┘   └─────────────┘   └─────────────────┘   └───────────────┘        │
      │                                                              │            │
      │                    Group becomes Empty                       │            │
      │◀─────────────────────────────────────────────────────────────┴────────────┘
      │
      ▼
┌───────────┐
│   Dead    │ (Final state before cleanup)
└───────────┘
```

**状态说明**（来自 `GroupMetadata.scala` 第36-124行）：

| 状态 | 说明 | 有效前置状态 |
|------|------|-------------|
| **Empty** | 组内无成员，仅用于偏移量提交 | PreparingRebalance |
| **PreparingRebalance** | 准备重平衡，收集成员加入请求 | Stable, CompletingRebalance, Empty |
| **CompletingRebalance** | 等待 Leader 分配分区 | PreparingRebalance |
| **Stable** | 重平衡完成，正常消费 | CompletingRebalance |
| **Dead** | 组将被删除 | 任意状态 |

---

## 二、消费者组交互机制

### 2.1 消费者端协调器 (ConsumerCoordinator)

**源码位置**：`clients/src/main/java/org/apache/kafka/clients/consumer/internals/ConsumerCoordinator.java`

```java
/**
 * 消费者协调器 - 管理消费者与 GroupCoordinator 的交互（第89-208行）
 */
public final class ConsumerCoordinator extends AbstractCoordinator {
    private final GroupRebalanceConfig rebalanceConfig;
    private final List<ConsumerPartitionAssignor> assignors;  // 分区分配策略
    private final SubscriptionState subscriptions;             // 订阅状态
    private boolean isLeader = false;                          // 是否为 Leader
    
    /**
     * 构造函数 - 初始化协调器
     */
    public ConsumerCoordinator(GroupRebalanceConfig rebalanceConfig,
                               LogContext logContext,
                               ConsumerNetworkClient client,
                               List<ConsumerPartitionAssignor> assignors,
                               ConsumerMetadata metadata,
                               SubscriptionState subscriptions,
                               ...) {
        super(rebalanceConfig, logContext, client, metrics, metricGrpPrefix, time);
        
        // 选择重平衡协议（优先选择最高级协议）
        List<RebalanceProtocol> supportedProtocols = 
            new ArrayList<>(assignors.get(0).supportedProtocols());
        
        for (ConsumerPartitionAssignor assignor : assignors) {
            supportedProtocols.retainAll(assignor.supportedProtocols());
        }
        Collections.sort(supportedProtocols);
        this.protocol = supportedProtocols.get(supportedProtocols.size() - 1);
    }
}
```

### 2.2 服务端协调器 (GroupCoordinator)

**源码位置**：`core/src/main/scala/kafka/coordinator/group/GroupCoordinator.scala`

```scala
/**
 * GroupCoordinator 处理组成员关系和偏移量管理（第53-60行）
 */
class GroupCoordinator(val brokerId: Int,
                       val groupConfig: GroupConfig,
                       val offsetConfig: OffsetConfig,
                       val groupManager: GroupMetadataManager,
                       val heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat],
                       val joinPurgatory: DelayedOperationPurgatory[DelayedJoin],
                       time: Time,
                       metrics: Metrics) extends Logging {
  
  type JoinCallback = JoinGroupResult => Unit
  type SyncCallback = SyncGroupResult => Unit
  
  /**
   * 处理 JoinGroup 请求（第156-201行）
   */
  def handleJoinGroup(groupId: String,
                      memberId: String,
                      groupInstanceId: Option[String],
                      requireKnownMemberId: Boolean,
                      clientId: String,
                      clientHost: String,
                      rebalanceTimeoutMs: Int,
                      sessionTimeoutMs: Int,
                      protocolType: String,
                      protocols: List[(String, Array[Byte])],
                      responseCallback: JoinCallback): Unit = {
    
    // 1. 验证组状态
    validateGroupStatus(groupId, ApiKeys.JOIN_GROUP).foreach { error =>
      responseCallback(JoinGroupResult(memberId, error))
      return
    }
    
    // 2. 获取或创建组
    groupManager.getOrMaybeCreateGroup(groupId, isUnknownMember) match {
      case None =>
        responseCallback(JoinGroupResult(memberId, Errors.UNKNOWN_MEMBER_ID))
        
      case Some(group) =>
        group.inLock {
          // 3. 检查是否接受成员
          if (!acceptJoiningMember(group, memberId)) {
            responseCallback(JoinGroupResult(..., Errors.GROUP_MAX_SIZE_REACHED))
          } 
          // 4. 新成员加入
          else if (isUnknownMember) {
            doUnknownJoinGroup(group, groupInstanceId, requireKnownMemberId, ...)
          } 
          // 5. 已知成员重新加入
          else {
            doJoinGroup(group, memberId, groupInstanceId, ...)
          }
          
          // 6. 尝试完成 Join（检查是否所有成员都已加入）
          if (group.is(PreparingRebalance)) {
            joinPurgatory.checkAndComplete(GroupKey(group.groupId))
          }
        }
    }
  }
}
```

---

## 三、重平衡触发条件

### 3.1 触发重平衡的场景

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        重平衡触发条件                                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  1. 新成员加入组                              2. 现有成员离开组              │
│     ┌──────────┐                                 ┌──────────┐               │
│     │ Consumer │ ──JoinGroup──▶                  │ Consumer │ ──LeaveGroup──▶│
│     │   加入   │                                 │   离开   │               │
│     └──────────┘                                 └──────────┘               │
│                                                                             │
│  3. Leader 重新加入                          4. 成员心跳超时                 │
│     ┌──────────┐                                 ┌──────────┐               │
│     │  Leader  │ ──JoinGroup──▶                  │  Member  │ ──Timeout──▶   │
│     │  触发    │  (主动触发重平衡)                │  失效    │               │
│     └──────────┘                                 └──────────┘               │
│                                                                             │
│  5. 成员元数据变更                           6. 订阅主题分区变更             │
│     ┌──────────┐                                 ┌──────────┐               │
│     │ 订阅变更 │ ──JoinGroup──▶                  │ 新增分区 │ ──Metadata──▶  │
│     └──────────┘                                 └──────────┘               │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.2 服务端判断逻辑

**源码位置**：`GroupCoordinator.scala` 第131-154行

```scala
/**
   * 验证是否接受加入的成员
   */
  private def acceptJoiningMember(group: GroupMetadata, member: String): Boolean = {
    group.currentState match {
      // 空组或死亡组总是接受
      case Empty | Dead => true

      // 准备重平衡阶段：检查等待成员数是否超过最大组大小
      case PreparingRebalance =>
        (group.has(member) && group.get(member).isAwaitingJoin) ||
          group.numAwaiting < groupConfig.groupMaxSize

      // 完成重平衡或稳定阶段：现有成员可直接加入，新成员需检查组大小
      case CompletingRebalance | Stable =>
        group.has(member) || group.size < groupConfig.groupMaxSize
    }
  }
```

### 3.3 准备重平衡的核心逻辑

**源码位置**：`GroupCoordinator.scala` 第1123-1145行

```scala
/**
   * 准备重平衡（核心方法）
   */
  private[group] def prepareRebalance(group: GroupMetadata, reason: String): Unit = {
    // 1. 如果组正在完成重平衡，取消所有成员的同步请求
    if (group.is(CompletingRebalance))
      resetAndPropagateAssignmentError(group, Errors.REBALANCE_IN_PROGRESS)

    // 2. 创建延迟重平衡操作
    val delayedRebalance = if (group.is(Empty))
      // 空组使用 InitialDelayedJoin，允许新成员快速加入
      new InitialDelayedJoin(this, joinPurgatory, group, 
        groupConfig.groupInitialRebalanceDelayMs, ...)
    else
      // 已有成员的组使用 DelayedJoin
      new DelayedJoin(this, group, group.rebalanceTimeoutMs)

    // 3. 转换组状态为 PreparingRebalance
    group.transitionTo(PreparingRebalance)

    info(s"Preparing to rebalance group ${group.groupId} in state ${group.currentState} " +
      s"with old generation ${group.generationId} (reason: $reason)")

    // 4. 将延迟操作加入净化器
    val groupKey = GroupKey(group.groupId)
    joinPurgatory.tryCompleteElseWatch(delayedRebalance, Seq(groupKey))
  }
```

---

## 四、重平衡执行全流程

### 4.1 完整流程图

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           重平衡完整流程                                         │
└─────────────────────────────────────────────────────────────────────────────────┘

  阶段1: 触发重平衡
  ════════════════
  
  Consumer-1 (Leader)          GroupCoordinator            Consumer-2
        │                            │                        │
        │  1. 订阅变更/重新加入        │                        │
        │ ──────────────────────────▶│                        │
        │     JoinGroup Request      │                        │
        │                            │                        │
        │                            │  2. 检查组成员          │
        │                            │  - 状态: Stable         │
        │                            │  - 触发重平衡           │
        │                            │                        │
        │                            │  3. 广播重平衡信号       │
        │◀────────────────────────── │───────────────────────▶│
        │   JoinGroup Response       │   (通过心跳响应)         │
        │   (REBALANCE_IN_PROGRESS)  │                        │
        │                            │                        │
        │                            │                        │
        ▼                            ▼                        ▼

  阶段2: 收集加入请求 (PreparingRebalance)
  ═════════════════════════════════════════
  
  Consumer-1                   GroupCoordinator            Consumer-2
        │                            │                        │
        │  4. 重新发送 JoinGroup      │                        │
        │ ──────────────────────────▶│                        │
        │                            │                        │
        │                            │◀───────────────────────│
        │                            │     JoinGroup Request  │
        │                            │                        │
        │                            │  5. 等待所有成员加入     │
        │                            │  - DelayedJoin 操作     │
        │                            │  - 超时: rebalanceTimeout│
        │                            │                        │
        │                            │                        │

  阶段3: 选举 Leader & 收集元数据
  ═════════════════════════════════
  
  Consumer-1 (Leader)          GroupCoordinator            Consumer-2 (Follower)
        │                            │                        │
        │◀────────────────────────── │───────────────────────▶│
        │   JoinGroup Response       │   JoinGroup Response   │
        │   - generationId           │   - generationId       │
        │   - memberId               │   - memberId           │
        │   - isLeader=true          │   - isLeader=false     │
        │   - all members metadata   │                        │
        │                            │                        │
        │                            │                        │

  阶段4: 分配分区 (CompletingRebalance)
  ═════════════════════════════════════
  
  Consumer-1 (Leader)          GroupCoordinator            Consumer-2 (Follower)
        │                            │                        │
        │  6. 执行分区分配             │                        │
        │    performAssignment()      │                        │
        │                            │                        │
        │  7. 发送 SyncGroup          │                        │
        │     (包含所有成员分配)        │                        │
        │ ──────────────────────────▶│                        │
        │                            │                        │
        │                            │◀───────────────────────│
        │                            │     SyncGroup Request  │
        │                            │     (空分配)            │
        │                            │                        │

  阶段5: 传播分配结果 & 稳定
  ═══════════════════════════════
  
  Consumer-1                   GroupCoordinator            Consumer-2
        │                            │                        │
        │◀────────────────────────── │───────────────────────▶│
        │   SyncGroup Response       │   SyncGroup Response   │
        │   (partition assignment)   │   (partition assignment)│
        │                            │                        │
        │                            │  8. 转换状态为 Stable    │
        │                            │                        │
        ▼                            ▼                        ▼
     开始消费                                                开始消费
```

### 4.2 消费者端详细流程

**源码位置**：`AbstractCoordinator.java` 第490-560行

```java
/**
     * 确保消费者活跃于组中（触发重平衡的核心方法）
     */
    synchronized RequestFuture<ByteBuffer> ensureActiveGroup() {
        // 1. 如果需要重新加入组
        if (rejoinNeeded) {
            // 1.1 准备重新加入 - 提交偏移量等清理工作
            if (needsJoinPrepare) {
                onJoinPrepare(generation.generationId, generation.memberId);
                needsJoinPrepare = false;
            }
            
            // 1.2 更新状态为准备重平衡
            state = MemberState.PREPARING_REBALANCE;
            
            // 1.3 记录重平衡开始时间
            if (lastRebalanceStartMs == -1L)
                lastRebalanceStartMs = time.milliseconds();
            
            // 1.4 发送 JoinGroup 请求
            joinFuture = sendJoinGroupRequest();
        }
        return joinFuture;
    }

    /**
     * 发送 JoinGroup 请求（第535-560行）
     */
    RequestFuture<ByteBuffer> sendJoinGroupRequest() {
        // 构建 JoinGroup 请求
        JoinGroupRequest.Builder requestBuilder = new JoinGroupRequest.Builder(
            new JoinGroupRequestData()
                .setGroupId(rebalanceConfig.groupId)
                .setSessionTimeoutMs(this.rebalanceConfig.sessionTimeoutMs)
                .setMemberId(this.generation.memberId)
                .setGroupInstanceId(this.rebalanceConfig.groupInstanceId.orElse(null))
                .setProtocolType(protocolType())
                .setProtocols(metadata())  // 消费者订阅的 Topic 列表
                .setRebalanceTimeoutMs(this.rebalanceConfig.rebalanceTimeoutMs)
        );
        
        // 发送请求并设置响应处理器
        return client.send(coordinator, requestBuilder, joinGroupTimeoutMs)
                     .compose(new JoinGroupResponseHandler(generation));
    }
```

### 4.3 JoinGroup 响应处理

**源码位置**：`AbstractCoordinator.java` 第562-667行

```java
private class JoinGroupResponseHandler extends CoordinatorResponseHandler<JoinGroupResponse, ByteBuffer> {
    @Override
    public void handle(JoinGroupResponse joinResponse, RequestFuture<ByteBuffer> future) {
        Errors error = joinResponse.error();
        
        if (error == Errors.NONE) {
            // 成功加入
            synchronized (AbstractCoordinator.this) {
                state = MemberState.COMPLETING_REBALANCE;
                
                // 更新 generation 信息
                AbstractCoordinator.this.generation = new Generation(
                    joinResponse.data().generationId(),
                    joinResponse.data().memberId(),
                    joinResponse.data().protocolName()
                );
                
                log.info("Successfully joined group with generation {}", 
                    AbstractCoordinator.this.generation);
                
                // 区分 Leader 和 Follower
                if (joinResponse.isLeader()) {
                    onJoinLeader(joinResponse).chain(future);
                } else {
                    onJoinFollower().chain(future);
                }
            }
        } 
        // 处理各种错误情况
        else if (error == Errors.REBALANCE_IN_PROGRESS) {
            // 需要重新加入
            future.raise(error);
        } 
        else if (error == Errors.UNKNOWN_MEMBER_ID) {
            // 重置 member id 并重试
            resetGenerationOnResponseError(ApiKeys.JOIN_GROUP, error);
            future.raise(error);
        }
        // ... 其他错误处理
    }
}
```

### 4.4 Leader 执行分区分配

**源码位置**：`ConsumerCoordinator.java` 第400-470行

```java
@Override
protected Map<String, ByteBuffer> performAssignment(String leaderId,
                                                      String assignmentStrategy,
                                                      List<JoinGroupResponseMember> allSubscribedTopics) {
    // 1. 查找分配策略
    ConsumerPartitionAssignor assignor = lookupAssignor(assignmentStrategy);
    
    // 2. 收集所有成员的订阅信息
    Map<String, Subscription> subscriptions = new HashMap<>();
    for (JoinGroupResponseMember member : allSubscribedTopics) {
        Subscription subscription = ConsumerProtocol.deserializeSubscription(
            ByteBuffer.wrap(member.metadata()));
        subscriptions.put(member.memberId(), subscription);
    }
    
    // 3. 更新 Leader 自己的订阅（兼容模式）
    maybeUpdateJoinedSubscription(assignor, subscriptions);
    
    // 4. 执行分区分配
    Map<String, Assignment> assignment = assignor.assign(metadata.fetch(), 
        new GroupSubscription(subscriptions)).groupAssignment();
    
    // 5. 序列化分配结果
    Map<String, ByteBuffer> groupAssignment = new HashMap<>();
    for (Map.Entry<String, Assignment> memberAssignment : assignment.entrySet()) {
        ByteBuffer buffer = ConsumerProtocol.serializeAssignment(memberAssignment.getValue());
        groupAssignment.put(memberAssignment.getKey(), buffer);
    }
    
    return groupAssignment;
}
```

### 4.5 服务端完成重平衡

**源码位置**：`GroupCoordinator.scala` 第1183-1247行

```scala
/**
   * 完成 Join 阶段，进入 CompletingRebalance
   */
  def onCompleteJoin(group: GroupMetadata): Unit = {
    group.inLock {
      // 1. 移除未重新加入的动态成员
      val notYetRejoinedDynamicMembers = group.notYetRejoinedMembers.filterNot(_._2.isStaticMember)
      notYetRejoinedDynamicMembers.values foreach { failedMember =>
        removeHeartbeatForLeavingMember(group, failedMember)
        group.remove(failedMember.memberId)
      }
      
      // 2. 检查是否还有成员
      if (group.is(Dead)) {
        info(s"Group ${group.groupId} is dead, skipping rebalance stage")
      } 
      // 3. 选举新 Leader（如果没有 Leader 重新加入）
      else if (!group.maybeElectNewJoinedLeader() && group.allMembers.nonEmpty) {
        error(s"Group ${group.groupId} could not complete rebalance because no members rejoined")
        // 重新调度 DelayedJoin
        joinPurgatory.tryCompleteElseWatch(
          new DelayedJoin(this, group, group.rebalanceTimeoutMs),
          Seq(GroupKey(group.groupId)))
      } 
      // 4. 初始化新 Generation
      else {
        group.initNextGeneration()
        
        if (group.is(Empty)) {
          // 空组直接存储元数据
          groupManager.storeGroup(group, Map.empty, error => {...})
        } else {
          info(s"Stabilized group ${group.groupId} generation ${group.generationId}")
          
          // 5. 触发所有成员的 JoinGroup 回调
          for (member <- group.allMemberMetadata) {
            val joinResult = JoinGroupResult(
              members = if (group.isLeader(member.memberId)) {
                // Leader 收到所有成员元数据
                group.currentMemberMetadata
              } else {
                // Follower 收到空列表
                List.empty
              },
              memberId = member.memberId,
              generationId = group.generationId,
              protocolType = group.protocolType,
              protocolName = group.protocolName,
              leaderId = group.leaderOrNull,
              error = Errors.NONE)
            
            // 调用回调发送响应
            group.maybeInvokeJoinCallback(member, joinResult)
            completeAndScheduleNextHeartbeatExpiration(group, member)
          }
        }
      }
    }
  }
```

---

## 五、延迟重平衡机制 (Delayed Rebalance)

### 5.1 DelayedJoin 实现

**源码位置**：`core/src/main/scala/kafka/coordinator/group/DelayedJoin.scala`

```scala
/**
 * 延迟重平衡操作 - 当组准备重平衡时加入净化器
 * 当所有已知组成员都请求重新加入时，完成此操作以继续重平衡
 */
private[group] class DelayedJoin(coordinator: GroupCoordinator,
                                 group: GroupMetadata,
                                 rebalanceTimeout: Long) 
  extends DelayedOperation(rebalanceTimeout, Some(group.lock)) {

  /**
   * 尝试完成操作 - 检查是否所有成员都已加入
   */
  override def tryComplete(): Boolean = coordinator.tryCompleteJoin(group, forceComplete _)
  
  /**
   * 操作过期时的处理
   */
  override def onExpiration(): Unit = {
    coordinator.onExpireJoin()
    tryToCompleteDelayedAction()
  }
  
  /**
   * 操作完成时的处理
   */
  override def onComplete(): Unit = coordinator.onCompleteJoin(group)
}

/**
 * 初始延迟加入 - 用于组从 Empty 转换到 PreparingRebalance 时
 * 允许新成员在重平衡开始前快速加入
 */
private[group] class InitialDelayedJoin(coordinator: GroupCoordinator,
                                        purgatory: DelayedOperationPurgatory[DelayedJoin],
                                        group: GroupMetadata,
                                        configuredRebalanceDelay: Int,
                                        delayMs: Int,
                                        remainingMs: Int) 
  extends DelayedJoin(coordinator, group, delayMs) {

  override def tryComplete(): Boolean = false  // 初始阶段不检查完成

  override def onComplete(): Unit = {
    group.inLock {
      // 如果有新成员加入且还有剩余时间，继续延迟
      if (group.newMemberAdded && remainingMs != 0) {
        group.newMemberAdded = false
        val delay = min(configuredRebalanceDelay, remainingMs)
        val remaining = max(remainingMs - delayMs, 0)
        
        // 创建新的延迟操作继续等待
        purgatory.tryCompleteElseWatch(new InitialDelayedJoin(
          coordinator, purgatory, group, configuredRebalanceDelay, delay, remaining
        ), Seq(GroupKey(group.groupId)))
      } else {
        // 时间用完或没有新成员，完成操作
        super.onComplete()
      }
    }
  }
}
```

### 5.2 检查是否可以完成 Join

**源码位置**：`GroupCoordinator.scala` 第1171-1177行

```scala
def tryCompleteJoin(group: GroupMetadata, forceComplete: () => Boolean) = {
  group.inLock {
    // 检查所有成员是否都已加入
    if (group.hasAllMembersJoined)
      forceComplete()  // 强制完成延迟操作
    else 
      false
  }
}
```

---

## 六、心跳机制与会话管理

### 6.1 心跳发送

消费者通过后台心跳线程维持会话：

```java
/**
 * 心跳线程（AbstractCoordinator 内部类）
 */
private class HeartbeatThread extends KafkaThread {
    private boolean enabled = false;
    
    @Override
    public void run() {
        while (true) {
            synchronized (AbstractCoordinator.this) {
                if (!enabled) break;
                
                // 检查是否需要发送心跳
                if (heartbeat.timeToNextHeartbeat(time.milliseconds()) > 0) {
                    AbstractCoordinator.this.wait(heartbeat.timeToNextHeartbeat(time.milliseconds()));
                    continue;
                }
            }
            
            // 发送心跳请求
            sendHeartbeatRequest().poll(heartbeatIntervalMs);
        }
    }
}
```

### 6.2 服务端心跳处理

**源码位置**：`GroupCoordinator.scala` 第607-666行

```scala
def handleHeartbeat(groupId: String,
                    memberId: String,
                    groupInstanceId: Option[String],
                    generationId: Int,
                    responseCallback: Errors => Unit): Unit = {
  groupManager.getGroup(groupId) match {
    case Some(group) => group.inLock {
      group.currentState match {
        case PreparingRebalance =>
          // 重平衡中，返回 REBALANCE_IN_PROGRESS
          responseCallback(Errors.REBALANCE_IN_PROGRESS)
          
        case CompletingRebalance =>
          // 完成重平衡中，接受心跳
          completeAndScheduleNextHeartbeatExpiration(group, member)
          responseCallback(Errors.NONE)
          
        case Stable =>
          // 正常状态
          completeAndScheduleNextHeartbeatExpiration(group, member)
          responseCallback(Errors.NONE)
          
        case Empty | Dead =>
          responseCallback(Errors.UNKNOWN_MEMBER_ID)
      }
    }
  }
}
```

### 6.3 延迟心跳与成员失效

**源码位置**：`core/src/main/scala/kafka/coordinator/group/DelayedHeartbeat.scala`

```scala
/**
 * 延迟心跳 - 用于检测成员会话超时
 */
private[group] class DelayedHeartbeat(coordinator: GroupCoordinator,
                                      group: GroupMetadata,
                                      memberId: String,
                                      isPending: Boolean,
                                      timeoutMs: Long)
  extends DelayedOperation(timeoutMs, Some(group.lock)) {

  override def tryComplete(): Boolean = {
    // 检查心跳是否已满足（成员已发送心跳）
    if (group.get(memberId).heartbeatSatisfied) {
      forceComplete()
    } else {
      false
    }
  }

  override def onExpiration(): Unit = {
    // 心跳超时，标记成员为失效
    coordinator.onMemberFailure(group, group.get(memberId))
  }

  override def onComplete(): Unit = {
    // 心跳完成，重置计时
  }
}
```

---

## 七、消费者组交互时序图

### 7.1 新成员加入完整时序

```
┌──────────┐     ┌──────────┐     ┌──────────────────┐     ┌───────────────┐
│Consumer-1│     │Consumer-2│     │GroupCoordinator  │     │DelayedPurgatory│
│(Existing)│     │(New)     │     │                  │     │               │
└────┬─────┘     └────┬─────┘     └────────┬─────────┘     └───────┬───────┘
     │                │                    │                       │
     │                │ 1. FindCoordinator │                       │
     │                │───────────────────▶│                       │
     │                │◀───────────────────│                       │
     │                │                    │                       │
     │                │ 2. JoinGroup       │                       │
     │                │ (unknown member)   │                       │
     │                │───────────────────▶│                       │
     │                │                    │                       │
     │                │◀───────────────────│                       │
     │                │ MEMBER_ID_REQUIRED │                       │
     │                │                    │                       │
     │                │ 3. JoinGroup       │                       │
     │                │ (with memberId)    │                       │
     │                │───────────────────▶│                       │
     │                │                    │                       │
     │◀───────────────│◀───────────────────│                       │
     │   Heartbeat    │   REBALANCE_IN_PROGRESS (广播)             │
     │ (REBALANCE_IN_PROGRESS)              │                       │
     │                │                    │                       │
     │ 4. Rejoin      │                    │                       │
     │────────────────────────────────────▶│                       │
     │                │ 5. Rejoin          │                       │
     │                │───────────────────▶│                       │
     │                │                    │                       │
     │                │                    │ 6. Create DelayedJoin │
     │                │                    │──────────────────────▶│
     │                │                    │                       │
     │                │                    │ 7. Check complete     │
     │                │                    │◀──────────────────────│
     │                │                    │                       │
     │                │                    │ 8. All members joined │
     │                │                    │                       │
     │                │                    │ 9. Complete DelayedJoin│
     │                │                    │ (onCompleteJoin)      │
     │                │                    │                       │
     │◀───────────────│◀───────────────────│                       │
     │ JoinResponse   │ JoinResponse       │                       │
     │ (isLeader=true)│ (isLeader=false)   │                       │
     │                │                    │                       │
     │ 10. performAssignment()              │                       │
     │    (计算分区分配)                     │                       │
     │                │                    │                       │
     │ 11. SyncGroup  │                    │                       │
     │ (assignments)  │                    │                       │
     │────────────────────────────────────▶│                       │
     │                │ 12. SyncGroup      │                       │
     │                │───────────────────▶│                       │
     │                │                    │                       │
     │                │                    │ 13. Persist to        │
     │                │                    │ __consumer_offsets    │
     │                │                    │                       │
     │◀───────────────│◀───────────────────│                       │
     │ SyncResponse   │ SyncResponse       │                       │
     │ (assignment)   │ (assignment)       │                       │
     │                │                    │                       │
     ▼                ▼                    ▼                       ▼
   开始消费         开始消费
```

---

## 八、关键配置参数

### 8.1 消费者端配置

| 参数名 | 默认值 | 说明 |
|--------|--------|------|
| `session.timeout.ms` | 45000 | 会话超时时间，心跳超时则视为失效 |
| `heartbeat.interval.ms` | 3000 | 心跳发送间隔 |
| `max.poll.interval.ms` | 300000 | 两次 poll 的最大间隔 |
| `rebalance.timeout.ms` | 60000 | 重平衡超时时间 |
| `group.instance.id` | null | 静态成员标识 |

### 8.2 服务端配置

| 参数名 | 默认值 | 说明 |
|--------|--------|------|
| `group.initial.rebalance.delay.ms` | 3000 | 初始重平衡延迟，等待新成员加入 |
| `group.min.session.timeout.ms` | 6000 | 最小会话超时时间 |
| `group.max.session.timeout.ms` | 1800000 | 最大会话超时时间 |
| `group.max.size` | 2147483647 | 最大组成员数 |

---

## 九、重平衡优化策略

### 9.1 静态成员 (Static Membership)

静态成员使用 `group.instance.id` 标识，重启后保持相同的 `member.id`，避免不必要的重平衡。

```java
// Consumer 配置
props.put("group.instance.id", "consumer-instance-1");  // 静态成员标识
```

**服务端处理**：`GroupCoordinator.scala` 第225-226行

```scala
if (group.hasStaticMember(groupInstanceId)) {
  // 静态成员重新加入，更新成员信息但不触发重平衡
  updateStaticMemberAndRebalance(group, newMemberId, groupInstanceId, protocols, responseCallback)
}
```

### 9.2 增量再平衡 (Incremental Rebalance)

使用 CooperativeStickyAssignor 实现增量再平衡，减少不必要的分区迁移：

```java
// Consumer 配置
props.put("partition.assignment.strategy", 
    "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
```

### 9.3 分区分配策略对比

| 策略 | 特点 | 适用场景 |
|------|------|----------|
| **Range** | 按 Topic 范围分配，可能不均衡 | 分区数少的场景 |
| **RoundRobin** | 轮询分配，均衡 | 分区数多的场景 |
| **Sticky** | 尽量保持现有分配，减少迁移 | 需要稳定性的场景 |
| **CooperativeSticky** | 增量再平衡，两阶段协议 | 大规模消费者组 |

---

## 十、常见问题与解决方案

### 10.1 重平衡风暴 (Rebalance Storm)

**问题原因**：
- 消费者处理消息时间过长，超过 `max.poll.interval.ms`
- 网络抖动导致心跳超时
- 频繁成员加入/离开

**解决方案**：
1. 增加 `max.poll.interval.ms`
2. 减少 `max.poll.records`
3. 使用静态成员 `group.instance.id`
4. 优化消费者处理逻辑

### 10.2 分区分配不均

**问题原因**：
- Range 策略在不同 Topic 间分配不均
- 消费者数量变化导致分配变化

**解决方案**：
1. 使用 StickyAssignor
2. 确保 Topic 分区数与消费者数成比例

### 10.3 重平衡卡住

**问题原因**：
- Leader 消费者挂掉
- 某些成员无法重新加入

**解决方案**：
1. 检查消费者日志
2. 调整 `rebalance.timeout.ms`
3. 重启消费者组

---

## 十一、总结

Kafka 消费者组的重平衡机制是一个复杂但精密的分布式协调过程：

1. **触发条件多样**：新成员加入、成员离开、心跳超时、元数据变更等
2. **状态机驱动**：通过 Empty → PreparingRebalance → CompletingRebalance → Stable 的状态转换保证一致性
3. **延迟操作优化**：使用 DelayedJoin 和 DelayedHeartbeat 减少不必要的重平衡
4. **Leader 协调**：Leader 消费者执行分区分配，减少服务端负担
5. **持久化保证**：分配结果写入 `__consumer_offsets` 主题，保证故障恢复

理解这些机制有助于优化消费者组性能，避免重平衡带来的消费延迟。
