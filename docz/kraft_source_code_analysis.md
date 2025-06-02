# KRaft 源码详细解析

## 概述

KRaft (Kafka Raft) 是 Apache Kafka 自主实现的共识协议，基于 Raft 算法但针对 Kafka 的特定需求进行了定制。本文档将深入分析 KRaft 的核心源码实现。

## 1. 整体架构

### 1.1 核心类层次结构

```
KafkaRaftClient (raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java)
├── QuorumState (状态管理)
├── ReplicatedLog (日志管理)
├── BatchAccumulator (批处理累加器)
└── NetworkChannel (网络通信)

RaftManager (core/src/main/scala/kafka/raft/RaftManager.scala)
├── KafkaRaftClient (包装)
├── KafkaRaftClientDriver (驱动器)
└── NetworkChannel (网络层)

KafkaRaftServer (core/src/main/scala/kafka/server/KafkaRaftServer.scala)
├── ControllerServer (Controller角色)
├── BrokerServer (Broker角色)
└── SharedServer (共享组件)
```

## 2. 状态机实现

### 2.1 QuorumState - 仲裁状态管理

**位置**: `raft/src/main/java/org/apache/kafka/raft/QuorumState.java`

```java
public class QuorumState {
    // 当前状态，可能是以下之一：
    // - UnattachedState: 未连接状态
    // - FollowerState: 跟随者状态  
    // - ProspectiveState: 预候选状态
    // - CandidateState: 候选者状态
    // - LeaderState: 领导者状态
    private EpochState state;
    
    // 状态转换方法
    public void transitionToCandidate() {
        checkValidTransitionToCandidate();
        
        int newEpoch = epoch() + 1;  // 增加epoch
        int electionTimeoutMs = randomElectionTimeoutMs();
        
        // 持久化状态转换
        durableTransitionTo(new CandidateState(
            time,
            localIdOrThrow(),
            localDirectoryId,
            newEpoch,
            partitionState.lastVoterSet(),
            state.highWatermark(),
            electionTimeoutMs,
            logContext
        ));
    }
    
    // 持久化状态转换
    private void durableTransitionTo(EpochState newState) {
        log.info("Attempting durable transition to {} from {}", newState, state);
        // 写入选举状态到磁盘
        store.writeElectionState(newState.election(), partitionState.lastKraftVersion());
        memoryTransitionTo(newState);
    }
}
```

**关键特性**:
- **状态持久化**: 每次状态转换都会写入磁盘，确保重启后状态一致
- **随机选举超时**: 避免多个节点同时发起选举
- **状态验证**: 确保状态转换的合法性

### 2.2 状态类型详解

#### 2.2.1 CandidateState - 候选者状态

**位置**: `raft/src/main/java/org/apache/kafka/raft/CandidateState.java`

```java
public class CandidateState extends NomineeState {
    @Override
    public boolean recordGrantedVote(int remoteNodeId) {
        if (epochElection().isRejectedVoter(remoteNodeId)) {
            throw new IllegalArgumentException("Attempt to grant vote from node " + remoteNodeId +
                " which previously rejected our request");
        }
        return epochElection().recordVote(remoteNodeId, true);
    }
    
    @Override
    public boolean recordRejectedVote(int remoteNodeId) {
        if (epochElection().isGrantedVoter(remoteNodeId)) {
            throw new IllegalArgumentException("Attempt to reject vote from node " + remoteNodeId +
                " which previously granted our request");
        }
        return epochElection().recordVote(remoteNodeId, false);
    }
}
```

#### 2.2.2 LeaderState - 领导者状态

**位置**: `raft/src/main/java/org/apache/kafka/raft/LeaderState.java`

```java
public class LeaderState<T> implements EpochState {
    // 跟踪所有副本的状态
    private final Map<ReplicaKey, ReplicaState> voterStates = new HashMap<>();
    private final Map<ReplicaKey, ReplicaState> observerStates = new HashMap<>();
    
    // 更新高水位标记
    public boolean updateReplicaState(
        ReplicaKey replicaKey,
        long currentTimeMs,
        LogOffsetMetadata fetchOffsetMetadata
    ) {
        ReplicaState state = getOrCreateReplicaState(replicaKey);
        
        // 更新副本状态
        state.updateFollowerState(
            currentTimeMs,
            fetchOffsetMetadata,
            leaderEndOffsetOpt
        );
        
        // 检查是否需要更新高水位
        return isVoter(state.replicaKey) && maybeUpdateHighWatermark();
    }
    
    // 计算新的高水位
    private boolean maybeUpdateHighWatermark() {
        // 获取所有投票者的复制进度
        List<ReplicaState> followersByDescendingFetchOffset = 
            followersByDescendingFetchOffset();
            
        // 计算多数派已复制的偏移量
        int indexOfHw = voterStates.size() / 2;
        Optional<LogOffsetMetadata> highWatermarkUpdateOpt = 
            followersByDescendingFetchOffset.get(indexOfHw).endOffset;
            
        if (highWatermarkUpdateOpt.isPresent()) {
            LogOffsetMetadata highWatermarkUpdateMetadata = highWatermarkUpdateOpt.get();
            long highWatermarkUpdateOffset = highWatermarkUpdateMetadata.offset();
            
            if (highWatermark.isEmpty() || 
                highWatermarkUpdateOffset > highWatermark.get().offset()) {
                // 更新高水位
                highWatermark = highWatermarkUpdateOpt;
                return true;
            }
        }
        return false;
    }
}
```

## 3. 选举机制实现

### 3.1 选举触发

**位置**: `raft/src/main/java/org/apache/kafka/raft/KafkaRaftClient.java`

```java
public class KafkaRaftClient<T> implements RaftClient<T> {
    
    // 候选者状态的轮询逻辑
    private long pollCandidate(long currentTimeMs) {
        CandidateState state = quorum.candidateStateOrThrow();
        
        if (shutdown != null) {
            // 关闭过程中继续选举直到满足条件
            long minRequestBackoffMs = maybeSendVoteRequests(state, currentTimeMs);
            return Math.min(shutdown.remainingTimeMs(), minRequestBackoffMs);
        } else if (state.hasElectionTimeoutExpired(currentTimeMs)) {
            // 选举超时，转为预候选状态
            logger.info("Election was not granted, transitioning to prospective");
            transitionToProspective(currentTimeMs);
            return 0L;
        } else {
            // 继续发送投票请求
            long minVoteRequestBackoffMs = maybeSendVoteRequests(state, currentTimeMs);
            return Math.min(minVoteRequestBackoffMs, state.remainingElectionTimeMs(currentTimeMs));
        }
    }
    
    // 发送投票请求
    private long maybeSendVoteRequests(
        NomineeState state,
        long currentTimeMs
    ) {
        if (!state.epochElection().isVoteRejected()) {
            VoterSet voters = partitionState.lastVoterSet();
            boolean preVote = quorum.isProspective();  // 是否为预投票
            
            return maybeSendRequest(
                currentTimeMs,
                state.epochElection().unrecordedVoters(),  // 未记录投票的节点
                voterId -> voters.voterNode(voterId, channel.listenerName()),
                voterId -> buildVoteRequest(voterId, preVote)  // 构建投票请求
            );
        }
        return Long.MAX_VALUE;
    }
}
```

### 3.2 投票请求处理

```java
// 处理投票请求
private VoteResponseData handleVoteRequest(RaftRequest.Inbound requestMetadata) {
    VoteRequestData request = (VoteRequestData) requestMetadata.data();
    
    // 验证集群ID
    if (!hasValidClusterId(request.clusterId())) {
        return new VoteResponseData().setErrorCode(Errors.INCONSISTENT_CLUSTER_ID.code());
    }
    
    VoteRequestData.PartitionData partitionRequest =
        request.topics().get(0).partitions().get(0);
    
    int replicaId = partitionRequest.replicaId();
    int replicaEpoch = partitionRequest.replicaEpoch();
    boolean preVote = partitionRequest.preVote();
    
    // 检查日志是否足够新
    OffsetAndEpoch lastEpochEndOffset = log.endOffsetForEpoch(partitionRequest.lastOffsetEpoch());
    boolean isLogUpToDate = isLogUpToDate(
        partitionRequest.lastOffsetEpoch(),
        partitionRequest.lastOffset(),
        lastEpochEndOffset
    );
    
    // 检查是否可以投票
    ReplicaKey candidateKey = ReplicaKey.of(replicaId, partitionRequest.replicaDirectoryId());
    boolean voteGranted = quorum.canGrantVote(candidateKey, isLogUpToDate, preVote);
    
    if (voteGranted && !preVote) {
        // 非预投票且同意投票，转换状态
        transitionToVoted(replicaEpoch, candidateKey);
    }
    
    logger.info("Received vote request {} with epoch {}, returning response {}",
        request, quorum.epoch(), voteGranted);
        
    return buildVoteResponse(voteGranted, Optional.empty());
}
```

### 3.3 投票响应处理

```java
// 处理投票响应
private boolean handleVoteResponse(
    RaftResponse.Inbound responseMetadata,
    long currentTimeMs
) {
    VoteResponseData response = (VoteResponseData) responseMetadata.data();
    VoteResponseData.PartitionData partitionResponse = 
        response.topics().get(0).partitions().get(0);
    
    boolean voteGranted = partitionResponse.voteGranted();
    int remoteNodeId = responseMetadata.source().id();
    
    if (quorum.isProspective()) {
        ProspectiveState state = quorum.prospectiveStateOrThrow();
        if (voteGranted) {
            if (state.recordGrantedVote(remoteNodeId)) {
                logger.info("Received vote from {} in epoch {}", remoteNodeId, quorum.epoch());
            }
            // 检查是否获得足够票数转为候选者
            if (state.epochElection().isVoteGranted()) {
                transitionToCandidate(currentTimeMs);
                return true;
            }
        } else {
            state.recordRejectedVote(remoteNodeId);
        }
    } else if (quorum.isCandidate()) {
        CandidateState state = quorum.candidateStateOrThrow();
        if (voteGranted) {
            if (state.recordGrantedVote(remoteNodeId)) {
                logger.info("Received vote from {} in epoch {}", remoteNodeId, quorum.epoch());
            }
            // 检查是否获得足够票数成为领导者
            if (state.epochElection().isVoteGranted()) {
                becomeLeader(state, currentTimeMs);
                return true;
            }
        } else {
            state.recordRejectedVote(remoteNodeId);
        }
    }
    
    return false;
}
```

## 4. 日志复制机制

### 4.1 Fetch请求处理

```java
// 处理Fetch请求（Leader端）
private CompletableFuture<FetchResponseData> handleFetchRequest(
    RaftRequest.Inbound request,
    long currentTimeMs
) {
    FetchRequestData fetchRequest = (FetchRequestData) request.data();
    
    if (quorum.isLeader()) {
        return handleLeaderFetchRequest(request, currentTimeMs);
    } else {
        // 非Leader节点重定向到Leader
        return completedFuture(buildEmptyFetchResponse(
            request.listenerName(),
            request.apiVersion(),
            Errors.NOT_LEADER_OR_FOLLOWER,
            Optional.of(quorum.leaderIdOrSentinel())
        ));
    }
}

// Leader处理Fetch请求
private CompletableFuture<FetchResponseData> handleLeaderFetchRequest(
    RaftRequest.Inbound requestMetadata,
    long currentTimeMs
) {
    FetchRequestData request = (FetchRequestData) requestMetadata.data();
    LeaderState<T> state = quorum.leaderStateOrThrow();
    
    FetchRequestData.FetchPartition fetchPartition = 
        request.topics().get(0).partitions().get(0);
    long fetchOffset = fetchPartition.fetchOffset();
    int lastFetchedEpoch = fetchPartition.lastFetchedEpoch();
    
    // 验证Fetch偏移量和epoch
    ValidOffsetAndEpoch validOffsetAndEpoch = validateFetchOffsetAndEpoch(
        fetchOffset, lastFetchedEpoch);
    
    if (validOffsetAndEpoch.kind() == ValidOffsetAndEpoch.Kind.VALID) {
        // 读取日志数据
        LogFetchInfo info = log.read(fetchOffset, Isolation.UNCOMMITTED);
        
        // 更新副本状态
        ReplicaKey replicaKey = ReplicaKey.of(
            request.replicaState().replicaId(),
            fetchPartition.replicaDirectoryId()
        );
        
        if (state.updateReplicaState(replicaKey, currentTimeMs, info.startOffsetMetadata)) {
            // 高水位更新，通知监听器
            onUpdateLeaderHighWatermark(state, currentTimeMs);
        }
        
        return completedFuture(buildFetchResponse(
            requestMetadata.listenerName(),
            requestMetadata.apiVersion(),
            Errors.NONE,
            info.records,
            validOffsetAndEpoch,
            state.highWatermark()
        ));
    } else {
        // 偏移量无效，返回错误
        return completedFuture(buildEmptyFetchResponse(
            requestMetadata.listenerName(),
            requestMetadata.apiVersion(),
            Errors.OFFSET_OUT_OF_RANGE,
            Optional.empty()
        ));
    }
}
```

### 4.2 Follower的日志同步

```java
// Follower发送Fetch请求
private FetchRequestData buildFetchRequest() {
    FetchRequestData request = RaftUtil.singletonFetchRequest(
        log.topicPartition(),
        log.topicId(),
        fetchPartition -> fetchPartition
            .setCurrentLeaderEpoch(quorum.epoch())
            .setLastFetchedEpoch(log.lastFetchedEpoch())
            .setFetchOffset(log.endOffset().offset())  // 从日志末尾开始拉取
            .setReplicaDirectoryId(quorum.localDirectoryId())
    );
    
    return request
        .setMaxBytes(MAX_FETCH_SIZE_BYTES)
        .setMaxWaitMs(fetchMaxWaitMs)
        .setClusterId(clusterId)
        .setReplicaState(new FetchRequestData.ReplicaState()
            .setReplicaId(quorum.localIdOrSentinel()));
}

// 处理Fetch响应
private boolean handleFetchResponse(
    RaftResponse.Inbound responseMetadata,
    long currentTimeMs
) {
    FetchResponseData response = (FetchResponseData) responseMetadata.data();
    FetchResponseData.PartitionData partitionResponse = 
        response.topics().get(0).partitions().get(0);
    
    if (partitionResponse.errorCode() == Errors.NONE.code()) {
        // 成功响应，追加日志
        Records records = (Records) partitionResponse.records();
        if (records != null && records.sizeInBytes() > 0) {
            LogAppendInfo appendInfo = log.appendAsFollower(records);
            
            // 更新高水位
            if (partitionResponse.highWatermark() >= 0) {
                updateFollowerHighWatermark(
                    quorum.followerStateOrThrow(),
                    OptionalLong.of(partitionResponse.highWatermark())
                );
            }
            
            // 通知状态机有新的已提交记录
            maybeFireHandleCommit(appendInfo.lastOffset + 1);
        }
        return true;
    } else {
        // 处理错误响应
        return handleFetchResponseError(partitionResponse, currentTimeMs);
    }
}
```

## 5. 快照机制

### 5.1 快照创建

**位置**: `raft/src/main/java/org/apache/kafka/snapshot/RecordsSnapshotWriter.java`

```java
public final class RecordsSnapshotWriter<T> implements SnapshotWriter<T> {
    private final RawSnapshotWriter snapshot;
    private final BatchAccumulator<T> accumulator;
    
    @Override
    public void append(List<T> records) {
        if (snapshot.isFrozen()) {
            throw new IllegalStateException("Snapshot is already frozen");
        }
        
        // 追加记录到累加器
        accumulator.append(snapshot.snapshotId().epoch(), records, false);
        
        // 如果需要，刷新批次到快照
        if (accumulator.needsDrain(time.milliseconds())) {
            appendBatches(accumulator.drain());
        }
    }
    
    @Override
    public long freeze() {
        // 完成快照写入
        finalizeSnapshotWithFooter();
        appendBatches(accumulator.drain());
        snapshot.freeze();
        accumulator.close();
        return snapshot.sizeInBytes();
    }
    
    private void appendBatches(List<MutableRecordBatch> batches) {
        try {
            for (MutableRecordBatch batch : batches) {
                snapshot.append(batch.buffer().duplicate());
            }
        } finally {
            batches.forEach(MutableRecordBatch::close);
        }
    }
}
```

### 5.2 快照传输

```java
// 处理快照拉取请求
private FetchSnapshotResponseData handleFetchSnapshotRequest(
    RaftRequest.Inbound requestMetadata,
    long currentTimeMs
) {
    FetchSnapshotRequestData request = (FetchSnapshotRequestData) requestMetadata.data();
    FetchSnapshotRequestData.PartitionSnapshot partitionRequest = 
        request.topics().get(0).partitions().get(0);
    
    OffsetAndEpoch snapshotId = new OffsetAndEpoch(
        partitionRequest.snapshotId().endOffset(),
        partitionRequest.snapshotId().epoch()
    );
    
    // 读取快照
    Optional<RawSnapshotReader> snapshotOpt = log.readSnapshot(snapshotId);
    if (snapshotOpt.isEmpty()) {
        return RaftUtil.singletonFetchSnapshotResponse(
            requestMetadata.listenerName(),
            requestMetadata.apiVersion(),
            log.topicPartition(),
            quorum.leaderIdOrSentinel(),
            quorum.leaderEndpoints(),
            responsePartitionSnapshot -> responsePartitionSnapshot
                .setErrorCode(Errors.SNAPSHOT_NOT_FOUND.code())
        );
    }
    
    RawSnapshotReader snapshot = snapshotOpt.get();
    long position = partitionRequest.position();
    
    // 读取快照数据
    UnalignedRecords records = snapshot.readAt(position, MAX_FETCH_SIZE_BYTES);
    
    return RaftUtil.singletonFetchSnapshotResponse(
        requestMetadata.listenerName(),
        requestMetadata.apiVersion(),
        log.topicPartition(),
        quorum.leaderIdOrSentinel(),
        quorum.leaderEndpoints(),
        responsePartitionSnapshot -> responsePartitionSnapshot
            .setSnapshotId(partitionRequest.snapshotId())
            .setSize(snapshot.sizeInBytes())
            .setPosition(position)
            .setUnalignedRecords(records)
    );
}
```

## 6. 网络通信层

### 6.1 RaftManager集成

**位置**: `core/src/main/scala/kafka/raft/RaftManager.scala`

```scala
class KafkaRaftManager[T](
  clusterId: String,
  config: KafkaConfig,
  metadataLogDirUuid: Uuid,
  serde: RecordSerde[T],
  // ...
) extends RaftManager[T] with Logging {
  
  // 构建Raft客户端
  private def buildRaftClient(): KafkaRaftClient[T] = {
    new KafkaRaftClient(
      OptionalInt.of(config.nodeId),
      metadataLogDirUuid,
      recordSerde,
      netChannel,
      replicatedLog,
      time,
      expirationService,
      logContext,
      // Controllers应该总是刷新日志，因为它们可能成为投票者
      config.processRoles.contains(ProcessRole.ControllerRole),
      clusterId,
      bootstrapServers,
      localListeners,
      Feature.KRAFT_VERSION.supportedVersionRange(),
      raftConfig
    )
  }
  
  def startup(): Unit = {
    // 初始化客户端
    client.initialize(
      controllerQuorumVotersFuture.get(),
      new FileQuorumStateStore(new File(dataDir, FileQuorumStateStore.DEFAULT_FILE_NAME)),
      metrics,
      externalKRaftMetrics
    )
    
    // 启动网络通道和客户端驱动器
    netChannel.start()
    clientDriver.start()
  }
  
  // 处理请求
  override def handleRequest(
    context: RequestContext,
    header: RequestHeader,
    request: ApiMessage,
    createdTimeMs: Long
  ): CompletableFuture[ApiMessage] = {
    clientDriver.handleRequest(context, header, request, createdTimeMs)
  }
}
```

### 6.2 KafkaRaftServer集成

**位置**: `core/src/main/scala/kafka/server/KafkaRaftServer.scala`

```scala
class KafkaRaftServer(
  config: KafkaConfig,
  time: Time,
) extends Server with Logging {
  
  // 根据process.roles配置创建组件
  private val controller: Option[ControllerServer] = 
    if (config.processRoles.contains(ProcessRole.ControllerRole)) {
      Some(new ControllerServer(sharedServer, configSchema, bootstrapMetadata))
    } else {
      None
    }
  
  private val broker: Option[BrokerServer] = 
    if (config.processRoles.contains(ProcessRole.BrokerRole)) {
      Some(new BrokerServer(sharedServer))
    } else {
      None
    }
  
  override def startup(): Unit = {
    // Controller必须在Broker之前启动
    controller.foreach(_.startup())
    broker.foreach(_.startup())
    
    AppInfoParser.registerAppInfo(Server.MetricsPrefix, config.brokerId.toString, metrics, time.milliseconds())
    info(KafkaBroker.STARTED_MESSAGE)
  }
}
```

## 7. 关键特性总结

### 7.1 与标准Raft的差异

1. **日志复制方式**: 使用Kafka的Fetch API而不是AppendEntries
2. **日志协调**: 使用Kafka的日志协调协议进行截断
3. **角色区分**: 区分投票者(Voters)和观察者(Observers)
4. **预投票机制**: 支持PreVote避免不必要的选举

### 7.2 性能优化

1. **批量处理**: 使用BatchAccumulator进行批量日志追加
2. **延迟刷新**: 可配置的日志刷新策略
3. **快照压缩**: 定期创建快照减少日志大小
4. **网络优化**: 复用Kafka的网络层和序列化机制

### 7.3 可靠性保证

1. **状态持久化**: 选举状态写入磁盘
2. **日志完整性**: CRC校验和epoch验证
3. **故障恢复**: 支持从快照和日志恢复
4. **一致性保证**: 严格的多数派提交规则

KRaft的实现充分利用了Kafka现有的基础设施，同时针对元数据管理的特定需求进行了优化，是一个高质量的分布式共识协议实现。
