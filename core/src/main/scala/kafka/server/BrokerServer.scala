/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import kafka.coordinator.group.{CoordinatorLoaderImpl, CoordinatorPartitionWriter}
import kafka.coordinator.transaction.TransactionCoordinator
import kafka.log.LogManager
import kafka.network.SocketServer
import kafka.raft.KafkaRaftManager
import kafka.server.metadata._
import kafka.server.share.{ShareCoordinatorMetadataCacheHelperImpl, SharePartitionManager}
import kafka.utils.CoreUtils
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.internals.Plugin
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.utils.{LogContext, Time, Utils}
import org.apache.kafka.common.{ClusterResource, TopicPartition, Uuid}
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord
import org.apache.kafka.coordinator.group.metrics.{GroupCoordinatorMetrics, GroupCoordinatorRuntimeMetrics}
import org.apache.kafka.coordinator.group.{GroupConfigManager, GroupCoordinator, GroupCoordinatorRecordSerde, GroupCoordinatorService}
import org.apache.kafka.coordinator.share.metrics.{ShareCoordinatorMetrics, ShareCoordinatorRuntimeMetrics}
import org.apache.kafka.coordinator.share.{ShareCoordinator, ShareCoordinatorRecordSerde, ShareCoordinatorService}
import org.apache.kafka.coordinator.transaction.ProducerIdManager
import org.apache.kafka.image.publisher.{BrokerRegistrationTracker, MetadataPublisher}
import org.apache.kafka.metadata.{BrokerState, ListenerInfo}
import org.apache.kafka.metadata.publisher.AclPublisher
import org.apache.kafka.security.CredentialProvider
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.common.{ApiMessageAndVersion, DirectoryEventHandler, NodeToControllerChannelManager, TopicIdPartition}
import org.apache.kafka.server.config.{ConfigType, DelegationTokenManagerConfigs}
import org.apache.kafka.server.log.remote.storage.{RemoteLogManager, RemoteLogManagerConfig}
import org.apache.kafka.server.metrics.{ClientMetricsReceiverPlugin, KafkaYammerMetrics}
import org.apache.kafka.server.network.{EndpointReadyFutures, KafkaAuthorizerServerInfo}
import org.apache.kafka.server.share.persister.{DefaultStatePersister, NoOpStatePersister, Persister, PersisterStateManager}
import org.apache.kafka.server.share.session.ShareSessionCache
import org.apache.kafka.server.util.timer.{SystemTimer, SystemTimerReaper}
import org.apache.kafka.server.util.{Deadline, FutureUtils, KafkaScheduler}
import org.apache.kafka.server.{AssignmentsManager, BrokerFeatures, ClientMetricsManager, DefaultApiVersionManager, DelayedActionQueue, DelegationTokenManager, ProcessRole}
import org.apache.kafka.storage.internals.log.LogDirFailureChannel
import org.apache.kafka.storage.log.metrics.BrokerTopicStats

import java.time.Duration
import java.util
import java.util.Optional
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.{Condition, ReentrantLock}
import java.util.concurrent.{CompletableFuture, ExecutionException, TimeUnit, TimeoutException}
import scala.collection.Map
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.RichOption


/**
 * A Kafka broker that runs in KRaft (Kafka Raft) mode.
 */
class BrokerServer(
  val sharedServer: SharedServer
) extends KafkaBroker {
  val config: KafkaConfig = sharedServer.brokerConfig
  val time: Time = sharedServer.time
  def metrics: Metrics = sharedServer.metrics

  // Get raftManager from SharedServer. It will be initialized during startup.
  def raftManager: KafkaRaftManager[ApiMessageAndVersion] = sharedServer.raftManager

  override def brokerState: BrokerState = Option(lifecycleManager).
    flatMap(m => Some(m.state)).getOrElse(BrokerState.NOT_RUNNING)

  import kafka.server.Server._

  private val logContext: LogContext = new LogContext(s"[BrokerServer id=${config.nodeId}] ")

  this.logIdent = logContext.logPrefix

  @volatile var lifecycleManager: BrokerLifecycleManager = _

  private var assignmentsManager: AssignmentsManager = _

  private val isShuttingDown = new AtomicBoolean(false)

  val lock: ReentrantLock = new ReentrantLock()
  val awaitShutdownCond: Condition = lock.newCondition()
  var status: ProcessStatus = SHUTDOWN

  @volatile var dataPlaneRequestProcessor: KafkaApis = _

  var authorizerPlugin: Option[Plugin[Authorizer]] = None
  @volatile var socketServer: SocketServer = _
  var dataPlaneRequestHandlerPool: KafkaRequestHandlerPool = _

  var logDirFailureChannel: LogDirFailureChannel = _
  var logManager: LogManager = _
  var remoteLogManagerOpt: Option[RemoteLogManager] = None

  var tokenManager: DelegationTokenManager = _

  var dynamicConfigHandlers: Map[ConfigType, ConfigHandler] = _

  @volatile private[this] var _replicaManager: ReplicaManager = _

  var credentialProvider: CredentialProvider = _
  var tokenCache: DelegationTokenCache = _

  @volatile var groupCoordinator: GroupCoordinator = _

  var groupConfigManager: GroupConfigManager = _

  var transactionCoordinator: TransactionCoordinator = _

  var shareCoordinator: ShareCoordinator = _

  var clientToControllerChannelManager: NodeToControllerChannelManager = _

  var forwardingManager: ForwardingManager = _

  var alterPartitionManager: AlterPartitionManager = _

  var autoTopicCreationManager: AutoTopicCreationManager = _

  var kafkaScheduler: KafkaScheduler = _

  @volatile var metadataCache: KRaftMetadataCache = _

  var quotaManagers: QuotaFactory.QuotaManagers = _

  var clientQuotaMetadataManager: ClientQuotaMetadataManager = _

  @volatile var brokerTopicStats: BrokerTopicStats = _

  val clusterId: String = sharedServer.metaPropsEnsemble.clusterId().get()

  var brokerMetadataPublisher: BrokerMetadataPublisher = _

  var brokerRegistrationTracker: BrokerRegistrationTracker = _

  val brokerFeatures: BrokerFeatures = BrokerFeatures.createDefault(config.unstableFeatureVersionsEnabled)

  def kafkaYammerMetrics: KafkaYammerMetrics = KafkaYammerMetrics.INSTANCE

  val metadataPublishers: util.List[MetadataPublisher] = new util.ArrayList[MetadataPublisher]()

  var clientMetricsManager: ClientMetricsManager = _

  var sharePartitionManager: SharePartitionManager = _

  var persister: Persister = _

  private def maybeChangeStatus(from: ProcessStatus, to: ProcessStatus): Boolean = {
    lock.lock()
    try {
      if (status != from) return false
      info(s"Transition from $status to $to")

      status = to
      if (to == SHUTTING_DOWN) {
        isShuttingDown.set(true)
      } else if (to == SHUTDOWN) {
        isShuttingDown.set(false)
        awaitShutdownCond.signalAll()
      }
    } finally {
      lock.unlock()
    }
    true
  }

  def replicaManager: ReplicaManager = _replicaManager

  /**
   * 启动 Broker 服务器
   1. 启动流程控制
    状态转换检查和超时设置
    异常处理和错误恢复
   2. 核心组件初始化
    共享服务器启动
    动态配置初始化
    配额管理器创建
    调度器启动
   3. 存储和日志管理
    日志管理器创建（延迟启动策略）
    元数据缓存初始化
    日志目录故障处理
   4. 网络和通信组件
    Socket 服务器创建
    控制器通道管理器
    API 版本管理器
    连接断开监听器
   5. 协调器和管理器
    副本管理器创建
    生命周期管理器
    事务协调器
    组协调器
    共享协调器
   6. 安全和认证
    委托令牌管理
    凭证提供者
    授权器插件
   7. 元数据发布和同步
    元数据发布器安装
    初始追赶等待
    Broker 注册和解围栏
   8. 网络服务启动
    授权器 Future 等待
    Socket 服务器端口启动
    请求处理启用
   **/
  override def startup(): Unit = {
    if (!maybeChangeStatus(SHUTDOWN, STARTING)) return // 尝试将状态从 SHUTDOWN 改为 STARTING，如果失败则直接返回
    val startupDeadline = Deadline.fromDelay(time, config.serverMaxStartupTimeMs, TimeUnit.MILLISECONDS) // 设置启动超时时间
    try {
      sharedServer.startForBroker() // 启动共享服务器组件

      info("Starting broker") // 记录启动日志

      val clientMetricsReceiverPlugin = new ClientMetricsReceiverPlugin() // 创建客户端指标接收插件

      config.dynamicConfig.initialize(Some(clientMetricsReceiverPlugin)) // 初始化动态配置，传入客户端指标插件
      quotaManagers = QuotaFactory.instantiate(config, metrics, time, s"broker-${config.nodeId}-", ProcessRole.BrokerRole.toString) // 创建配额管理器实例
      DynamicBrokerConfig.readDynamicBrokerConfigsFromSnapshot(raftManager, config, quotaManagers, logContext) // 从快照中读取动态 Broker 配置

      /* start scheduler */
      kafkaScheduler = new KafkaScheduler(config.backgroundThreads) // 创建 Kafka 调度器，使用配置的后台线程数
      kafkaScheduler.startup() // 启动调度器

      /* register broker metrics */
      brokerTopicStats = new BrokerTopicStats(config.remoteLogManagerConfig.isRemoteStorageSystemEnabled()) // 创建 Broker 主题统计对象

      logDirFailureChannel = new LogDirFailureChannel(config.logDirs.size) // 创建日志目录故障通道

      metadataCache = new KRaftMetadataCache(config.nodeId, () => raftManager.client.kraftVersion()) // 创建 KRaft 元数据缓存

      // Create log manager, but don't start it because we need to delay any potential unclean shutdown log recovery
      // until we catch up on the metadata log and have up-to-date topic and broker configs.
      // 创建日志管理器，但不启动它，因为需要延迟任何潜在的非正常关闭日志恢复，直到我们追上元数据日志并获得最新的主题和 Broker 配置
      logManager = LogManager(config,
        sharedServer.metaPropsEnsemble.errorLogDirs().asScala.toSeq, // 传入错误日志目录
        metadataCache, // 传入元数据缓存
        kafkaScheduler, // 传入调度器
        time, // 传入时间对象
        brokerTopicStats, // 传入主题统计
        logDirFailureChannel) // 传入日志目录故障通道

      lifecycleManager = new BrokerLifecycleManager(config, // 创建 Broker 生命周期管理器
        time, // 传入时间对象
        s"broker-${config.nodeId}-", // 设置线程名前缀
        logDirs = logManager.directoryIdsSet, // 传入日志目录 ID 集合
        () => new Thread(() => shutdown(), "kafka-shutdown-thread").start()) // 设置关闭回调函数

      // Enable delegation token cache for all SCRAM mechanisms to simplify dynamic update.
      // This keeps the cache up-to-date if new SCRAM mechanisms are enabled dynamically.
      // 为所有 SCRAM 机制启用委托令牌缓存以简化动态更新，如果动态启用新的 SCRAM 机制，这将保持缓存最新
      tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames) // 创建委托令牌缓存
      credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames, tokenCache) // 创建凭证提供者

      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "controller quorum voters future",
        sharedServer.controllerQuorumVotersFuture,
        startupDeadline, time) // 等待控制器仲裁投票者 Future 完成，确保控制器集群已准备就绪
      val controllerNodeProvider = RaftControllerNodeProvider(raftManager, config) // 创建控制器节点提供者

      clientToControllerChannelManager = new NodeToControllerChannelManagerImpl( // 创建客户端到控制器的通道管理器
        controllerNodeProvider, // 控制器节点提供者
        time, // 时间对象
        metrics, // 指标对象
        config, // 配置对象
        channelName = "forwarding", // 通道名称为转发
        s"broker-${config.nodeId}-", // 线程名前缀
        retryTimeoutMs = 60000 // 重试超时时间 60 秒
      )
      clientToControllerChannelManager.start() // 启动通道管理器
      forwardingManager = new ForwardingManagerImpl(clientToControllerChannelManager, metrics) // 创建转发管理器
      clientMetricsManager = new ClientMetricsManager(clientMetricsReceiverPlugin, config.clientTelemetryMaxBytes, time, metrics) // 创建客户端指标管理器

      val apiVersionManager = new DefaultApiVersionManager( // 创建 API 版本管理器
        ListenerType.BROKER, // 监听器类型为 Broker
        () => forwardingManager.controllerApiVersions, // 获取控制器 API 版本的函数
        brokerFeatures, // Broker 特性
        metadataCache, // 元数据缓存
        config.unstableApiVersionsEnabled, // 是否启用不稳定的 API 版本
        Optional.of(clientMetricsManager) // 客户端指标管理器
      )

      val shareFetchSessionCache : ShareSessionCache = new ShareSessionCache( // 创建共享获取会话缓存
        config.shareGroupConfig.shareGroupMaxShareSessions() // 最大共享会话数
      )

      val connectionDisconnectListeners = Seq( // 创建连接断开监听器序列
        clientMetricsManager.connectionDisconnectListener(), // 客户端指标管理器的断开监听器
        shareFetchSessionCache.connectionDisconnectListener() // 共享获取会话缓存的断开监听器
      )

      // Create and start the socket server acceptor threads so that the bound port is known.
      // Delay starting processors until the end of the initialization sequence to ensure
      // that credentials have been loaded before processing authentications.
      // 创建并启动 Socket 服务器接受器线程，以便知道绑定的端口
      // 延迟启动处理器直到初始化序列结束，确保在处理身份验证之前已加载凭证
      socketServer = new SocketServer(config, // 创建 Socket 服务器
        metrics, // 指标对象
        time, // 时间对象
        credentialProvider, // 凭证提供者
        apiVersionManager, // API 版本管理器
        sharedServer.socketFactory, // Socket 工厂
        connectionDisconnectListeners) // 连接断开监听器

      clientQuotaMetadataManager = new ClientQuotaMetadataManager(quotaManagers, socketServer.connectionQuotas) // 创建客户端配额元数据管理器

      val listenerInfo = ListenerInfo.create(Optional.of(config.interBrokerListenerName.value()),
          config.effectiveAdvertisedBrokerListeners.asJava).
            withWildcardHostnamesResolved().
            withEphemeralPortsCorrected(name => socketServer.boundPort(new ListenerName(name))) // 创建监听器信息，解析通配符主机名并修正临时端口

      remoteLogManagerOpt = createRemoteLogManager(listenerInfo) // 创建远程日志管理器（可选）

      alterPartitionManager = AlterPartitionManager( // 创建分区变更管理器
        config, // 配置对象
        scheduler = kafkaScheduler, // 调度器
        controllerNodeProvider, // 控制器节点提供者
        time = time, // 时间对象
        metrics, // 指标对象
        s"broker-${config.nodeId}-", // 线程名前缀
        brokerEpochSupplier = () => lifecycleManager.brokerEpoch // Broker 纪元供应器
      )
      alterPartitionManager.start() // 启动分区变更管理器

      val addPartitionsLogContext = new LogContext(s"[AddPartitionsToTxnManager broker=${config.brokerId}]") // 创建添加分区到事务的日志上下文
      val addPartitionsToTxnNetworkClient = NetworkUtils.buildNetworkClient("AddPartitionsManager", config, metrics, time, addPartitionsLogContext) // 构建网络客户端
      val addPartitionsToTxnManager = new AddPartitionsToTxnManager( // 创建添加分区到事务管理器
        config, // 配置对象
        addPartitionsToTxnNetworkClient, // 网络客户端
        metadataCache, // 元数据缓存
        // The transaction coordinator is not created at this point so we must
        // use a lambda here.
        // 此时事务协调器尚未创建，因此必须使用 lambda 表达式
        transactionalId => transactionCoordinator.partitionFor(transactionalId), // 获取事务 ID 对应分区的函数
        time // 时间对象
      )

      val assignmentsChannelManager = new NodeToControllerChannelManagerImpl( // 创建分配通道管理器
        controllerNodeProvider, // 控制器节点提供者
        time, // 时间对象
        metrics, // 指标对象
        config, // 配置对象
        "directory-assignments", // 通道名称为目录分配
        s"broker-${config.nodeId}-", // 线程名前缀
        retryTimeoutMs = 60000 // 重试超时时间
      )
      assignmentsManager = new AssignmentsManager( // 创建分配管理器
        time, // 时间对象
        assignmentsChannelManager, // 分配通道管理器
        config.brokerId, // Broker ID
        () => metadataCache.getImage(), // 获取元数据镜像的函数
        (directoryId: Uuid) => logManager.directoryPath(directoryId). // 获取目录路径的函数
          getOrElse("[unknown directory path]") // 如果找不到则返回未知目录路径
      )
      val directoryEventHandler = new DirectoryEventHandler { // 创建目录事件处理器
        override def handleAssignment(partition: TopicIdPartition, directoryId: Uuid, reason: String, callback: Runnable): Unit =
          assignmentsManager.onAssignment(partition, directoryId, reason, callback) // 处理分区分配事件

        override def handleFailure(directoryId: Uuid): Unit =
          lifecycleManager.propagateDirectoryFailure(directoryId, config.logDirFailureTimeoutMs) // 处理目录故障事件
      }

      /**
       * TODO: move this action queue to handle thread so we can simplify concurrency handling
       */
      val defaultActionQueue = new DelayedActionQueue // 创建默认的延迟动作队列

      this._replicaManager = new ReplicaManager( // 创建副本管理器
        config = config, // 配置对象
        metrics = metrics, // 指标对象
        time = time, // 时间对象
        scheduler = kafkaScheduler, // 调度器
        logManager = logManager, // 日志管理器
        remoteLogManager = remoteLogManagerOpt, // 远程日志管理器（可选）
        quotaManagers = quotaManagers, // 配额管理器
        metadataCache = metadataCache, // 元数据缓存
        logDirFailureChannel = logDirFailureChannel, // 日志目录故障通道
        alterPartitionManager = alterPartitionManager, // 分区变更管理器
        brokerTopicStats = brokerTopicStats, // Broker 主题统计
        isShuttingDown = isShuttingDown, // 是否正在关闭的函数
        threadNamePrefix = None, // The ReplicaManager only runs on the broker, and already includes the ID in thread names.
        delayedRemoteFetchPurgatoryParam = None, // 延迟远程获取炼狱参数
        brokerEpochSupplier = () => lifecycleManager.brokerEpoch, // Broker 纪元供应器
        addPartitionsToTxnManager = Some(addPartitionsToTxnManager), // 添加分区到事务管理器
        directoryEventHandler = directoryEventHandler, // 目录事件处理器
        defaultActionQueue = defaultActionQueue // 默认动作队列
      )

      /* start token manager */
      tokenManager = new DelegationTokenManager(new DelegationTokenManagerConfigs(config), tokenCache) // 创建委托令牌管理器

      // Create and initialize an authorizer if one is configured.
      authorizerPlugin = config.createNewAuthorizer(metrics, ProcessRole.BrokerRole.toString) // 创建并初始化授权器（如果配置了）

      /* initializing the groupConfigManager */
      groupConfigManager = new GroupConfigManager(config.groupCoordinatorConfig.extractGroupConfigMap(config.shareGroupConfig)) // 初始化组配置管理器

      /* create share coordinator */
      shareCoordinator = createShareCoordinator() // 创建共享协调器

      /* create persister */
      persister = createShareStatePersister() // 创建共享状态持久化器

      groupCoordinator = createGroupCoordinator() // 创建组协调器

      val producerIdManagerSupplier = () => ProducerIdManager.rpc( // 创建生产者 ID 管理器供应器
        config.brokerId, // Broker ID
        time, // 时间对象
        () => lifecycleManager.brokerEpoch, // Broker 纪元供应器
        clientToControllerChannelManager // 客户端到控制器通道管理器
      )

      // Create transaction coordinator, but don't start it until we've started replica manager.
      // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good to fix the underlying issue
      // 创建事务协调器，但在启动副本管理器之前不启动它
      // 目前硬编码使用 Time.SYSTEM，因为某些 Streams 测试会失败，最好修复底层问题
      transactionCoordinator = TransactionCoordinator(config, replicaManager, // 创建事务协调器
        new KafkaScheduler(1, true, "transaction-log-manager-"), // 事务日志管理器调度器
        producerIdManagerSupplier, metrics, metadataCache, Time.SYSTEM) // 生产者 ID 管理器供应器、指标、元数据缓存、系统时间

      autoTopicCreationManager = new DefaultAutoTopicCreationManager( // 创建自动主题创建管理器
        config, clientToControllerChannelManager, groupCoordinator,
        transactionCoordinator, shareCoordinator)

      dynamicConfigHandlers = Map[ConfigType, ConfigHandler]( // 创建动态配置处理器映射
        ConfigType.TOPIC -> new TopicConfigHandler(replicaManager, config, quotaManagers), // 主题配置处理器
        ConfigType.BROKER -> new BrokerConfigHandler(config, quotaManagers), // Broker 配置处理器
        ConfigType.CLIENT_METRICS -> new ClientMetricsConfigHandler(clientMetricsManager), // 客户端指标配置处理器
        ConfigType.GROUP -> new GroupConfigHandler(groupCoordinator)) // 组配置处理器

      val featuresRemapped = BrokerFeatures.createDefaultFeatureMap(brokerFeatures) // 创建重映射的特性映射

      val brokerLifecycleChannelManager = new NodeToControllerChannelManagerImpl( // 创建 Broker 生命周期通道管理器
        controllerNodeProvider, // 控制器节点提供者
        time, // 时间对象
        metrics, // 指标对象
        config, // 配置对象
        "heartbeat", // 通道名称为心跳
        s"broker-${config.nodeId}-", // 线程名前缀
        config.brokerSessionTimeoutMs / 2 // KAFKA-14392 // 会话超时时间的一半
      )
      lifecycleManager.start( // 启动生命周期管理器
        () => sharedServer.loader.lastAppliedOffset(), // 获取最后应用偏移量的函数
        brokerLifecycleChannelManager, // 生命周期通道管理器
        clusterId, // 集群 ID
        listenerInfo.toBrokerRegistrationRequest, // Broker 注册请求
        featuresRemapped, // 重映射的特性
        logManager.readBrokerEpochFromCleanShutdownFiles() // 从正常关闭文件读取 Broker 纪元
      )

      // The FetchSessionCache is divided into config.numIoThreads shards, each responsible
      // for Math.max(1, shardNum * sessionIdRange) <= sessionId < (shardNum + 1) * sessionIdRange
      // FetchSessionCache 被分为 config.numIoThreads 个分片，每个分片负责 Math.max(1, shardNum * sessionIdRange) <= sessionId < (shardNum + 1) * sessionIdRange
      val sessionIdRange = Int.MaxValue / NumFetchSessionCacheShards // 计算会话 ID 范围
      val fetchSessionCacheShards = (0 until NumFetchSessionCacheShards) // 创建获取会话缓存分片
        .map(shardNum => new FetchSessionCacheShard(
          config.maxIncrementalFetchSessionCacheSlots / NumFetchSessionCacheShards, // 每个分片的最大增量获取会话缓存槽数
          KafkaBroker.MIN_INCREMENTAL_FETCH_SESSION_EVICTION_MS, // 最小增量获取会话驱逐时间
          sessionIdRange, // 会话 ID 范围
          shardNum // 分片编号
        ))
      val fetchManager = new FetchManager(Time.SYSTEM, new FetchSessionCache(fetchSessionCacheShards)) // 创建获取管理器

      sharePartitionManager = new SharePartitionManager(
        replicaManager,
        time,
        shareFetchSessionCache,
        config.shareGroupConfig.shareGroupRecordLockDurationMs,
        config.shareGroupConfig.shareGroupDeliveryCountLimit,
        config.shareGroupConfig.shareGroupPartitionMaxRecordLocks,
        config.remoteLogManagerConfig.remoteFetchMaxWaitMs().toLong,
        persister,
        groupConfigManager,
        brokerTopicStats
      )

      dataPlaneRequestProcessor = new KafkaApis(
        requestChannel = socketServer.dataPlaneRequestChannel,
        forwardingManager = forwardingManager,
        replicaManager = replicaManager,
        groupCoordinator = groupCoordinator,
        txnCoordinator = transactionCoordinator,
        shareCoordinator = shareCoordinator,
        autoTopicCreationManager = autoTopicCreationManager,
        brokerId = config.nodeId,
        config = config,
        configRepository = metadataCache,
        metadataCache = metadataCache,
        metrics = metrics,
        authorizerPlugin = authorizerPlugin,
        quotas = quotaManagers,
        fetchManager = fetchManager,
        sharePartitionManager = sharePartitionManager,
        brokerTopicStats = brokerTopicStats,
        clusterId = clusterId,
        time = time,
        tokenManager = tokenManager,
        apiVersionManager = apiVersionManager,
        clientMetricsManager = clientMetricsManager,
        groupConfigManager = groupConfigManager)

      dataPlaneRequestHandlerPool = new KafkaRequestHandlerPool(config.nodeId,
        socketServer.dataPlaneRequestChannel, dataPlaneRequestProcessor, time,
        config.numIoThreads, "RequestHandlerAvgIdlePercent")

      metadataPublishers.add(new MetadataVersionConfigValidator(config, sharedServer.metadataPublishingFaultHandler))
      brokerMetadataPublisher = new BrokerMetadataPublisher(config,
        metadataCache,
        logManager,
        replicaManager,
        groupCoordinator,
        transactionCoordinator,
        shareCoordinator,
        sharePartitionManager,
        new DynamicConfigPublisher(
          config,
          sharedServer.metadataPublishingFaultHandler,
          dynamicConfigHandlers.toMap,
        "broker"),
        new DynamicClientQuotaPublisher(
          config,
          sharedServer.metadataPublishingFaultHandler,
          "broker",
          clientQuotaMetadataManager,
        ),
        new DynamicTopicClusterQuotaPublisher(
          clusterId,
          config,
          sharedServer.metadataPublishingFaultHandler,
          "broker",
          quotaManagers,
        ),
        new ScramPublisher(
          config,
          sharedServer.metadataPublishingFaultHandler,
          "broker",
          credentialProvider),
        new DelegationTokenPublisher(
          config,
          sharedServer.metadataPublishingFaultHandler,
          "broker",
          tokenManager),
        new AclPublisher(
          config.nodeId,
          sharedServer.metadataPublishingFaultHandler,
          "broker",
          authorizerPlugin.toJava
        ),
        sharedServer.initialBrokerMetadataLoadFaultHandler,
        sharedServer.metadataPublishingFaultHandler
      )
      // If the BrokerLifecycleManager's initial catch-up future fails, it means we timed out
      // or are shutting down before we could catch up. Therefore, also fail the firstPublishFuture.
      lifecycleManager.initialCatchUpFuture.whenComplete((_, e) => {
        if (e != null) brokerMetadataPublisher.firstPublishFuture.completeExceptionally(e)
      })
      metadataPublishers.add(brokerMetadataPublisher)
      brokerRegistrationTracker = new BrokerRegistrationTracker(config.brokerId,
        () => lifecycleManager.resendBrokerRegistration())
      metadataPublishers.add(brokerRegistrationTracker)


      // Register parts of the broker that can be reconfigured via dynamic configs.  This needs to
      // be done before we publish the dynamic configs, so that we don't miss anything.
      config.dynamicConfig.addReconfigurables(this)

      // Install all the metadata publishers.
      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "the broker metadata publishers to be installed",
        sharedServer.loader.installPublishers(metadataPublishers), startupDeadline, time)

      // Wait for this broker to contact the quorum, and for the active controller to acknowledge
      // us as caught up. It will do this by returning a heartbeat response with isCaughtUp set to
      // true. The BrokerLifecycleManager tracks this.
      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "the controller to acknowledge that we are caught up",
        lifecycleManager.initialCatchUpFuture, startupDeadline, time)

      // Wait for the first metadata update to be published. Metadata updates are not published
      // until we read at least up to the high water mark of the cluster metadata partition.
      // Usually, we publish the initial metadata before lifecycleManager.initialCatchUpFuture
      // is completed, so this check is not necessary. But this is a simple check to make
      // completely sure.
      // 等待第一个元数据更新发布。元数据更新只有在读取到集群元数据分区的高水位标记后才会发布
      // 通常我们会在 lifecycleManager.initialCatchUpFuture 完成之前发布初始元数据，所以这个检查不是必需的
      // 但这是一个简单的检查，确保完全正确
      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "the initial broker metadata update to be published",
        brokerMetadataPublisher.firstPublishFuture , startupDeadline, time)

      // Now that we have loaded some metadata, we can log a reasonably up-to-date broker
      // configuration.  Keep in mind that KafkaConfig.originals is a mutable field that gets set
      // by the dynamic configuration publisher. Ironically, KafkaConfig.originals does not
      // contain the original configuration values.
      // 现在我们已经加载了一些元数据，可以记录一个相当最新的 Broker 配置
      // 请记住 KafkaConfig.originals 是一个可变字段，由动态配置发布器设置
      // 讽刺的是，KafkaConfig.originals 并不包含原始配置值
      new KafkaConfig(config.originals(), true)

      // We're now ready to unfence the broker. This also allows this broker to transition
      // from RECOVERY state to RUNNING state, once the controller unfences the broker.
      // 现在我们准备好解除 Broker 的围栏。这也允许此 Broker 从 RECOVERY 状态转换到 RUNNING 状态，一旦控制器解除 Broker 的围栏
      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "the broker to be unfenced",
        lifecycleManager.setReadyToUnfence(), startupDeadline, time)

      // Enable inbound TCP connections. Each endpoint will be started only once its matching
      // authorizer future is completed.
      // 启用入站 TCP 连接。每个端点只有在其匹配的授权器 Future 完成后才会启动
      val endpointReadyFutures = {
        val builder = new EndpointReadyFutures.Builder()
        builder.build(authorizerPlugin.toJava,
          new KafkaAuthorizerServerInfo(
            new ClusterResource(clusterId),
            config.nodeId,
            listenerInfo.listeners().values(),
            listenerInfo.firstListener(),
            config.earlyStartListeners.map(_.value()).asJava))
      }
      val authorizerFutures = endpointReadyFutures.futures().asScala.toMap
      val enableRequestProcessingFuture = socketServer.enableRequestProcessing(authorizerFutures)

      // Block here until all the authorizer futures are complete.
      // 在这里阻塞，直到所有授权器 Future 完成
      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "all of the authorizer futures to be completed",
        CompletableFuture.allOf(authorizerFutures.values.toSeq: _*), startupDeadline, time)

      // Wait for all the SocketServer ports to be open, and the Acceptors to be started.
      // 等待所有 SocketServer 端口打开，以及 Acceptor 启动
      FutureUtils.waitWithLogging(logger.underlying, logIdent,
        "all of the SocketServer Acceptors to be started",
        enableRequestProcessingFuture, startupDeadline, time)

      maybeChangeStatus(STARTING, STARTED) // 将状态从 STARTING 改为 STARTED
    } catch {
      case e: Throwable =>
        maybeChangeStatus(STARTING, STARTED)
        fatal("Fatal error during broker startup. Prepare to shutdown", e)
        shutdown()
        throw if (e.isInstanceOf[ExecutionException]) e.getCause else e
    }
  }

  private def createGroupCoordinator(): GroupCoordinator = {
    // Create group coordinator, but don't start it until we've started replica manager.
    // Hardcode Time.SYSTEM for now as some Streams tests fail otherwise, it would be good
    // to fix the underlying issue.
    val time = Time.SYSTEM
    val serde = new GroupCoordinatorRecordSerde
    val timer = new SystemTimerReaper(
      "group-coordinator-reaper",
      new SystemTimer("group-coordinator")
    )
    val loader = new CoordinatorLoaderImpl[CoordinatorRecord](
      time,
      replicaManager,
      serde,
      config.groupCoordinatorConfig.offsetsLoadBufferSize
    )
    val writer = new CoordinatorPartitionWriter(
      replicaManager
    )
    new GroupCoordinatorService.Builder(config.brokerId, config.groupCoordinatorConfig)
      .withTime(time)
      .withTimer(timer)
      .withLoader(loader)
      .withWriter(writer)
      .withCoordinatorRuntimeMetrics(new GroupCoordinatorRuntimeMetrics(metrics))
      .withGroupCoordinatorMetrics(new GroupCoordinatorMetrics(KafkaYammerMetrics.defaultRegistry, metrics))
      .withGroupConfigManager(groupConfigManager)
      .withPersister(persister)
      .withAuthorizerPlugin(authorizerPlugin.toJava)
      .build()
  }

  private def createShareCoordinator(): ShareCoordinator = {
    val time = Time.SYSTEM
    val timer = new SystemTimerReaper(
      "share-coordinator-reaper",
      new SystemTimer("share-coordinator")
    )

    val serde = new ShareCoordinatorRecordSerde
    val loader = new CoordinatorLoaderImpl[CoordinatorRecord](
      time,
      replicaManager,
      serde,
      config.shareCoordinatorConfig.shareCoordinatorLoadBufferSize()
    )
    val writer = new CoordinatorPartitionWriter(
      replicaManager
    )
    new ShareCoordinatorService.Builder(config.brokerId, config.shareCoordinatorConfig)
      .withTimer(timer)
      .withTime(time)
      .withLoader(loader)
      .withWriter(writer)
      .withCoordinatorRuntimeMetrics(new ShareCoordinatorRuntimeMetrics(metrics))
      .withCoordinatorMetrics(new ShareCoordinatorMetrics(metrics))
      .withShareGroupEnabledConfigSupplier(() => config.shareGroupConfig.isShareGroupEnabled)
      .build()
  }

  private def createShareStatePersister(): Persister = {
    if (config.shareGroupConfig.shareGroupPersisterClassName.nonEmpty) {
      val klass = Utils.loadClass(config.shareGroupConfig.shareGroupPersisterClassName, classOf[Object]).asInstanceOf[Class[Persister]]

      if (klass.getName.equals(classOf[DefaultStatePersister].getName)) {
        klass.getConstructor(classOf[PersisterStateManager])
          .newInstance(
            new PersisterStateManager(
              NetworkUtils.buildNetworkClient("Persister", config, metrics, Time.SYSTEM, new LogContext(s"[Persister broker=${config.brokerId}]")),
              new ShareCoordinatorMetadataCacheHelperImpl(metadataCache, key => shareCoordinator.partitionFor(key), config.interBrokerListenerName),
              Time.SYSTEM,
              new SystemTimerReaper(
                "persister-state-manager-reaper",
                new SystemTimer("persister")
              )
            )
          )
      } else if (klass.getName.equals(classOf[NoOpStatePersister].getName)) {
        info("Using no-op persister")
        new NoOpStatePersister()
      } else {
        error("Unknown persister specified. Persister is only factory-pluggable!")
        throw new IllegalArgumentException("Unknown persister specified " + config.shareGroupConfig.shareGroupPersisterClassName)
      }
    } else {
      // in case share coordinator not enabled or persister class name deliberately empty (key=)
      info("Using no-op persister")
      new NoOpStatePersister()
    }
  }

  protected def createRemoteLogManager(listenerInfo: ListenerInfo): Option[RemoteLogManager] = {
    if (config.remoteLogManagerConfig.isRemoteStorageSystemEnabled) {
      val listenerName = config.remoteLogManagerConfig.remoteLogMetadataManagerListenerName()
      val endpoint = if (listenerName != null) {
        Some(listenerInfo.listeners().values().stream
          .filter(e =>
            e.listenerName().isPresent &&
              ListenerName.normalised(e.listenerName().get()).equals(ListenerName.normalised(listenerName))
          )
          .findFirst()
          .orElseThrow(() => new ConfigException(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_LISTENER_NAME_PROP,
            listenerName, "Should be set as a listener name within valid broker listener name list: " + listenerInfo.listeners().values())))
      } else {
        None
      }

      val rlm = new RemoteLogManager(config.remoteLogManagerConfig, config.brokerId, config.logDirs.get(0), clusterId, time,
        (tp: TopicPartition) => logManager.getLog(tp).toJava,
        (tp: TopicPartition, remoteLogStartOffset: java.lang.Long) => {
          logManager.getLog(tp).foreach { log =>
            log.updateLogStartOffsetFromRemoteTier(remoteLogStartOffset)
          }
        },
        brokerTopicStats, metrics, endpoint.toJava)
      Some(rlm)
    } else {
      None
    }
  }

  override def shutdown(timeout: Duration): Unit = {
    if (!maybeChangeStatus(STARTED, SHUTTING_DOWN)) return
    try {
      val deadline = time.milliseconds() + timeout.toMillis
      info("shutting down")

      if (config.controlledShutdownEnable) {
        if (replicaManager != null)
          replicaManager.beginControlledShutdown()

        if (lifecycleManager != null) {
          lifecycleManager.beginControlledShutdown()
          try {
            val controlledShutdownTimeoutMs = deadline - time.milliseconds()
            lifecycleManager.controlledShutdownFuture.get(controlledShutdownTimeoutMs, TimeUnit.MILLISECONDS)
          } catch {
            case _: TimeoutException =>
              error("Timed out waiting for the controller to approve controlled shutdown")
            case e: Throwable =>
              error("Got unexpected exception waiting for controlled shutdown future", e)
          }
        }
      }
      if (lifecycleManager != null)
        lifecycleManager.beginShutdown()

      // Stop socket server to stop accepting any more connections and requests.
      // Socket server will be shutdown towards the end of the sequence.
      if (socketServer != null) {
        CoreUtils.swallow(socketServer.stopProcessingRequests(), this)
      }
      metadataPublishers.forEach(p => sharedServer.loader.removeAndClosePublisher(p).get())
      metadataPublishers.clear()
      if (dataPlaneRequestHandlerPool != null)
        CoreUtils.swallow(dataPlaneRequestHandlerPool.shutdown(), this)
      if (dataPlaneRequestProcessor != null)
        CoreUtils.swallow(dataPlaneRequestProcessor.close(), this)
      authorizerPlugin.foreach(Utils.closeQuietly(_, "authorizer plugin"))

      /**
       * We must shutdown the scheduler early because otherwise, the scheduler could touch other
       * resources that might have been shutdown and cause exceptions.
       * For example, if we didn't shutdown the scheduler first, when LogManager was closing
       * partitions one by one, the scheduler might concurrently delete old segments due to
       * retention. However, the old segments could have been closed by the LogManager, which would
       * cause an IOException and subsequently mark logdir as offline. As a result, the broker would
       * not flush the remaining partitions or write the clean shutdown marker. Ultimately, the
       * broker would have to take hours to recover the log during restart.
       */
      if (kafkaScheduler != null)
        CoreUtils.swallow(kafkaScheduler.shutdown(), this)

      if (transactionCoordinator != null)
        CoreUtils.swallow(transactionCoordinator.shutdown(), this)

      if (groupConfigManager != null)
        CoreUtils.swallow(groupConfigManager.close(), this)
      if (groupCoordinator != null)
        CoreUtils.swallow(groupCoordinator.shutdown(), this)
      if (shareCoordinator != null)
        CoreUtils.swallow(shareCoordinator.shutdown(), this)

      if (assignmentsManager != null)
        CoreUtils.swallow(assignmentsManager.close(), this)

      if (replicaManager != null)
        CoreUtils.swallow(replicaManager.shutdown(), this)

      if (alterPartitionManager != null)
        CoreUtils.swallow(alterPartitionManager.shutdown(), this)

      if (forwardingManager != null)
        CoreUtils.swallow(forwardingManager.close(), this)

      if (clientToControllerChannelManager != null)
        CoreUtils.swallow(clientToControllerChannelManager.shutdown(), this)

      if (logManager != null) {
        val brokerEpoch = if (lifecycleManager != null) lifecycleManager.brokerEpoch else -1
        CoreUtils.swallow(logManager.shutdown(brokerEpoch), this)
      }

      // Close remote log manager to give a chance to any of its underlying clients
      // (especially in RemoteStorageManager and RemoteLogMetadataManager) to close gracefully.
      remoteLogManagerOpt.foreach(Utils.closeQuietly(_, "remote log manager"))

      if (quotaManagers != null)
        CoreUtils.swallow(quotaManagers.shutdown(), this)

      if (socketServer != null)
        CoreUtils.swallow(socketServer.shutdown(), this)

      Utils.closeQuietly(brokerTopicStats, "broker topic stats")
      Utils.closeQuietly(sharePartitionManager, "share partition manager")

      if (persister != null)
        CoreUtils.swallow(persister.stop(), this)

      isShuttingDown.set(false)

      if (lifecycleManager != null)
        CoreUtils.swallow(lifecycleManager.close(), this)

      CoreUtils.swallow(config.dynamicConfig.clear(), this)
      Utils.closeQuietly(clientMetricsManager, "client metrics manager")
      sharedServer.stopForBroker()
      info("shut down completed")
    } catch {
      case e: Throwable =>
        fatal("Fatal error during broker shutdown.", e)
        throw e
    } finally {
      maybeChangeStatus(SHUTTING_DOWN, SHUTDOWN)
    }
  }

  override def isShutdown(): Boolean = {
    status == SHUTDOWN || status == SHUTTING_DOWN
  }

  override def awaitShutdown(): Unit = {
    lock.lock()
    try {
      while (true) {
        if (status == SHUTDOWN) return
        awaitShutdownCond.awaitUninterruptibly()
      }
    } finally {
      lock.unlock()
    }
  }

  override def boundPort(listenerName: ListenerName): Int = socketServer.boundPort(listenerName)

}
