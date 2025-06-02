/*
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

import kafka.network.SocketServer
import kafka.raft.KafkaRaftManager
import kafka.server.QuotaFactory.QuotaManagers

import scala.collection.immutable
import kafka.server.metadata.{ClientQuotaMetadataManager, DelegationTokenPublisher, DynamicClientQuotaPublisher, DynamicConfigPublisher, DynamicTopicClusterQuotaPublisher, KRaftMetadataCache, KRaftMetadataCachePublisher, ScramPublisher}
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.common.internals.Plugin
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.common.security.token.delegation.internals.DelegationTokenCache
import org.apache.kafka.common.utils.{LogContext, Utils}
import org.apache.kafka.common.{ClusterResource, Endpoint, Uuid}
import org.apache.kafka.controller.metrics.{ControllerMetadataMetricsPublisher, QuorumControllerMetrics}
import org.apache.kafka.controller.{Controller, QuorumController, QuorumFeatures}
import org.apache.kafka.image.publisher.{ControllerRegistrationsPublisher, MetadataPublisher}
import org.apache.kafka.metadata.{KafkaConfigSchema, ListenerInfo}
import org.apache.kafka.metadata.authorizer.ClusterMetadataAuthorizer
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata
import org.apache.kafka.metadata.publisher.{AclPublisher, FeaturesPublisher}
import org.apache.kafka.raft.QuorumConfig
import org.apache.kafka.security.CredentialProvider
import org.apache.kafka.server.{DelegationTokenManager, ProcessRole, SimpleApiVersionManager}
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.config.ServerLogConfigs.{ALTER_CONFIG_POLICY_CLASS_NAME_CONFIG, CREATE_TOPIC_POLICY_CLASS_NAME_CONFIG}
import org.apache.kafka.server.common.{ApiMessageAndVersion, KRaftVersion, NodeToControllerChannelManager}
import org.apache.kafka.server.config.{ConfigType, DelegationTokenManagerConfigs}
import org.apache.kafka.server.metrics.{KafkaMetricsGroup, KafkaYammerMetrics, LinuxIoMetricsCollector}
import org.apache.kafka.server.network.{EndpointReadyFutures, KafkaAuthorizerServerInfo}
import org.apache.kafka.server.policy.{AlterConfigPolicy, CreateTopicPolicy}
import org.apache.kafka.server.util.{Deadline, FutureUtils}

import java.util
import java.util.{Optional, OptionalLong}
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{CompletableFuture, TimeUnit}
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.RichOption


/**
 * A Kafka controller that runs in KRaft (Kafka Raft) mode.
 */
class ControllerServer(
  val sharedServer: SharedServer,
  val configSchema: KafkaConfigSchema,
  val bootstrapMetadata: BootstrapMetadata
) extends Logging {

  import kafka.server.Server._

  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  val config = sharedServer.controllerConfig
  val logContext = new LogContext(s"[ControllerServer id=${config.nodeId}] ")
  val time = sharedServer.time
  def metrics = sharedServer.metrics
  def raftManager: KafkaRaftManager[ApiMessageAndVersion] = sharedServer.raftManager

  val lock = new ReentrantLock()
  val awaitShutdownCond = lock.newCondition()
  var status: ProcessStatus = SHUTDOWN

  var linuxIoMetricsCollector: LinuxIoMetricsCollector = _
  @volatile var authorizerPlugin: Option[Plugin[Authorizer]] = None
  var tokenCache: DelegationTokenCache = _
  var credentialProvider: CredentialProvider = _
  var socketServer: SocketServer = _
  val socketServerFirstBoundPortFuture = new CompletableFuture[Integer]()
  var createTopicPolicy: Option[CreateTopicPolicy] = None
  var alterConfigPolicy: Option[AlterConfigPolicy] = None
  @volatile var quorumControllerMetrics: QuorumControllerMetrics = _
  var controller: Controller = _
  var quotaManagers: QuotaManagers = _
  var clientQuotaMetadataManager: ClientQuotaMetadataManager = _
  var controllerApis: ControllerApis = _
  var controllerApisHandlerPool: KafkaRequestHandlerPool = _
  def kafkaYammerMetrics: KafkaYammerMetrics = KafkaYammerMetrics.INSTANCE
  val metadataPublishers: util.List[MetadataPublisher] = new util.ArrayList[MetadataPublisher]()
  @volatile var metadataCache : KRaftMetadataCache = _
  @volatile var metadataCachePublisher: KRaftMetadataCachePublisher = _
  @volatile var featuresPublisher: FeaturesPublisher = _
  @volatile var registrationsPublisher: ControllerRegistrationsPublisher = _
  @volatile var incarnationId: Uuid = _
  @volatile var registrationManager: ControllerRegistrationManager = _
  @volatile var registrationChannelManager: NodeToControllerChannelManager = _

  private def maybeChangeStatus(from: ProcessStatus, to: ProcessStatus): Boolean = {
    lock.lock()
    try {
      if (status != from) return false
      status = to
      if (to == SHUTDOWN) awaitShutdownCond.signalAll()
    } finally {
      lock.unlock()
    }
    true
  }

  def clusterId: String = sharedServer.clusterId

  /**
   * 启动控制器服务器
   *
   * 此方法负责初始化和启动Kafka控制器的所有组件，包括：
   * - 状态管理和日志配置
   * - 指标收集器和监控组件
   * - 授权插件和安全组件
   * - 元数据缓存和发布器
   * - Socket服务器和API处理器
   * - 配额管理器和策略组件
   * - 控制器核心组件和Raft客户端
   * - 各种元数据发布器和管理器
   *
   * 启动过程是有序的，确保所有依赖组件在使用前都已正确初始化。
   * 如果启动过程中发生异常，会自动清理已初始化的资源并重新抛出异常。
   */
  def startup(): Unit = {
    if (!maybeChangeStatus(SHUTDOWN, STARTING)) return // 检查并更改状态从SHUTDOWN到STARTING
    val startupDeadline = Deadline.fromDelay(time, config.serverMaxStartupTimeMs, TimeUnit.MILLISECONDS) // 设置启动超时时间
    try {
      this.logIdent = logContext.logPrefix() // 设置日志标识符
      info("Starting controller") // 记录启动日志
      config.dynamicConfig.initialize(clientMetricsReceiverPluginOpt = None) // 初始化动态配置

      maybeChangeStatus(STARTING, STARTED) // 更改状态从STARTING到STARTED

      metricsGroup.newGauge("ClusterId", () => clusterId) // 注册集群ID指标
      metricsGroup.newGauge("yammer-metrics-count", () =>  KafkaYammerMetrics.defaultRegistry.allMetrics.size) // 注册Yammer指标数量

      linuxIoMetricsCollector = new LinuxIoMetricsCollector("/proc", time) // 创建Linux IO指标收集器
      if (linuxIoMetricsCollector.usable()) { // 如果IO指标收集器可用
        metricsGroup.newGauge("linux-disk-read-bytes", () => linuxIoMetricsCollector.readBytes()) // 注册磁盘读取字节数指标
        metricsGroup.newGauge("linux-disk-write-bytes", () => linuxIoMetricsCollector.writeBytes()) // 注册磁盘写入字节数指标
      }

      authorizerPlugin = config.createNewAuthorizer(metrics, ProcessRole.ControllerRole.toString) // 创建授权插件

      metadataCache = new KRaftMetadataCache(config.nodeId, () => raftManager.client.kraftVersion()) // 创建KRaft元数据缓存

      metadataCachePublisher = new KRaftMetadataCachePublisher(metadataCache) // 创建元数据缓存发布器

      featuresPublisher = new FeaturesPublisher(logContext) // 创建特性发布器

      registrationsPublisher = new ControllerRegistrationsPublisher() // 创建控制器注册发布器

      incarnationId = Uuid.randomUuid() // 生成随机的实例化ID

      val apiVersionManager = new SimpleApiVersionManager( // 创建API版本管理器
        ListenerType.CONTROLLER,
        config.unstableApiVersionsEnabled,
        () => featuresPublisher.features().setFinalizedLevel(
          KRaftVersion.FEATURE_NAME,
          raftManager.client.kraftVersion().featureLevel())
      )

      //  metrics will be set to null when closing a controller, so we should recreate it for testing
      if (sharedServer.metrics == null){ // 如果指标对象为空，重新创建（用于测试）
        sharedServer.metrics = new Metrics()
      }

      tokenCache = new DelegationTokenCache(ScramMechanism.mechanismNames) // 创建委托令牌缓存
      credentialProvider = new CredentialProvider(ScramMechanism.mechanismNames, tokenCache) // 创建凭证提供器
      socketServer = new SocketServer(config, // 创建Socket服务器
        metrics,
        time,
        credentialProvider,
        apiVersionManager,
        sharedServer.socketFactory)

      val listenerInfo = ListenerInfo // 创建监听器信息
        .create(config.effectiveAdvertisedControllerListeners.asJava)
        .withWildcardHostnamesResolved()
        .withEphemeralPortsCorrected(name => socketServer.boundPort(new ListenerName(name)))
      socketServerFirstBoundPortFuture.complete(listenerInfo.firstListener().port()) // 完成第一个绑定端口的Future

      val endpointReadyFutures = { // 创建端点就绪Future
        val builder = new EndpointReadyFutures.Builder()
        builder.build(authorizerPlugin.toJava,
          new KafkaAuthorizerServerInfo(
            new ClusterResource(clusterId),
            config.nodeId,
            listenerInfo.listeners().values(),
            listenerInfo.firstListener(),
            config.earlyStartListeners.map(_.value()).asJava))
      }

      sharedServer.startForController(listenerInfo) // 启动共享服务器的控制器部分

      createTopicPolicy = Option(config. // 创建主题策略配置
        getConfiguredInstance(CREATE_TOPIC_POLICY_CLASS_NAME_CONFIG, classOf[CreateTopicPolicy]))
      alterConfigPolicy = Option(config. // 修改配置策略配置
        getConfiguredInstance(ALTER_CONFIG_POLICY_CLASS_NAME_CONFIG, classOf[AlterConfigPolicy]))

      val voterConnections = FutureUtils.waitWithLogging(logger.underlying, logIdent, // 等待控制器仲裁投票者连接
        "controller quorum voters future",
        sharedServer.controllerQuorumVotersFuture,
        startupDeadline, time)
      val controllerNodes = QuorumConfig.voterConnectionsToNodes(voterConnections) // 将投票者连接转换为节点
      val quorumFeatures = new QuorumFeatures(config.nodeId, // 创建仲裁特性配置
        QuorumFeatures.defaultSupportedFeatureMap(config.unstableFeatureVersionsEnabled),
        controllerNodes.asScala.map(node => Integer.valueOf(node.id())).asJava)

      val delegationTokenManagerConfigs = new DelegationTokenManagerConfigs(config) // 创建委托令牌管理器配置
      val delegationTokenKeyString = { // 获取委托令牌密钥字符串
        if (delegationTokenManagerConfigs.tokenAuthEnabled) {
          delegationTokenManagerConfigs.delegationTokenSecretKey.value
        } else {
          null
        }
      }

      val controllerBuilder = { // 创建控制器构建器
        val leaderImbalanceCheckIntervalNs = if (config.autoLeaderRebalanceEnable) { // 如果启用自动领导者重平衡，设置检查间隔
          OptionalLong.of(TimeUnit.NANOSECONDS.convert(config.leaderImbalanceCheckIntervalSeconds, TimeUnit.SECONDS))
        } else {
          OptionalLong.empty()
        }

        val maxIdleIntervalNs = config.metadataMaxIdleIntervalNs.fold(OptionalLong.empty)(OptionalLong.of) // 设置最大空闲间隔

        quorumControllerMetrics = new QuorumControllerMetrics(Optional.of(KafkaYammerMetrics.defaultRegistry), time, config.brokerSessionTimeoutMs) // 创建仲裁控制器指标

        new QuorumController.Builder(config.nodeId, sharedServer.clusterId). // 创建仲裁控制器构建器
          setTime(time). // 设置时间
          setThreadNamePrefix(s"quorum-controller-${config.nodeId}-"). // 设置线程名前缀
          setConfigSchema(configSchema). // 设置配置模式
          setRaftClient(raftManager.client). // 设置Raft客户端
          setQuorumFeatures(quorumFeatures). // 设置仲裁特性
          setDefaultReplicationFactor(config.defaultReplicationFactor.toShort). // 设置默认复制因子
          setDefaultNumPartitions(config.numPartitions.intValue()). // 设置默认分区数
          setSessionTimeoutNs(TimeUnit.NANOSECONDS.convert(config.brokerSessionTimeoutMs.longValue(), // 设置会话超时时间
            TimeUnit.MILLISECONDS)).
          setLeaderImbalanceCheckIntervalNs(leaderImbalanceCheckIntervalNs). // 设置领导者不平衡检查间隔
          setMaxIdleIntervalNs(maxIdleIntervalNs). // 设置最大空闲间隔
          setMetrics(quorumControllerMetrics). // 设置指标
          setCreateTopicPolicy(createTopicPolicy.toJava). // 设置创建主题策略
          setAlterConfigPolicy(alterConfigPolicy.toJava). // 设置修改配置策略
          setConfigurationValidator(new ControllerConfigurationValidator(sharedServer.brokerConfig)). // 设置配置验证器
          setStaticConfig(config.originals). // 设置静态配置
          setBootstrapMetadata(bootstrapMetadata). // 设置引导元数据
          setFatalFaultHandler(sharedServer.fatalQuorumControllerFaultHandler). // 设置致命故障处理器
          setNonFatalFaultHandler(sharedServer.nonFatalQuorumControllerFaultHandler). // 设置非致命故障处理器
          setDelegationTokenCache(tokenCache). // 设置委托令牌缓存
          setDelegationTokenSecretKey(delegationTokenKeyString). // 设置委托令牌密钥
          setDelegationTokenMaxLifeMs(delegationTokenManagerConfigs.delegationTokenMaxLifeMs). // 设置委托令牌最大生命周期
          setDelegationTokenExpiryTimeMs(delegationTokenManagerConfigs.delegationTokenExpiryTimeMs). // 设置委托令牌过期时间
          setDelegationTokenExpiryCheckIntervalMs(delegationTokenManagerConfigs.delegationTokenExpiryCheckIntervalMs). // 设置委托令牌过期检查间隔
          setUncleanLeaderElectionCheckIntervalMs(config.uncleanLeaderElectionCheckIntervalMs). // 设置不洁领导者选举检查间隔
          setInterBrokerListenerName(config.interBrokerListenerName.value()). // 设置代理间监听器名称
          setControllerPerformanceSamplePeriodMs(config.controllerPerformanceSamplePeriodMs). // 设置控制器性能采样周期
          setControllerPerformanceAlwaysLogThresholdMs(config.controllerPerformanceAlwaysLogThresholdMs) // 设置控制器性能总是记录阈值
      }
      controller = controllerBuilder.build() // 构建控制器

      // If we are using a ClusterMetadataAuthorizer, requests to add or remove ACLs must go
      // through the controller.
      authorizerPlugin.foreach { plugin => // 如果使用集群元数据授权器，ACL的添加或删除请求必须通过控制器
        plugin.get match {
          case a: ClusterMetadataAuthorizer => a.setAclMutator(controller)
          case _ =>
        }
      }

      quotaManagers = QuotaFactory.instantiate(config, // 创建配额管理器
        metrics,
        time,
        s"controller-${config.nodeId}-", ProcessRole.ControllerRole.toString)
      clientQuotaMetadataManager = new ClientQuotaMetadataManager(quotaManagers, socketServer.connectionQuotas) // 创建客户端配额元数据管理器
      controllerApis = new ControllerApis(socketServer.dataPlaneRequestChannel, // 创建控制器API处理器
        authorizerPlugin,
        quotaManagers,
        time,
        controller,
        raftManager,
        config,
        clusterId,
        registrationsPublisher,
        apiVersionManager,
        metadataCache)
      controllerApisHandlerPool = new KafkaRequestHandlerPool(config.nodeId, // 创建控制器API处理器线程池
        socketServer.dataPlaneRequestChannel,
        controllerApis,
        time,
        config.numIoThreads,
        "RequestHandlerAvgIdlePercent",
        "controller")

      // Set up the metadata cache publisher.
      metadataPublishers.add(metadataCachePublisher) // 设置元数据缓存发布器

      // Set up the metadata features publisher.
      metadataPublishers.add(featuresPublisher) // 设置元数据特性发布器

      // Set up the controller registrations publisher.
      metadataPublishers.add(registrationsPublisher) // 设置控制器注册发布器

      // Create the registration manager, which handles sending KIP-919 controller registrations.
      registrationManager = new ControllerRegistrationManager(config.nodeId, // 创建注册管理器，处理KIP-919控制器注册
        clusterId,
        time,
        s"controller-${config.nodeId}-",
        QuorumFeatures.defaultSupportedFeatureMap(config.unstableFeatureVersionsEnabled),
        incarnationId,
        listenerInfo)

      // Add the registration manager to the list of metadata publishers, so that it receives
      // callbacks when the cluster registrations change.
      metadataPublishers.add(registrationManager) // 将注册管理器添加到元数据发布器列表，以便在集群注册变更时接收回调

      // Set up the dynamic config publisher. This runs even in combined mode, since the broker
      // has its own separate dynamic configuration object. 设置动态配置发布器。即使在组合模式下也会运行，因为 broker 有自己独立的动态配置对象。
      metadataPublishers.add(new DynamicConfigPublisher(
        config,
        sharedServer.metadataPublishingFaultHandler,
        immutable.Map[ConfigType, ConfigHandler](
          // controllers don't host topics, so no need to do anything with dynamic topic config changes here
          ConfigType.BROKER -> new BrokerConfigHandler(config, quotaManagers)
        ),
        "controller"))

      // Register this instance for dynamic config changes to the KafkaConfig. This must be called
      // after the authorizer and quotaManagers are initialized, since it references those objects.
      // It must be called before DynamicClientQuotaPublisher is installed, since otherwise we may
      // miss the initial update which establishes the dynamic configurations that are in effect on
      // startup.
      // 注册当前实例以监听 KafkaConfig 的动态配置变更。此操作必须在 authorizer 和 quotaManagers 初始化之后进行，因为会引用这些对象。
      // 必须在安装 DynamicClientQuotaPublisher 之前调用，否则可能会错过启动时生效的动态配置的初始更新。
      config.dynamicConfig.addReconfigurables(this)

      // Set up the client quotas publisher. This will enable controller mutation quotas and any
      // other quotas which are applicable.
      metadataPublishers.add(new DynamicClientQuotaPublisher(
        config,
        sharedServer.metadataPublishingFaultHandler,
        "controller",
        clientQuotaMetadataManager
      ))

      // Set up the DynamicTopicClusterQuotaPublisher. This will enable quotas for the cluster and topics.
      metadataPublishers.add(new DynamicTopicClusterQuotaPublisher(
        clusterId,
        config,
        sharedServer.metadataPublishingFaultHandler,
        "controller",
        quotaManagers,
      ))

      // Set up the SCRAM publisher.
      metadataPublishers.add(new ScramPublisher(
        config,
        sharedServer.metadataPublishingFaultHandler,
        "controller",
        credentialProvider
      ))

      // Set up the DelegationToken publisher.
      // We need a tokenManager for the Publisher
      // The tokenCache in the tokenManager is the same used in DelegationTokenControlManager
      metadataPublishers.add(new DelegationTokenPublisher(
          config,
          sharedServer.metadataPublishingFaultHandler,
          "controller",
          new DelegationTokenManager(delegationTokenManagerConfigs, tokenCache)
      ))

      // Set up the metrics publisher.
      metadataPublishers.add(new ControllerMetadataMetricsPublisher(
        sharedServer.controllerServerMetrics,
        sharedServer.metadataPublishingFaultHandler
      ))

      // Set up the ACL publisher.
      metadataPublishers.add(new AclPublisher(
        config.nodeId,
        sharedServer.metadataPublishingFaultHandler,
        "controller",
        authorizerPlugin.toJava
      ))

      // Install all metadata publishers.
      FutureUtils.waitWithLogging(logger.underlying, logIdent, // 安装所有元数据发布器
        "the controller metadata publishers to be installed",
        sharedServer.loader.installPublishers(metadataPublishers), startupDeadline, time)

      val authorizerFutures: Map[Endpoint, CompletableFuture[Void]] = endpointReadyFutures.futures().asScala.toMap // 获取授权器Future映射

      /**
       * Enable the controller endpoint(s). If we are using an authorizer which stores
       * ACLs in the metadata log, such as StandardAuthorizer, we will be able to start
       * accepting requests from principals included super.users right after this point,
       * but we will not be able to process requests from non-superusers until AclPublisher
       * publishes metadata from the QuorumController. MetadataPublishers do not publish
       * metadata until the controller has caught up to the high watermark.
       */
      val socketServerFuture = socketServer.enableRequestProcessing(authorizerFutures) // 启用控制器端点请求处理

      /**
       * Start the KIP-919 controller registration manager.
       */
      val controllerNodeProvider = RaftControllerNodeProvider(raftManager, config) // 创建控制器节点提供器
      registrationChannelManager = new NodeToControllerChannelManagerImpl( // 创建节点到控制器通道管理器
        controllerNodeProvider,
        time,
        metrics,
        config,
        "registration",
        s"controller-${config.nodeId}-",
        5000)
      registrationChannelManager.start() // 启动注册通道管理器
      registrationManager.start(registrationChannelManager) // 启动注册管理器

      // Block here until all the authorizer futures are complete
      FutureUtils.waitWithLogging(logger.underlying, logIdent, // 等待所有授权器Future完成
        "all of the authorizer futures to be completed",
        CompletableFuture.allOf(authorizerFutures.values.toSeq: _*), startupDeadline, time)

      // Wait for all the SocketServer ports to be open, and the Acceptors to be started.
      FutureUtils.waitWithLogging(logger.underlying, logIdent, // 等待所有SocketServer端口打开和接受器启动
        "all of the SocketServer Acceptors to be started",
        socketServerFuture, startupDeadline, time)
    } catch {
      case e: Throwable => // 捕获异常时的处理
        maybeChangeStatus(STARTING, STARTED) // 更改状态
        sharedServer.controllerStartupFaultHandler.handleFault("caught exception", e) // 处理启动故障
        shutdown() // 关闭服务
        throw e // 重新抛出异常
    }
  }

  def shutdown(): Unit = {
    if (!maybeChangeStatus(STARTED, SHUTTING_DOWN)) return
    try {
      info("shutting down")
      // Ensure that we're not the Raft leader prior to shutting down our socket server, for a
      // smoother transition.
      sharedServer.ensureNotRaftLeader()
      incarnationId = null
      Utils.closeQuietly(registrationManager, "registration manager")
      registrationManager = null
      if (registrationChannelManager != null) {
        CoreUtils.swallow(registrationChannelManager.shutdown(), this)
        registrationChannelManager = null
      }
      metadataPublishers.forEach(p => sharedServer.loader.removeAndClosePublisher(p).get())
      metadataPublishers.clear()
      if (metadataCache != null) {
        metadataCache = null
      }
      Utils.closeQuietly(metadataCachePublisher, "metadata cache publisher")
      metadataCachePublisher = null
      Utils.closeQuietly(featuresPublisher, "features publisher")
      featuresPublisher = null
      Utils.closeQuietly(registrationsPublisher, "registrations publisher")
      registrationsPublisher = null
      if (socketServer != null)
        CoreUtils.swallow(socketServer.stopProcessingRequests(), this)
      if (controller != null)
        controller.beginShutdown()
      if (socketServer != null)
        CoreUtils.swallow(socketServer.shutdown(), this)
      if (controllerApisHandlerPool != null)
        CoreUtils.swallow(controllerApisHandlerPool.shutdown(), this)
      if (controllerApis != null)
        CoreUtils.swallow(controllerApis.close(), this)
      if (quotaManagers != null)
        CoreUtils.swallow(quotaManagers.shutdown(), this)
      Utils.closeQuietly(controller, "controller")
      Utils.closeQuietly(quorumControllerMetrics, "quorum controller metrics")
      authorizerPlugin.foreach(Utils.closeQuietly(_, "authorizer plugin"))
      createTopicPolicy.foreach(policy => Utils.closeQuietly(policy, "create topic policy"))
      alterConfigPolicy.foreach(policy => Utils.closeQuietly(policy, "alter config policy"))
      socketServerFirstBoundPortFuture.completeExceptionally(new RuntimeException("shutting down"))
      CoreUtils.swallow(config.dynamicConfig.clear(), this)
      sharedServer.stopForController()
    } catch {
      case e: Throwable =>
        fatal("Fatal error during controller shutdown.", e)
        throw e
    } finally {
      maybeChangeStatus(SHUTTING_DOWN, SHUTDOWN)
    }
  }

  def awaitShutdown(): Unit = {
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
}
