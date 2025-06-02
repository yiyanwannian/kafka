# Kafka MetadataImage 包含信息详细清单

## 概述

MetadataImage是Kafka集群元数据的完整快照，包含了集群运行所需的所有信息。本文档详细列出了每个组件包含的具体信息。

## 1. MetadataProvenance - 元数据来源信息

```java
public final class MetadataProvenance {
    private final long lastContainedOffset;      // 最后包含的日志偏移量
    private final int lastContainedEpoch;        // 最后包含的领导者纪元
    private final long lastContainedLogTimeMs;   // 最后包含的日志时间戳
    private final boolean isOffsetBatchAligned;  // 偏移量是否批次对齐
}
```

**包含信息：**
- **版本标识**：确定元数据的版本和新旧程度
- **时间戳**：记录元数据的生成时间
- **对齐标识**：用于快照创建和恢复优化

## 2. FeaturesImage - 集群特性版本

```java
public final class FeaturesImage {
    private final Map<String, Short> finalizedVersions;        // 已确定的特性版本
    private final Optional<MetadataVersion> metadataVersion;   // 元数据版本
}
```

**包含的特性信息：**
- **metadata.version**：元数据格式版本
- **kraft.version**：KRaft协议版本
- **eligible.leader.replicas**：ELR特性版本
- **group.version**：消费者组协调器版本
- **transaction.version**：事务协调器版本
- **自定义特性**：用户定义的特性版本

**示例数据：**
```json
{
  "metadata.version": 19,
  "kraft.version": 1,
  "eligible.leader.replicas": 1,
  "group.version": 1
}
```

## 3. ClusterImage - 集群拓扑信息

```java
public final class ClusterImage {
    private final Map<Integer, BrokerRegistration> brokers;      // Broker注册信息
    private final Map<Integer, ControllerRegistration> controllers;  // 控制器注册信息
}
```

### 3.1 BrokerRegistration - Broker注册信息

```java
public class BrokerRegistration {
    private final int id;                                    // Broker ID
    private final long epoch;                               // Broker纪元
    private final Uuid incarnationId;                       // 实例化ID
    private final Map<String, Endpoint> listeners;          // 监听器端点
    private final Map<String, VersionRange> supportedFeatures;  // 支持的特性
    private final Optional<String> rack;                    // 机架信息
    private final boolean fenced;                           // 是否被围栏
    private final boolean inControlledShutdown;             // 是否在受控关闭中
    private final boolean isMigratingZkBroker;             // 是否为ZK迁移Broker
    private final List<Uuid> directories;                   // 日志目录
}
```

**Broker信息详情：**
- **基本信息**：ID、纪元、实例化ID
- **网络配置**：监听器地址和端口
- **特性支持**：支持的协议版本范围
- **物理位置**：机架信息（用于副本分配）
- **状态标识**：围栏状态、关闭状态
- **存储信息**：可用的日志目录

**示例Broker信息：**
```json
{
  "id": 1,
  "epoch": 1001,
  "incarnationId": "U52uRe20RsGI0RvpcTx33Q",
  "listeners": {
    "PLAINTEXT": {
      "host": "localhost",
      "port": 9093,
      "securityProtocol": "PLAINTEXT"
    }
  },
  "supportedFeatures": {
    "kraft.version": "0-1"
  },
  "rack": "rack-1",
  "fenced": false,
  "inControlledShutdown": false,
  "directories": ["dir1-uuid", "dir2-uuid"]
}
```

### 3.2 ControllerRegistration - 控制器注册信息

```java
public class ControllerRegistration {
    private final int id;                                    // 控制器ID
    private final Uuid incarnationId;                       // 实例化ID
    private final boolean zkMigrationReady;                 // ZK迁移就绪状态
    private final Map<String, Endpoint> listeners;          // 监听器端点
    private final Map<String, VersionRange> supportedFeatures;  // 支持的特性
}
```

**控制器信息详情：**
- **标识信息**：控制器ID和实例化ID
- **网络配置**：控制器监听地址
- **迁移状态**：ZooKeeper迁移准备状态
- **特性支持**：支持的协议版本

## 4. TopicsImage - 主题和分区信息

```java
public final class TopicsImage {
    private final ImmutableMap<Uuid, TopicImage> topicsById;    // 按ID索引的主题
    private final ImmutableMap<String, TopicImage> topicsByName; // 按名称索引的主题
}
```

### 4.1 TopicImage - 单个主题信息

```java
public final class TopicImage {
    private final String name;                               // 主题名称
    private final Uuid id;                                  // 主题ID
    private final Map<Integer, PartitionRegistration> partitions; // 分区信息
}
```

### 4.2 PartitionRegistration - 分区注册信息

```java
public class PartitionRegistration {
    private final int[] replicas;                           // 副本列表
    private final Uuid[] directories;                       // 副本目录
    private final int[] isr;                                // 同步副本集合
    private final int[] removingReplicas;                   // 正在移除的副本
    private final int[] addingReplicas;                     // 正在添加的副本
    private final int[] elr;                                // 符合条件的领导者副本
    private final int[] lastKnownElr;                       // 最后已知的ELR
    private final Integer leader;                           // 当前领导者
    private final LeaderRecoveryState leaderRecoveryState; // 领导者恢复状态
    private final Integer leaderEpoch;                     // 领导者纪元
    private final Integer partitionEpoch;                  // 分区纪元
}
```

**分区信息详情：**
- **副本配置**：副本分配、ISR状态
- **领导者信息**：当前领导者、纪元信息
- **重分配状态**：正在进行的副本重分配
- **恢复状态**：分区恢复和ELR状态
- **存储位置**：副本在各个目录中的位置

**示例主题信息：**
```json
{
  "name": "test-topic",
  "id": "xtzWWN4bTjitpL3kfd9s5w",
  "partitions": {
    "0": {
      "replicas": [1, 2, 3],
      "isr": [1, 2],
      "leader": 1,
      "leaderEpoch": 5,
      "partitionEpoch": 10
    }
  }
}
```

## 5. ConfigurationsImage - 配置信息

```java
public final class ConfigurationsImage {
    private final Map<ConfigResource, ConfigurationImage> data; // 配置资源映射
}
```

### 5.1 ConfigResource - 配置资源类型

**支持的配置资源类型：**
- **BROKER**：Broker级别配置
- **TOPIC**：主题级别配置
- **CLIENT_METRICS**：客户端指标配置
- **USER**：用户级别配置
- **IP**：IP级别配置

### 5.2 ConfigurationImage - 配置值

```java
public final class ConfigurationImage {
    private final Map<String, String> data; // 配置键值对
}
```

**示例配置信息：**
```json
{
  "BROKER:1": {
    "num.network.threads": "8",
    "num.io.threads": "8",
    "log.retention.hours": "168"
  },
  "TOPIC:test-topic": {
    "cleanup.policy": "delete",
    "retention.ms": "604800000",
    "segment.ms": "86400000"
  }
}
```

## 6. ClientQuotasImage - 客户端配额

```java
public final class ClientQuotasImage {
    private final Map<ClientQuotaEntity, ClientQuotaImage> entities; // 配额实体映射
}
```

### 6.1 ClientQuotaEntity - 配额实体

**配额实体类型：**
- **用户配额**：基于用户名的配额
- **客户端配额**：基于客户端ID的配额
- **用户+客户端配额**：组合配额
- **IP配额**：基于IP地址的配额

### 6.2 ClientQuotaImage - 配额值

**配额类型：**
- **producer_byte_rate**：生产者字节速率
- **consumer_byte_rate**：消费者字节速率
- **request_percentage**：请求百分比
- **controller_mutation_rate**：控制器变更速率

**示例配额信息：**
```json
{
  "user:alice": {
    "producer_byte_rate": 1048576,
    "consumer_byte_rate": 2097152
  },
  "client-id:my-app": {
    "request_percentage": 50
  }
}
```

## 7. AclsImage - 访问控制列表

```java
public final class AclsImage {
    private final Map<Uuid, StandardAcl> acls; // ACL规则映射
}
```

### 7.1 StandardAcl - ACL规则

**ACL规则组成：**
- **资源类型**：TOPIC、GROUP、CLUSTER等
- **资源名称**：具体的资源名称或模式
- **主体**：用户或服务账号
- **操作**：READ、WRITE、CREATE等
- **权限类型**：ALLOW或DENY
- **主机**：允许的主机地址

**示例ACL信息：**
```json
{
  "acl-uuid-1": {
    "resourceType": "TOPIC",
    "resourceName": "test-topic",
    "principal": "User:alice",
    "operation": "READ",
    "permissionType": "ALLOW",
    "host": "*"
  }
}
```

## 8. ScramImage - SCRAM认证信息

```java
public final class ScramImage {
    private final Map<ScramMechanism, Map<String, ScramCredentialData>> mechanisms;
}
```

### 8.1 SCRAM机制和凭证

**支持的SCRAM机制：**
- **SCRAM-SHA-256**
- **SCRAM-SHA-512**

**凭证信息：**
- **用户名**：认证用户名
- **盐值**：随机盐值
- **存储密钥**：服务器存储的密钥
- **迭代次数**：PBKDF2迭代次数

## 9. DelegationTokenImage - 委托令牌

```java
public final class DelegationTokenImage {
    private final Map<String, DelegationTokenData> tokens; // 令牌ID到令牌数据的映射
}
```

### 9.1 DelegationTokenData - 令牌数据

**令牌信息：**
- **令牌ID**：唯一标识符
- **所有者**：令牌所有者
- **续期者**：可以续期的用户列表
- **发行时间**：令牌发行时间
- **过期时间**：令牌过期时间
- **最大生命周期**：令牌最大有效期

## 10. ProducerIdsImage - 生产者ID管理

```java
public final class ProducerIdsImage {
    private final long nextProducerId; // 下一个可用的生产者ID
}
```

**生产者ID信息：**
- **下一个ID**：下一个分配的生产者ID
- **ID范围**：用于幂等性和事务性生产者
- **分配策略**：ID的分配和回收机制

## 总结

MetadataImage包含了Kafka集群的完整状态信息：

1. **集群拓扑**：所有broker和控制器的注册信息
2. **主题分区**：所有主题的分区分配和状态
3. **配置管理**：各级别的配置参数
4. **安全信息**：ACL规则、认证凭证、委托令牌
5. **配额控制**：客户端配额限制
6. **特性版本**：集群支持的特性版本
7. **生产者管理**：生产者ID分配状态
8. **版本信息**：元数据的版本和来源

这些信息共同构成了Kafka集群运行所需的完整元数据视图，确保集群的正确运行和数据一致性。

## 数据量级和性能考虑

### 典型数据量级

| 组件 | 小型集群 | 中型集群 | 大型集群 | 超大集群 |
|------|----------|----------|----------|----------|
| Brokers | 3-10个 | 10-50个 | 50-200个 | 200+个 |
| Topics | 10-100个 | 100-1000个 | 1000-5000个 | 5000+个 |
| Partitions | 100-1000个 | 1000-10000个 | 10000-50000个 | 50000+个 |
| Configs | 10-50个 | 50-200个 | 200-500个 | 500+个 |
| ACLs | 5-20个 | 20-100个 | 100-500个 | 500+个 |
| Users | 5-20个 | 20-100个 | 100-500个 | 500+个 |

### 内存占用估算

**MetadataImage内存占用主要因素：**

1. **TopicsImage**（占比最大，约60-80%）
   - 每个分区约200-500字节
   - 包含副本列表、ISR、领导者信息等

2. **ClusterImage**（占比约10-20%）
   - 每个Broker约1-2KB
   - 包含端点、特性、目录信息等

3. **ConfigurationsImage**（占比约5-10%）
   - 每个配置项约50-100字节
   - 配置值的长度影响占用

4. **安全组件**（占比约5-10%）
   - ACL规则、用户凭证、令牌信息

**内存占用估算公式：**
```
总内存 ≈ 分区数 × 300字节 + Broker数 × 1.5KB + 配置数 × 75字节 + ACL数 × 200字节
```

### 性能优化建议

#### 1. 读取优化
- **索引结构**：TopicsImage提供按ID和按名称的双重索引
- **不可变设计**：支持无锁并发读取
- **缓存友好**：数据结构紧凑，提高缓存命中率

#### 2. 更新优化
- **增量更新**：通过Delta机制只更新变更部分
- **结构共享**：未变更的数据结构在新旧版本间共享
- **批量应用**：批量应用多个变更，减少对象创建

#### 3. 序列化优化
- **有序写入**：按依赖关系有序写入各组件
- **版本兼容**：支持不同版本间的兼容性
- **压缩支持**：快照文件支持压缩存储

## 实际应用场景

### 1. 集群状态查询
```java
// 查询Broker状态
BrokerRegistration broker = image.cluster().broker(brokerId);
boolean isFenced = broker.fenced();

// 查询主题信息
TopicImage topic = image.topics().getTopic("my-topic");
int partitionCount = topic.partitions().size();

// 查询分区领导者
PartitionRegistration partition = image.topics().getPartition(topicId, partitionId);
int leader = partition.leader();
```

### 2. 配置管理
```java
// 获取Broker配置
ConfigResource brokerResource = new ConfigResource(ConfigResource.Type.BROKER, "1");
Properties brokerConfigs = image.configs().configProperties(brokerResource);

// 获取主题配置
ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, "my-topic");
String retentionMs = image.configs().configMapForResource(topicResource).get("retention.ms");
```

### 3. 权限检查
```java
// 检查ACL权限
for (StandardAcl acl : image.acls().acls().values()) {
    if (acl.matches(resourceType, resourceName, principal, operation)) {
        return acl.permissionType() == AclPermissionType.ALLOW;
    }
}

// 检查配额限制
ClientQuotaEntity entity = new ClientQuotaEntity(Map.of("user", username));
ClientQuotaImage quota = image.clientQuotas().entities().get(entity);
Double producerRate = quota.values().get("producer_byte_rate");
```

### 4. 监控和告警
```java
// 监控集群健康状态
long fencedBrokers = image.cluster().brokers().values().stream()
    .mapToLong(broker -> broker.fenced() ? 1 : 0)
    .sum();

// 监控分区状态
long underReplicatedPartitions = image.topics().topicsById().values().stream()
    .flatMap(topic -> topic.partitions().values().stream())
    .mapToLong(partition -> partition.isr().length < partition.replicas().length ? 1 : 0)
    .sum();
```

## 故障排查指南

### 1. 常见问题诊断

**问题：MetadataImage占用内存过大**
- 检查主题和分区数量是否合理
- 检查配置项是否过多或过长
- 检查ACL规则是否冗余

**问题：元数据更新延迟**
- 检查Delta应用是否有阻塞
- 检查发布器处理是否及时
- 检查网络和序列化性能

**问题：数据不一致**
- 检查元数据版本和偏移量
- 检查快照和日志的一致性
- 检查发布器的同步状态

### 2. 调试工具

**元数据导出工具：**
```bash
# 导出完整元数据
kafka-metadata-shell.sh --snapshot /path/to/snapshot --print

# 导出特定组件
kafka-metadata-shell.sh --snapshot /path/to/snapshot --entity-type topics
```

**内存分析工具：**
```java
// 分析各组件内存占用
long topicsMemory = ObjectSizeCalculator.getObjectSize(image.topics());
long clusterMemory = ObjectSizeCalculator.getObjectSize(image.cluster());
long configsMemory = ObjectSizeCalculator.getObjectSize(image.configs());
```

## 总结

MetadataImage是Kafka KRaft架构的核心数据结构，它：

1. **全面性**：包含集群运行所需的所有元数据信息
2. **高效性**：通过优化的数据结构和算法实现高性能访问
3. **一致性**：确保集群状态的强一致性
4. **可扩展性**：支持大规模集群的元数据管理
5. **可靠性**：提供完善的错误处理和恢复机制

理解MetadataImage的结构和内容对于：
- **系统运维**：监控集群状态和性能
- **故障排查**：快速定位和解决问题
- **容量规划**：合理规划集群资源
- **功能开发**：基于元数据开发新功能

具有重要意义。
