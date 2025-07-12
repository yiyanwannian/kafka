SocketServer

KafkaApis

GroupCoordinator

ReplicaManager

LogManager

Partitions

KRaft


Broker由以下九个基本模块组成：SocketServer（监听Socket请求）、KafkaRequestHandler-Pool（请求处理资源池）、LogManager（日志管理）、ReplicaManager（分区副本管理）、OffsetManager（偏移量管理）、KafkaScheduler（后台任务调度资源池）、KafkaApis（业务逻辑实现层）、KafkaHealthcheck（提供Broker健康状态）、TopicConfigManager（Topic配置信息管理）