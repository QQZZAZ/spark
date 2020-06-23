package com.tf.tfserversparkscala.service.onlineservice

import com.tf.tfserversparkscala.common.datasources.redis.{RedisDBClusterManager, RedisDBManager}
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.{ForeachWriter, Row}
import redis.clients.jedis.Jedis

/**
  * 实时流连接Redis单机和集群模式
  * 如果不需要手动指定IP和端口
  * 给默认值
  * 继承此类之后实现process函数，开发逻辑即可
  *
  * 连接集群需输入集群的ip+端口的字符串后台会进行自动拆分，以，号分割
  * localhost:6379:testredis,localhost:6380:testredis,localhost:6381:testredis
  *
  * 如果想要手动指定参数，需要自己实现open，process和close方法
  *
  * @param redisIp
  * @param redisPort
  * @param clusterOrNot 是否是集群模式 默认单机，false 为连接集群
  */
abstract class RedisSink(redisIp: String = "",
                         redisPort: Int = -1,
                         clusterOrNot: Boolean = true
                        ) extends ForeachWriter[Row] {
  var jedis: Jedis = _


  //创建连接
  override def open(partitionId: Long, version: Long): Boolean = {
    if (clusterOrNot) {
      //连接单机版redis
      jedis = RedisDBManager.getMDBManager(redisIp, redisPort).getConnections()
    } else if (!clusterOrNot) {
      //连接redis集群
      require(redisPort == -1)
      jedis = RedisDBClusterManager.getMDBManager(redisIp).getConnections()
    }
    true
  }

  //业务逻辑
  override def process(value: Row): Unit = {
  }

  // 关闭连接
  override def close(errorOrNull: Throwable): Unit = {
    if (jedis != null) {
      jedis.close()
    }
  }
}
