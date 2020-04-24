package com.tf.tfserversparkscala.common.datasources.redis

import java.util

import com.tf.tfserversparkscala.config.EnumUtil
import org.apache.commons.lang3.StringUtils
import redis.clients.jedis._

import scala.collection.mutable

/**
  * Redis集群连接方式
  * 集群地址格式：服务器，地址和密码
  * localhost:6379:testredis,localhost:6380:testredis,localhost:6381:testredis
  * 传空串则默认单机redis
  *
  * @param jedisClusterAddress
  */
class RedisDBClusterManager(jedisClusterAddress: String) {
  //连接配置
  val config = new JedisPoolConfig
  //最大连接数
  config.setMaxTotal(20)
  //最大空闲连接数
  config.setMaxIdle(10)

  def init(host: String) = {
    if (StringUtils.isBlank(host)) {
      throw new NullPointerException("redis host not found");
    }
    val paramter = new mutable.HashSet[String]();
    val sentinelArray = host.split(",");
    for (str <- sentinelArray) {
      paramter.add(str)
    }
    paramter
  }

  //设置连接池属性分别有： 配置  主机名，端口号，连接超时时间，Redis密码
  val pool = jedisClusterAddress match {
    case "" => new JedisPool(config, EnumUtil.REDISURL, EnumUtil.REDISPORT, 10000, "")
    case _ => {
      val shards = new util.ArrayList[JedisShardInfo]()
      val host = jedisClusterAddress
      val hosts = init(host)
      for (hs <- hosts) {
        val values = hs.split(":")
        val shard = new JedisShardInfo(values(0), values(1).toInt)
        if (values.length > 2) {
          shard.setPassword(values(2))
        }
        shards.add(shard)
      }
      new ShardedJedisPool(config, shards)
    }
  }

  //连接池获取连接
  def getConnections() = {
    pool.getResource.asInstanceOf[Jedis]
  }
}

//单例对象
object RedisDBClusterManager {
  private var redisdbmanager: RedisDBClusterManager = _

  //动态获取jedis连接池，不传参则默认创建配置文件中的连接地址
  def getMDBManager(jedisClusterAddress: String): RedisDBClusterManager = {
    if (redisdbmanager == null) {
      this.synchronized {
        if (redisdbmanager == null) {
          redisdbmanager = new RedisDBClusterManager(jedisClusterAddress)
        }
      }
    }

    redisdbmanager
  }

}
