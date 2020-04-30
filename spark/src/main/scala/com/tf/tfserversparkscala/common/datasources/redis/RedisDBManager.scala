package com.tf.tfserversparkscala.common.datasources.redis

import java.util

import com.tf.tfserversparkscala.config.EnumUtil
import org.apache.commons.lang3.StringUtils
import redis.clients.jedis._

import scala.collection.mutable

/**
  * 单机redis连接池
  *
  * @param jedisAddress
  * @param port
  */
class RedisDBManager(jedisAddress: String, port: Int) extends Serializable {
  //连接配置
  val config = new JedisPoolConfig
  //最大连接数
  config.setMaxTotal(20)
  //最大空闲连接数
  config.setMaxIdle(10)

  //设置连接池属性分别有： 配置  主机名，端口号，连接超时时间，Redis密码
  val pool = jedisAddress match {
    case "" => new JedisPool(config, EnumUtil.REDISURL, EnumUtil.REDISPORT, 10000)
    case _ => new JedisPool(config, jedisAddress, port, 10000)
  }

  //连接池获取连接
  def getConnections() = {
    pool.getResource
  }
}

object RedisDBManager {
  private var redisdbmanager: RedisDBManager = _

  //动态获取jedis连接池，不传参则默认创建配置文件中的连接地址
  def getMDBManager(jedisAddress: String, port: Int): RedisDBManager = {
    if (redisdbmanager == null) {
      this.synchronized {
        if (redisdbmanager == null) {
          redisdbmanager = new RedisDBManager(jedisAddress, port)
        }
      }
    }

    redisdbmanager
  }
}
