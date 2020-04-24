package com.tf.tfserversparkscala.common.tfserversparkscala.common.utils.redis

import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import redis.clients.jedis.Jedis



/**
  * 此类进档开发人员写集合数据到redis的样板
  */
@Deprecated
object ConnectRedisUtils {

  /**
    * 批量想redis写入dataframe
    * @param sc
    * @param dataframe 数据
    * @param column 字段
    * @param listKey
    */
  def rdd2RedisList(sc: SparkContext,
                    dataframe: DataFrame,
                    column: String,
                    listKey: String
                   ): Unit = {
    import com.redislabs.provider.redis._
    sc.toRedisLIST(
      dataframe.rdd.map(f => {
        val rowkey = f.getAs[String](0)
        //传输rowkey和column
        rowkey + "|||||" + column
        //传输Json数据格式
        //"{\"rowkey\":" + "\"" + rowkey + "\"" + "," + "\"table\":\"" + tableNames + "\"}"
      }), listKey
    )
  }


  /**
    * 获取redis的结合数据
    *
    * @param key
    * @param start
    * @param end
    * @return
    */
  def getListFromRedis(jedis: Jedis,
                       key: String,
                       start: Int,
                       end: Int
                      ) = {
    val value = jedis.lrange(key, start, end)
    value
  }


}
