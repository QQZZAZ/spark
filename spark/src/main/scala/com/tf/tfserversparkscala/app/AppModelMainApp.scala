package com.tf.tfserversparkscala.app

import com.tf.tfserversparkscala.common.tfserversparkscala.common.utils.hbase.ConnectHbaseUtils
import com.tf.tfserversparkscala.config.EnumUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * 程序入口
  */
object AppModelMainApp {
  def main(args: Array[String]): Unit = {
    //开启日志级别
    Logger.getLogger("org").setLevel(Level.ERROR)

    //创建链接
    val spark = SparkSession.builder()
      .appName("AppModelMain")
      .master("local[*]")
      .config("redis.host", EnumUtil.REDISCLUSTERURL)
      .config("redis.port", EnumUtil.REDISCLUSTERPOST)
      .config("redis.timeout", "100000")
      .getOrCreate()
    val sc = spark.sparkContext
    val conf = ConnectHbaseUtils.hbase_conf("表名", EnumUtil.HBASE_ZOOKEEPER_IP, EnumUtil.HBASE_ZOOKEEPER_PORT)
    val hbaseRDD = ConnectHbaseUtils.create_Initial_RDD(sc, conf)
    //调用servie具体实现类


    //关闭链接
    spark.close()
  }
}
