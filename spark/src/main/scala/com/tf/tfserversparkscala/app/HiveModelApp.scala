package com.tf.tfserversparkscala.app

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * hive 程序入口
  */
object HiveModelApp {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("HiveModelApp").setLevel(Level.ERROR)

    val hiveSession = SparkSession.builder()
      .appName("Hive_HbaseTest")
      .enableHiveSupport()
      .master("local[*]")
      .config("spark.sql.parquet.writeLegacyFormat", true)
      .config("spark.sql.hive.convertMetastoreParquet",true)
      .config("spark.io.compression.codec","snappy")
      .config("spark.rdd.compress",true)
      //lzo,gzip,snappy,none  默认是snappy
      .config("spark.sql.parquet.compression.codec","lzo")
      //orc
      //默认压缩格式：snappy
      //可用压缩格式：none, snappy, zlib, lzo
//      .config("spark.sql.orc.compression.codec","lzo")
      .getOrCreate()
    //本地测试配置hadoop地址，提交集群需要关闭此条内容
    System.setProperty("HADOOP_USER_NAME", "root")

//生产环境开启，刷新hive的表名的元数据信息
//    hiveSession.catalog.refreshByPath(s"${dbname.tablename}")

    hiveSession.sql("use default")
    val sc = hiveSession.sparkContext



    sc.stop()
    hiveSession.close()
  }
}
