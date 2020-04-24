package com.tf.tfserversparkscala.app

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/**
  * hive 程序入口
  */
object HiveModelApp {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val hiveSession = SparkSession.builder()
      .appName("Hive_HbaseTest")
      .enableHiveSupport()
      .master("local[*]")
      .config("spark.sql.parquet.writeLegacyFormat", true)
      .getOrCreate()
    //本地测试配置hadoop地址，提交集群需要关闭此条内容
    System.setProperty("HADOOP_USER_NAME", "root")

    hiveSession.sql("use default")
    val sc = hiveSession.sparkContext



    sc.stop()
    hiveSession.close()
  }
}
