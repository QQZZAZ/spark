package com.tf.tfserversparkscala.common.spark

import java.sql.Timestamp

import org.apache.spark.sql.{Dataset, ForeachWriter, Row, SparkSession}

object TFSparkSesssion {

  //创建本地模式session，适合测试
  def createSparkLocalSession(appName: String) = {
    val spark = SparkSession
      .builder
      .appName("StructuredSessionization")
      .config("spark.speculation",true)
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .master("local[*]")
      .getOrCreate()
    spark
  }
  //创建集群模式session，生产环境
  def createSparkClusterSession(appName: String) = {
    val spark = SparkSession
      .builder
      .appName(appName)
      .config("spark.speculation",true)
      .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    spark
  }

  /**
    * 创建启动实时流的session触发器
    * @param events 数据对象
    * @param outputMode 输出方式
    * append 追加
    * update 聚合更新
    * complete 有状态计算时使用
    * @param checkpointLocation kafka的checkpoint保存地址
    * @param format 输出方式
    * 文件方式
    * kafka
    * 内存方式
    * 控制台
    * @param sink 第三方组件输出方式
    * hbase，redis等
    */
  def sinkSession(events: Dataset[(String, Timestamp)],
                  outputMode: String,
                  checkpointLocation: String,
                  format: String,
                  sink: ForeachWriter[Row]
                 ): Unit = {

    val query = events
      .writeStream
      .outputMode("append")
      //检查点必须设置，不然会报错
      .option("checkpointLocation", "hdfs://")
      .format("console")
      .start()
    query.awaitTermination()
  }
}
