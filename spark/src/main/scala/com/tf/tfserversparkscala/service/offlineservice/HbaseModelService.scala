package com.tf.tfserversparkscala.service.offlineservice

import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * 设计具体服务
  */
object HbaseModelService {
  def process(spark: SparkSession,
              sc: SparkContext,
              regData: Broadcast[scala.collection.mutable.HashMap[String, scala.collection.mutable.ArrayBuffer[Tuple3[String, String, String]]]],
              hbaseRDD: RDD[(ImmutableBytesWritable, Result)],
              tableName: String): Unit = {

    //todo



  }


}
