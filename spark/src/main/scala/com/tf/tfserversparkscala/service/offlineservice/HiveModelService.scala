package com.tf.tfserversparkscala.service.offlineservice

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object HiveModelService {

  /**
    * 设计sql服务
    *
    * @param hive
    */
  def process(hive: SparkSession, sparkContext: SparkContext): Unit = {
    //todo
    hive.sql("")

  }


}
