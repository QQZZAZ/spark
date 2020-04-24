package com.tf.tfserversparkscala.service.offlineservice

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object ElasticSearchService {
  def process(esDf:DataFrame,sparkSession: SparkSession,sparkContext: SparkContext): Unit ={
    // TODO:
  }
}
