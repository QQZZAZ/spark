package com.tf.tfserversparkscala.app

import com.tf.tfserversparkscala.config.EnumUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.sql._

object ElasticSearchApp {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val esSession = SparkSession.builder()
      .appName("ElasticSearchApp")
      .master("local[*]")
      .config("es.nodes", EnumUtil.ELASTICSEARCHCLUSTERURL)
      .config("es.port", EnumUtil.ELASTICSEARCHCLUSTERPORT)
      .config("es.index.auto.create", true)
      .getOrCreate()

    //指定查询条件
    val query =
      s"""
         |{
         |  "query": {
         |    "match_all": {}
         |  }
         |}
         """.stripMargin

    val esDf = esSession.esDF("/index/type", query)
    //todo


    esDf.saveToEs("/index/type")
    esSession.close()
  }
}
