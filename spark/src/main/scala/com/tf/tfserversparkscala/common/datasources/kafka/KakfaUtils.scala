package com.tf.tfserversparkscala.common.datasources.kafka

import java.sql.Timestamp

import com.tf.tfserversparkscala.config.EnumUtil
import org.apache.spark.sql.SparkSession

import org.apache.commons.lang3.StringUtils

object KakfaUtils {
  /**
    * @param spark
    * @param servers   kafka bootstrap地址
    * @param subscribe kafka的topic
    * @return
    */
  private var kafkaBootstrap: String = _
  private var topic: String = _

  /**
    * 静态获取配置文件中的kafka地址和topic
    *
    * @param spark
    * @param pollSize 一次从kafka读取的最大数据
    */
  def getKafkaStreamingData(spark: SparkSession, pollSize: Int) = {
    kafkaBootstrap = EnumUtil.KAFKA_ZOOKEEPER_URL
    topic = EnumUtil.KAFKA_TOPIC
    createSession(spark, pollSize)
  }

  /**
    * 动态获取kafka的地址和topic
    *
    * @param spark
    * @param servers   kafka的zookeeper地址
    * @param subscribe kafka的topic
    * @param pollSize  一次从kafka读取的最大数据
    */
  def getKafkaStreamingData(spark: SparkSession,
                            servers: String,
                            subscribe: String,
                            pollSize: Int
                           ) = {
    // Create DataFrame representing the stream of input lines from connection to host:port
    if (StringUtils.isBlank(servers)) {
      kafkaBootstrap = EnumUtil.KAFKA_ZOOKEEPER_URL
    } else {
      kafkaBootstrap = servers
    }

    if (StringUtils.isBlank(subscribe)) {
      topic = EnumUtil.KAFKA_TOPIC
    } else {
      topic = subscribe
    }
    createSession(spark, pollSize)
  }

  /**
    * 建立kafka 会话
    *
    * @param spark
    * @param pollSize
    * @return 数据集 结构为kafak的value字段及时间戳
    */
  private def createSession(spark: SparkSession, pollSize: Int) = {
    import spark.implicits._
    val lines = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrap)
      .option("subscribe", topic)
      .option("includeTimestamp", true)
      .option("failOnDataLoss", false)
      .option("max.poll.records", pollSize)
      .load()

    val events = lines
      .select("value", "timestamp")
      .selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
      .as[(String, Timestamp)]
      .map(f => f)
    events
  }
}
