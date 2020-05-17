package com.tf.tfserversparkscala.app

import java.sql.Timestamp

import com.tf.tfserversparkscala.common.datasources.kafka.KakfaUtils
import com.tf.tfserversparkscala.common.spark.TFSparkSesssion
import com.tf.tfserversparkscala.entity.publiccaseclass.online.Event
import com.tf.tfserversparkscala.config.EnumUtil
import com.tf.tfserversparkscala.service.onlineservice.HbaseSink
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{Dataset, ForeachWriter, Row, SparkSession}
import org.json.JSONObject


/**
  * 实时流读取kafka样板
  */
object StreamingModelApp {

  /** User-defined data type representing the input events */

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    //建立spark会话
    val spark: SparkSession = TFSparkSesssion.createSparkLocalSession(this.getClass.getName)
    //创建kafka连接,需传入kafka ZK地址，topic和每次拉取kafka的数据总量
    val events: Dataset[(String, Timestamp)] = KakfaUtils.getKafkaStreamingData(spark, EnumUtil.KAFKA_ZOOKEEPER_URL, EnumUtil.KAFKA_TOPIC, 4800)
    //todo 处理逻辑


    //启动流
    /*TFSparkSesssion.sinkSession(events,
      "append",
      EnumUtil.HDFSCHECKPOINT,
      "",
      new HbaseSink())*/
  }
}
