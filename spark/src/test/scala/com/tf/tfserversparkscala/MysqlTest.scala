package com.tf.tfserversparkscala

import com.tf.tfserversparkscala.config.EnumUtil
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.HashMap

object MysqlTest {
  lazy val url = EnumUtil.MYSQL_URL
  lazy val username = EnumUtil.MYSQL_USERNAME
  lazy val password = EnumUtil.MYSQL_PASSWORD

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("dataframe").getOrCreate()

    val options = new HashMap[String, String]
    options.put("url", url)
    options.put("user", username)
    options.put("password", password)
    options.put("dbtable", "stop_words")
    val studentInfosDF: DataFrame = spark.read.format("jdbc").options(options).load()
    val rdd = studentInfosDF.collect().toList.sortWith(_.get(2).toString.length > _.get(2).toString.length)
    val list = new scala.collection.mutable.ArrayBuffer[(String,String)]()

    //从数据库获取的数据，无法在driver上保存，需要通过collect()拉取到driver本地，
    // 否则在运算结束之后，调用隐式函数，清空内存，除print外，所有的操作全部失效
    //所有非RDD对象，都是保存到driver中
    rdd.foreach(x =>{
      list += Tuple2[String,String](x.get(1).toString,x.get(2).toString)
    })
  }
}
