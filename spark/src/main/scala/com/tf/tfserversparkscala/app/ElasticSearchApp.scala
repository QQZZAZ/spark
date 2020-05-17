package com.tf.tfserversparkscala.app

import java.text.SimpleDateFormat
import java.util.Date

import com.tf.tfserversparkscala.config.EnumUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.elasticsearch.spark.sql._

object ElasticSearchApp {

  case class EsData(id: Int, date: String, title: String, content: String) extends Serializable

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val esSession = SparkSession.builder()
      .appName("ElasticSearchApp")
      .master("local[*]")
      .config("es.nodes", EnumUtil.ELASTICSEARCHCLUSTERURL)
      .config("es.port", EnumUtil.ELASTICSEARCHCLUSTERPORT)
      .config("es.index.auto.create", true)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", "com.tf.tfserversparkscala.common.kryo.DemoRegistrator")
      .config("es.mapping.date.rich",false)
      .getOrCreate()

    //匹配所有数据
    val query =
      s"""
         | {
         |  "query":{
         |    "match_all":{}
         |   }
         | }
         """.stripMargin

    //根据分词索引查询
    val query2 =
      s"""
         | {
         |  "query":{
         |    "term":{"content":"国家"}
         |   }
         | }
         """.stripMargin

    val esDf = esSession.esDF("/test/article", query2)
    //todo

   /* val list = List(
      EsData(1003,
        "2020-5-16 01:10:00",
        "标题c",
        "最近一段时间，全球多个国家都笼罩在新冠病毒的阴霾之下，民众不能正常上班，出门全都戴着口罩。到现在还没有找到病毒的源头，导致不能完全地根治新冠病毒，防患于未然。美国作为一个大国来讲，在此次疫情中表现非常糟糕，简直有损大国风范，但是最后也自食其果。截至目前全球疫情的情况而言，美国的新冠病毒感染率直接上升至了全球第一，逝世的人数也还在增加中。")
    )
    val sc = esSession.sparkContext
    import esSession.implicits._

    val df = sc.parallelize(list)
      .toDF("id", "posttime", "title", "content")*/
    //    df.saveToEs("/test/article")

    esDf.show()
    esDf.count()
    esSession.close()
  }
}
