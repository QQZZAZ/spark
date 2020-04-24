package com.tf.tfserversparkscala.common.tfserversparkscala.common.utils.mysql


import java.sql.PreparedStatement

import com.tf.tfserversparkscala.common.datasources.mysql.MDBManager
import com.tf.tfserversparkscala.config.EnumUtil
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.mutable.{HashMap, ListBuffer}

/**
  * 对mysql进行增+改
  * spark目前原生仅支持增加，覆盖，去重不支持改
  * update操作和replace操作时通过封装完成
  */
object ConnectMysqlUtils {

  /**
    * 添加过滤条件 获取 Mysql 表的数据
    *
    * @param sqlContext
    * @param table           读取Mysql表的名字
    * @param filterCondition 过滤条件
    * @param proPath         配置文件的路径
    * @return 返回 Mysql 表的 DataFrame
    */
  def readMysqlTable(sparkSession: SparkSession, table: String, filterCondition: String) = {
    val sql = s"(select * from $table where $filterCondition ) as t1"
    sparkSession
      .read
      .format("jdbc")
      .option("url", EnumUtil.MYSQL_URL)
      .option("driver", EnumUtil.MYSQL_DRIVER)
      .option("user", EnumUtil.MYSQL_USERNAME)
      .option("password", EnumUtil.MYSQL_PASSWORD)
      .option("dbtable", sql)
      .load()
  }

  /**
    * 全量读取mysql数据
    *
    * @param spark sparkSession会话
    * @param table 读取的表名
    * @return
    */
  def getMysqlData(spark: SparkSession, table: String) = {

    val options = new HashMap[String, String]
    options.put("driver", EnumUtil.MYSQL_DRIVER)
    options.put("url", EnumUtil.MYSQL_URL)
    options.put("user", EnumUtil.MYSQL_USERNAME)
    options.put("password", EnumUtil.MYSQL_PASSWORD)
    options.put("dbtable", table)

    val studentInfosDF: DataFrame = spark.read.format("jdbc").options(options).load()
    studentInfosDF
  }

  /**
    * 这种方式适用于全字段追加写入,
    * 字段无值必须给默认值
    *
    * SaveMode参数
    * Append 追加模式
    * Overwrite 覆盖模式
    * ErrorIfExists 如果数据库中有重复数据则报错
    * Ignore忽略重复数据
    *
    * @param dataFrame
    * @param dbtable
    * @param partitionNum 增加并行度
    * @SaveMode
    */
  def insertMysqlData(dataFrame: DataFrame,
                      dbtable: String,
                      partitionNum: Int,
                      saveMode: SaveMode
                     ): Unit = {
    if (partitionNum > 0) {
      dataFrame.repartition(partitionNum).write
        .mode(saveMode)
        .format("jdbc")
        .option("url", EnumUtil.MYSQL_URL)
        .option("driver", EnumUtil.MYSQL_DRIVER)
        .option("dbtable", dbtable)
        .option("user", EnumUtil.MYSQL_USERNAME)
        .option("password", EnumUtil.MYSQL_PASSWORD)
        .save()
    } else {
      dataFrame.write
        .mode(saveMode)
        .format("jdbc")
        .option("url", EnumUtil.MYSQL_URL)
        .option("driver", EnumUtil.MYSQL_DRIVER)
        .option("dbtable", dbtable)
        .option("user", EnumUtil.MYSQL_USERNAME)
        .option("password", EnumUtil.MYSQL_PASSWORD)
        .save()
    }


    /**
      * 覆盖插入数据
      * 此函数仅做行级插入模板不支持直接调用
      * 需根据业务需求定义字段
      */
    def replaceIntoMysqlData(dataList: ListBuffer[Tuple2[String, String]]): Unit = {
      //通过连接池方式获取数据库连接
      val conn = MDBManager.GetMDBManagerInstance.getConnection
      conn.setAutoCommit(false)
      var preparedStatement: PreparedStatement = null
      try {
        dataList.foreach(f => {
          val name = f._1
          val age = f._2
          val sql = s"replace into test_data.mysql_stu_info values($name,$age)"
          preparedStatement = conn.prepareStatement(sql)
          preparedStatement.addBatch()
        })
        preparedStatement.executeBatch()
      } catch {
        case exception: Exception => {
          exception.printStackTrace()
          conn.rollback()
        }
      } finally {
        conn.commit()
        conn.close()
      }
    }

  }

  /**
    * 局部更新mysql模板
    * 此函数仅做行级插入模板不支持直接调用
    * 需根据业务需求定义字段
    *
    * @param rdd
    */
  def updateMysqlDataTest(dataList: ListBuffer[Tuple2[String, String]]): Unit = {
    //通过连接池方式获取数据库连接
    val conn = MDBManager.GetMDBManagerInstance.getConnection
    conn.setAutoCommit(false)
    var preparedStatement: PreparedStatement = null
    try {
      dataList.foreach(f => {
        val name = f._1
        val age = f._2
        val sql = s"update test_data.mysql_stu_info set age = $age where name=$name"
        preparedStatement = conn.prepareStatement(sql)
        preparedStatement.addBatch()
      })
      preparedStatement.executeBatch()
    } catch {
      case exception: Exception => {
        exception.printStackTrace()
        conn.rollback()
      }
    } finally {
      conn.commit()
      conn.close()
    }
  }


  //测试代码
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("test")
      .master("local[*]")
      .getOrCreate()

    val list = List(
      "2", "3", "4"
    )

    import spark.implicits._

    val dft = spark.createDataset(list).toDF("name")
    val df = readMysqlTable(spark, "zdb", "name > 2")
    df.show()
    spark.close()
  }
}



