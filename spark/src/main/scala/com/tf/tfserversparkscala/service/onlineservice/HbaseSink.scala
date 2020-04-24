package com.tf.tfserversparkscala.service.onlineservice

import com.tf.tfserversparkscala.common.tfserversparkscala.common.utils.hbase.ConnectHbaseUtils
import com.tf.tfserversparkscala.config.EnumUtil
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client._
import org.apache.spark.sql.{ForeachWriter, Row}

/**
  * 需要访问外部数据库进行过滤的sink
  * 自定义写Hbase的sink
  */
abstract class HbaseSink extends ForeachWriter[Row] {
  //Hbase配置文件
  var configuration: Configuration = _
  //hbase连接
  var connection: Connection = _


  //创建连接
  override def open(partitionId: Long, version: Long): Boolean = {
    configuration = ConnectHbaseUtils.config(EnumUtil.HBASE_ZOOKEEPER_IP, EnumUtil.HBASE_ZOOKEEPER_PORT, EnumUtil.HBASE_MASTER)
    connection = ConnectionFactory.createConnection(configuration)
    true
  }


  //业务逻辑
  override def process(value: Row): Unit = {

  }

  /**
    * 关闭链接
    *
    * @param errorOrNull
    */
  override def close(errorOrNull: Throwable): Unit = {
    if (null != connection) {
      connection.close()
    }
  }
}
