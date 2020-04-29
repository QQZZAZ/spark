package com.tf.tfserversparkscala.service.onlineservice

import java.sql.Connection

import com.tf.tfserversparkscala.common.datasources.mysql.MDBManager
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.{ForeachWriter, Row}

/**
  * 实时流写Mysql的sink
  * 如果不需要手动指定IP和端口
  * 给默认值
  * 继承此类之后实现process函数，开发逻辑即可
  *
  * 如果想要手动指定参数，需要自己实现open，process和close方法
  */
abstract class MysqlSink(url: String,
                         user: String,
                         password: String) extends ForeachWriter[Row]() {
  var conn: Connection = _

  override def open(partitionId: Long, version: Long): Boolean = {
    //通过连接池创建mysql连接
    if (StringUtils.isBlank(url) && StringUtils.isBlank(user) & StringUtils.isBlank(password)) {
      conn = MDBManager.GetMDBManagerInstance.getConnection
    } else {
      require(StringUtils.isNotBlank(url))
      require(StringUtils.isNotBlank(user))
      require(StringUtils.isNotBlank(password))

      conn = MDBManager.GetMDBManagerInstance(url, user, password).getConnection
    }
    true
  }

  //业务逻辑
  override def process(value: Row): Unit = {

  }

  def commit(): Unit ={
    this.conn.commit()
  }

  /**
    * 归还连接池
    *
    * @param errorOrNull
    */
  override def close(errorOrNull: Throwable): Unit = {
    conn.close()
  }
}
