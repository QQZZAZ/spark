package com.tf.tfserversparkscala

import com.mchange.v2.c3p0.PooledDataSource
import com.tf.tfserversparkscala.common.datasources.mysql.MDBManager

object MysqlPool {
  def main(args: Array[String]): Unit = {
    while (true) {
      val m = MDBManager.GetMDBManagerInstance()
      val c = m.getConnection
      println(m.cpds.asInstanceOf[PooledDataSource].getNumIdleConnectionsDefaultUser)
      c.close()
    }
  }
}
