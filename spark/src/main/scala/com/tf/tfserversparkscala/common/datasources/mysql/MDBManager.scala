package com.tf.tfserversparkscala.common.datasources.mysql

import com.mchange.v2.c3p0.{ComboPooledDataSource, PooledDataSource}
import com.tf.tfserversparkscala.config.EnumUtil

/**
  * 静态获取配置文件中的数据源
  */
class MDBManager extends Serializable {

  //配置初始化参数
  val cpds: ComboPooledDataSource = new ComboPooledDataSource(true)
  try {
    //mysql 连接地址
    cpds.setJdbcUrl(EnumUtil.MYSQL_URL)
    //驱动
    cpds.setDriverClass(EnumUtil.MYSQL_DRIVER)
    //用户
    cpds.setUser(EnumUtil.MYSQL_USERNAME)
    //密码
    cpds.setPassword(EnumUtil.MYSQL_PASSWORD)
    //最大连接池数量
    cpds.setMaxPoolSize(Integer.valueOf(EnumUtil.MYSQL_MAXPOOLSIZE))
    //最小连接池数量
    cpds.setMinPoolSize(Integer.valueOf(EnumUtil.MYSQL_MINPOOLSIZE))
    //当连接池连接用完了，根据该属性决定一次性新建多少连接
    cpds.setAcquireIncrement(Integer.valueOf(EnumUtil.MYSQL_ACQUIREINCREMENT))
    //初始化线程池的数量
    cpds.setInitialPoolSize(Integer.valueOf(EnumUtil.MYSQL_INITIALPOOLSIZE))
    //最大空闲时间，当连接池中连接经过一段时间没有使用，根据该数据进行释放
    cpds.setMaxIdleTime(Integer.valueOf(EnumUtil.MYSQL_MAXIDLETIME))

  } catch {
    case ex: Exception => ex.printStackTrace()
  }

  //获取mysql的连接
  def getConnection = {
    try {
      cpds.getConnection()
    } catch {
      case ex: Exception => ex.printStackTrace()
        null
    }
  }

}

//动态获取用户自定义的数据源
class DynamicMDBManager(jdbcURL: String, user: String, passWord: String) extends MDBManager {
  //配置初始化参数
  try {
    //mysql 连接地址
    cpds.setJdbcUrl(jdbcURL)
    //用户
    cpds.setUser(user)
    //密码
    cpds.setPassword(passWord)
  } catch {
    case ex: Exception => ex.printStackTrace()
  }
}

/**
  * 多线程并发访问线程池类，每个JVM仅创建一个单例
  */
object MDBManager {
  private var mdbManager: MDBManager = new MDBManager
  private var DynamicmdbManager: DynamicMDBManager = _

  //获取静态数据库连接对象
  def GetMDBManagerInstance(): MDBManager = {
    mdbManager
  }

  //获取动态连接对象
  def GetDynamicMDBManagerInstance(jdbcURL: String, user: String, passWord: String): DynamicMDBManager = {
    if (DynamicmdbManager == null) {
      this.synchronized {
        if (DynamicmdbManager == null) {
          DynamicmdbManager = new DynamicMDBManager(jdbcURL, user, passWord)
        }
      }
    }
    DynamicmdbManager
  }

}

/*
class ThreadExample extends Runnable {
  override def run() {
    val m = MDBManager.GetMDBManagerInstance()
    val c = m.getConnection
    println("正在使用连接数：" + m.cpds.getNumBusyConnections() + "线程名：" + Thread.currentThread().getName)
    println("空闲连接数：" + m.cpds.getNumIdleConnections()+ "线程名：" + Thread.currentThread().getName)
    println("总连接数：" + m.cpds.getNumConnections()+ "线程名：" + Thread.currentThread().getName)
    c.close()
  }
}

object MysqlPool {
  def main(args: Array[String]): Unit = {
    for (i <- 0 to 100) {
      val i = new ThreadExample
      val t2 = new Thread(i)
      t2.start()
    }
  }
}*/
