package com.tf.tfserversparkscala.common.datasources.mysql

import java.util.concurrent.{Executors, LinkedBlockingDeque, ThreadPoolExecutor, TimeUnit}

import com.mchange.v2.c3p0.ComboPooledDataSource
import com.tf.tfserversparkscala.config.EnumUtil
import org.apache.commons.lang.StringUtils

/**
  * 静态获取配置文件中的数据源
  */
class MDBManager extends Serializable {
  @volatile var url: String = _
  @volatile var user: String = _
  @volatile var password: String = _

  def this(url: String, user: String, password: String) {
    this
    this.url = url
    this.user = user
    this.password = password
  }

  //配置初始化参数
  val cpds: ComboPooledDataSource = new ComboPooledDataSource(true)
  try {
    if (StringUtils.isEmpty(url) && StringUtils.isEmpty(user) && StringUtils.isEmpty(password)) {
      //mysql 连接地址
      cpds.setJdbcUrl(EnumUtil.MYSQL_URL)
      //用户
      cpds.setUser(EnumUtil.MYSQL_USERNAME)
      //密码
      cpds.setPassword(EnumUtil.MYSQL_PASSWORD)
    } else {
      require(StringUtils.isNotEmpty(url) && StringUtils.isNotBlank(url))
      require(StringUtils.isNotEmpty(user) && StringUtils.isNotBlank(user))
      require(StringUtils.isNotEmpty(password) && StringUtils.isNotBlank(password))
      //mysql 连接地址
      cpds.setJdbcUrl(url)
      //用户
      cpds.setUser(user)
      //密码
      cpds.setPassword(password)
    }
    //驱动
    cpds.setDriverClass(EnumUtil.MYSQL_DRIVER)
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

/**
  * 多线程并发访问线程池类，每个JVM仅创建一个单例
  */
object MDBManager {
  //添加volatile ，防止CPU指令重排序造成的线程获取的是初始值而不是构造值
  //java对象创建过程 object o = new  Object 1.内存中首先分配对象并指定初始默认值 2.实例化构造参数 3.建立连接
  @volatile private var mdbManager: MDBManager = _

  //获取动态数据库连接对象
  def GetMDBManagerInstance(url: String ,
    user: String ,
    password: String): MDBManager = {

    if (mdbManager == null) {
      this.synchronized {
        if (mdbManager == null) {
          mdbManager = new MDBManager(url, user, password)
        }
      }
    }
    mdbManager
  }

  //获取静态数据库连接对象
  def GetMDBManagerInstance(): MDBManager = {

    if (mdbManager == null) {
      this.synchronized {
        if (mdbManager == null) {
          mdbManager = new MDBManager()
        }
      }
    }
    mdbManager
  }
}


class ThreadExample extends Runnable {
  override def run() {
    val m = MDBManager.GetMDBManagerInstance(EnumUtil.MYSQL_URL, EnumUtil.MYSQL_USERNAME, EnumUtil.MYSQL_PASSWORD)
    val c = m.getConnection
    println("正在使用连接数：" + m.cpds.getNumBusyConnections() + "线程名：" + Thread.currentThread().getName)
    println("空闲连接数：" + m.cpds.getNumIdleConnections() + "线程名：" + Thread.currentThread().getName)
    println("总连接数：" + m.cpds.getNumConnections() + "线程名：" + Thread.currentThread().getName)
    c.close()
  }
}

object MysqlPool {
  def main(args: Array[String]): Unit = {
    val exc = new ThreadPoolExecutor(2,
      50,
      1L,
      TimeUnit.SECONDS,
      new LinkedBlockingDeque[Runnable](50),
      Executors.privilegedThreadFactory(),
      new ThreadPoolExecutor.DiscardOldestPolicy
    )
    for (i <- 0 to 100000) {
      val i = new ThreadExample
      val t2 = new Thread(i)
      exc.execute(t2)
    }

    exc.shutdown()
  }
}
