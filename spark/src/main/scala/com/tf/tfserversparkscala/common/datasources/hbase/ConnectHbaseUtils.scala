package com.tf.tfserversparkscala.common.tfserversparkscala.common.utils.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
  * Created by guo on 2018/4/18.
  */
object ConnectHbaseUtils {

  /**
    * 创建初始的rdd
    */
  def create_Initial_RDD(sc: SparkContext, conf: Configuration): RDD[(ImmutableBytesWritable, Result)] = {
    val initialRDD = sc.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
    initialRDD
  }

  /**
    * 创建输出连接的JobConf文件对象
    *
    * @param tableName 需要读取的Hbase表名
    * @param conf      hbase输入连接文件
    * @return 输出连接文件
    */
  def createJobConf(tableName: String, conf: Configuration): JobConf = {
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    jobConf
  }

  /**
    * 创建输入连接configuration文件对象
    *
    * @param tableName
    * @return
    */
  def hbase_conf(tableName: String, hbase_zkip: String, hbase_zkport: String): Configuration = {
    val conf = HBaseConfiguration.create()
    //设置zookeeper集群地址
    conf.set("hbase.zookeeper.quorum", hbase_zkip)
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", hbase_zkport)
    //设置hbase的表名
    conf.set(TableInputFormat.INPUT_TABLE, tableName)
    conf.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 1000000)
    conf
  }

  //实时流创建配置文件
  def config(hbase_zkport: String, hbase_zkip: String, hbaseMaster: String) = {
    val configuration = HBaseConfiguration.create
    //hbase挂载的zookeeper端口号
    configuration.set("hbase.zookeeper.property.clientPort", hbase_zkport)
    //hbase挂载的zookeeper地址
    configuration.set("hbase.zookeeper.quorum", hbase_zkip)
    //hbase主节点
    configuration.set("hbase.master", hbaseMaster + ":600000")
    //hbase集群彼此最大通信时间
    configuration.set("hbase.rpc.timeout", "600000")
    //超时时间
    configuration.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 1000000)
    configuration
  }
}
