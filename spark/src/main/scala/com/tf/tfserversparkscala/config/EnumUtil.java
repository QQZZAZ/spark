package com.tf.tfserversparkscala.config;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by guo on 2017/11/7.
 */
public class EnumUtil {

    public static Properties property;

    static {
        property = new Properties();
        try {
            property.load(EnumUtil.class.getClassLoader().getResourceAsStream("application.properties"));
            String path = property.getProperty("profiles.active");
            System.out.println("path: "+ path);

            property.load(EnumUtil.class.getClassLoader().getResourceAsStream(path));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    //hbase zookeeper地址
    public static final String HBASE_ZOOKEEPER_IP = property.getProperty("hbase.zookeeper.ip");
    //hbase的master
    public static final String HBASE_MASTER = property.getProperty("hbase.master");
    //hbase的zookeeper的端口
    public static final String HBASE_ZOOKEEPER_PORT = "2181";
    //kafka zookeeper地址
    public static final String KAFKA_ZOOKEEPER_URL = property.getProperty("KAFKA_ZOOKEEPER_URL");
    //kafka的groupid
    public static final String GROUP_ID = "text_analysis_spark_ge";
    //mysql的用户名
    public static final String MYSQL_URL = property.getProperty("mysql.jdbc.url");
    //mysql的密码
    public static final String MYSQL_USERNAME = property.getProperty("mysql.username");
    //redis地址
    public static final String MYSQL_PASSWORD = property.getProperty("mysql.password");
    //redis端口
    public static final String MYSQL_MINPOOLSIZE = property.getProperty("mysql.pool.jdbc.minPoolSize");
    //redis集群地址
    public static final String MYSQL_MAXPOOLSIZE = property.getProperty("mysql.pool.jdbc.maxPoolSize");
    //redis集群端口
    public static final String MYSQL_INITIALPOOLSIZE = property.getProperty("mysql.pool.jdbc.initialPoolSize");
    //连接elasticsearch证书路径
    public static final String MYSQL_DRIVER = property.getProperty("mysql.driver");
    //连接elasticsearch证书路径
    public static final String MYSQL_MAXIDLETIME = property.getProperty("mysql.pool.jdbc.maxIdleTime");
    //连接elasticsearch证书路径
    public static final String MYSQL_ACQUIREINCREMENT = property.getProperty("mysql.pool.jdbc.acquireIncrement");
    //redis地址
    public static final String REDISURL = property.getProperty("redis.url");
    //redis端口
    public static final int REDISPORT = Integer.parseInt(property.getProperty("redis.port"));
    //redis集群地址
    public static final String REDISCLUSTERURL = property.getProperty("redisCluster.url");
    //redis集群端口
    public static final int REDISCLUSTERPOST = Integer.parseInt(property.getProperty("redisCluster.port"));
    //连接elasticsearch证书路径
    public static final String CERTIFICATE_PATH = property.getProperty("certificate.path");
    //kafka的topic
    public static final String KAFKA_TOPIC = property.getProperty("kafka_topic");
    //elasticSearch集群地址
    public static final String ELASTICSEARCHCLUSTERURL = property.getProperty("elasticsearchCluster.url");
    //elasticSearch集群端口
    public static final int ELASTICSEARCHCLUSTERPORT = Integer.parseInt(property.getProperty("elasticsearchCluster.port"));
    //hdfscheckpoint
    public static final String HDFSCHECKPOINT = property.getProperty("hdfscheckpoint");

}