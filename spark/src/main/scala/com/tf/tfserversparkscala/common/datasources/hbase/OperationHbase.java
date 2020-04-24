package com.tf.tfserversparkscala.common.datasources.hbase;


import com.tf.tfserversparkscala.config.EnumUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * java请求Hbase连接
 */
public class OperationHbase {
    /**
     * 连接HBase地址
     */
    public static Configuration configuration;
    public static Connection connection;
    public static Admin admin;
    private static final Object lock = new Object();

    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", EnumUtil.HBASE_ZOOKEEPER_PORT);
        configuration.set("hbase.zookeeper.quorum", EnumUtil.HBASE_ZOOKEEPER_IP);
        configuration.set("hbase.master", EnumUtil.HBASE_MASTER);
        configuration.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 1000000);
        try {
            connection = ConnectionFactory.createConnection(configuration);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //获取一次Hbase链接
    public static Connection createHbaseClient() {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", EnumUtil.HBASE_ZOOKEEPER_PORT);
        configuration.set("hbase.zookeeper.quorum", EnumUtil.HBASE_ZOOKEEPER_IP);
        configuration.set("hbase.master", EnumUtil.HBASE_MASTER);
        configuration.set("hbase.client.scanner.caching", "1");
        configuration.set("hbase.rpc.timeout", "600000");
        configuration.setLong(HConstants.HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, 1000000);
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * 关闭连接
     */
    public static void close() {
        try {
            if (null != admin) {
                admin.close();
            }
            if (null != connection) {
                connection.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 批量删除Hbase数据
     *
     * @param tableName
     */
    public static void deleteRows(String tableName, List<Delete> deleteList) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            table.delete(deleteList);
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据rowkey申请单次链接查询数据
     * @param tableName
     * @param rowkey
     * @param connection
     * @return
     */
    public static Map<String, String> QueryByCondition(String tableName, String rowkey, Connection connection) {
        Map<String, String> map = new HashMap<String, String>();
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowkey));
            Result result = table.get(get);
            for (Cell cell : result.rawCells()) {
                String key = new String(CellUtil.cloneQualifier(cell));
                String value = new String(CellUtil.cloneValue(cell));
                map.put(key, value);
            }
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return map;
    }


    public static void main(String[] args) {
        Connection con = OperationHbase.createHbaseClient();
        Map<String, String> map = OperationHbase.QueryByCondition("INFO_TABLE",
                "http://cn.rfi.fr/wire/20190822-%E5%AE%9D%E7%89%B9%E7%93%B6%E5%8F%98%E7%8E%B0%E9%87%91-%E5%8E%84%E7%93%9C%E5%A4%9A%E9%80%9A%E5%8B%A4%E6%97%8F%E6%8B%BF%E9%92%B1%E6%90%AD%E5%85%AC%E8%BD%A6"
                , con);
        map.forEach((k, v) -> System.out.println(k + " " + v));
    }
}
