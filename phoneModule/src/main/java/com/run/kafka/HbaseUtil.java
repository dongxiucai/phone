package com.run.kafka;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import java.text.SimpleDateFormat;
import java.util.Date;

public class HbaseUtil {

    // 配置conf
    static Configuration configuration = null;


    public static void setLog(String log) throws Exception {
        // 切割字符串
        // 打电话者 手机号 接电话者 手机号 年 月日 时间间隔
        String[] split = log.split("\t");
        // 判断log数据是否有效
        if(split.length > 4){
            // 取出各段数据
            String callName = split[0];
            String callPhone = split[1];
            String calledName = split[2];
            String calledPhone = split[3];
            String date = split[4];
            String calllong = split[5];
            // 时间戳
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            Date de = sdf.parse(date);
            long deTime = de.getTime();
            // 生成rowkey
            // row+手机号+时间戳
            String rowkey = "row" + calledName + deTime;
            // 存到hbase
            PutData(rowkey,callName,callPhone,calledName,calledPhone,date,calllong);
            //
        }
    }

    // 初始化
    public static void init(String zkquorum,String zkproperty){
        // 配置conf
        configuration = HBaseConfiguration.create();
        // 配置zk集群的ip
        configuration.set("hbase.zookeeper.quorum",zkquorum);
        // 配置zk集群的端口
        configuration.set("hbase.zookeeper.property.clientport",zkproperty);
    }

    // 存储put
    private static int PutData(String rowkey,String callName, String callPhone,
                           String calledName,String calledPhone,
                           String date,String calllong) throws Exception{
        // 获取连接
        Connection connection = ConnectionFactory.createConnection(configuration);
        // 创建表的对象
        Table table = connection.getTable(TableName.valueOf("phone:log"));
        // 创建put对象
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("callName"),Bytes.toBytes(callName));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("callPhone"),Bytes.toBytes(callPhone));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("calledName"),Bytes.toBytes(calledName));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("calledPhone"),Bytes.toBytes(calledPhone));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("date"),Bytes.toBytes(date));
        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("calllong"),Bytes.toBytes(calllong));
        // 存入
        table.put(put);
        table.close();
        // 存入成功
        return 1;
    }
}
