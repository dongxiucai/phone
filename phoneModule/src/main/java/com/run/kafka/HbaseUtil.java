package com.run.kafka;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.text.DecimalFormat;


public class HbaseUtil {

    // 配置conf
    static Configuration configuration = null;

    /**
     * 存放log
     * @param log
     * @throws Exception
     */
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

            // 获取到rowkey
            String rowkey = getRowKey(6,callPhone,date,"1"); // 主叫
            String rowkeyed = getRowKey(6,calledPhone,date,"0"); // 被叫

            // 存储主叫
            PutData(rowkey,callName,callPhone,calledName,calledPhone,date,calllong);
            // 存储被叫
            PutData(rowkeyed,callName,callPhone,calledName,calledPhone,date,calllong);
        }
    }


    // 获取到rowkey
    private static String getRowKey(int regions,String phone,String date,String stat){
        // 生成分区
        // 取后四位
        String ph = phone.substring(7);
        // 取日期前6位
        String[] sp = date.split("-");
        String da = sp[0]+sp[1];
        // 取模，求分区
        int qu = (Integer.valueOf(ph)+Integer.valueOf(da))%regions;
        // 格式化qu
        DecimalFormat df = new DecimalFormat("00");//设置格式
        String strMo = df.format(qu);//格式转换
        // 时间格式转换
        String s = date.replaceAll(" ", "")
                .replaceAll("-", "")
                .replaceAll(":", "");
        // 生成rowkey
        // 01_手机号_yyyyMMddhhmmss_1 主叫
        String rowkey = strMo + "_" + phone + "_" + s + "_"+stat;
        return rowkey;
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
        Table table = connection.getTable(TableName.valueOf("phone:log2"));
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

    // 创建表，预切割
    public static void createTableHbase(String tableName,int regions,String... famliy) throws Exception{
        // 获得连接
        Connection connection = ConnectionFactory.createConnection(configuration);
        // 创建admin
        Admin admin = connection.getAdmin();
        // 判断表是否存在
        if(admin.tableExists(TableName.valueOf(tableName))){
            // 存在
            return;
        }else {
            // 不存在创建
            //创建表的操作对象
            HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            // 创建表
            for(String f : famliy){
                // 创建列族描述器
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(Bytes.toBytes(f));
                // 添加到表描述器
                hTableDescriptor.addFamily(hColumnDescriptor);
            }

            byte[][] by = new byte[regions][];
            // 预切割
            for(int i = 0;i < regions;i++){
                // 格式化
                DecimalFormat format = new DecimalFormat("00");
                String s = format.format(i);
                // 存入byte数组
                by[i] = Bytes.toBytes(s);
            }
            // 创建表
            admin.createTable(hTableDescriptor,by);
            // 关闭连接
            admin.close();
        }
    }
}
