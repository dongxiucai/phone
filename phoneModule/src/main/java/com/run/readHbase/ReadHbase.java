package com.run.readHbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ReadHbase {

    //
    public static void main(String[] args) throws Exception{
        // config
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum","spark101,spark102,spark103");
        configuration.set("hbase.zookeeper.property.clientport","2181");

        // 数据库设置
        DBConfiguration.configureDB(configuration,"com.mysql.jdbc.Driver","jdbc:mysql://192.168.80.103:3306/phoneLog","root", "123456");

        // 创建job任务
        Job job = Job.getInstance(configuration);
        job.setJarByClass(ReadHbase.class);
        // 配置job

        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.setCaching(500);

        // 设置Mapper
        TableMapReduceUtil.initTableMapperJob(
                "phone:log2", // 数据源的表名
                scan, // scan扫描控制器
                ReadHbaseMapper.class, // 设置Mapper类
                Text.class, // 设置Mapper输出key类型
                Text.class, // 设置Mapper输出value值类型
                job // 设置job
        );

        // 设置Reduce
        job.setReducerClass(WriteMysqlReduce.class);
       /* job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);*/

        // 设置输出
        // 表名，字段 按CallPhoneData中序列化的顺序
        DBOutputFormat.setOutput(job,"phone_info","id","yearM","calllong");
        job.setOutputFormatClass(DBOutputFormat.class);

        // FileOutputFormat.setOutputPath(job, new Path("D:\\Demo\\hadoop\\ouput\\out2"));

        boolean isSuccess = job.waitForCompletion(true);
        if(!isSuccess){
            throw  new IOException("Job running with error");
        }

    }

}
