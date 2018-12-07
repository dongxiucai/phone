package com.run.readHbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.Text;

import java.io.IOException;


/**
 * map端
 */
public class ReadHbaseMapper extends TableMapper<Text, Text> {

    Text ke = new Text();
    Text va = new Text();
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        // 获取到rowkey
        String rowkey = new String(key.get());
        // 获取一行数据
        Cell[] cells = value.rawCells();
        // 获取的数据，通话时长，日期
        String date = "";
        String calllong = "";
        // 循环取出
        for(Cell cell : cells){
            // 取出行名称
            String lineName = new String(CellUtil.cloneQualifier(cell));
            // 判断日期
            if(lineName.equals("date")){
                date = new String(CellUtil.cloneValue(cell));
            }
            // 判断时长
            if(lineName.equals("calllong")){
                calllong = new String(CellUtil.cloneValue(cell));
            }
        }

        // 测试输出
        System.out.println("rowkey: "+rowkey+", date: "+date+", calllong: "+calllong);

        // 01_手机号_yyyyMMddhhmmss_1
        String[] split = rowkey.split("_");
        // 截取电话号码
        String phoneNum = split[1];
        // 拼接key
        String dataCallKe = phoneNum+"_"+split[2].substring(0,6);
        // 输出到文件
        ke.set(dataCallKe);
        va.set(calllong);
        context.write(ke, va);
    }
}
