package com.run.readHbase;

import com.run.pojo.CallPhoneData;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import scala.xml.Null;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.ParseException;

/**
 * 把结果聚合后输入到Mysql中
 */
public class WriteMysqlReduce extends Reducer<Text, Text, CallPhoneData, CallPhoneData> {

    CallPhoneData ke = new CallPhoneData();
    CallPhoneData va = new CallPhoneData();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int dateLong = 0;
        // key手机号_年份月份
        // 统计通话时长，秒
        for(Text calllong : values){
            // 装换
            Integer llong = Integer.valueOf(calllong.toString());
            dateLong =+ llong;
        }
        // 切割key
        String[] split = key.toString().split("_");
        // 取出手机号，作为id
        ke.setId(split[0]);
        // 取出年月
        ke.setYearM(split[1]);
        // 取出通话时长
        ke.setCalllong(String.valueOf(dateLong));
        // 输出到文件
        context.write(ke,null);
    }
}
