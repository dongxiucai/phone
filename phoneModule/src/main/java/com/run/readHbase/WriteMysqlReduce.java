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
public class WriteMysqlReduce extends Reducer<CallPhoneData, Text, CallPhoneData, NullWritable> {

    @Override
    protected void reduce(CallPhoneData key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 总分钟
        int call_sum = 0;
        // 总次数
        int call_duration_sum = 0;
        // 迭代
        for(Text calllong : values){
            // 装换
            Integer llong = Integer.valueOf(calllong.toString());
            call_sum =+ llong;
            // 次数
            call_duration_sum++;
        }
        // 封装数据到key
        key.setCall_sum(call_sum);
        key.setCall_duration_sum(call_duration_sum);
        // 输出到文件
        context.write(key,NullWritable.get());
    }
}
