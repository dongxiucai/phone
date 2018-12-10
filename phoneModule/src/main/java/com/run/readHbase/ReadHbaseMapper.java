package com.run.readHbase;

import com.run.pojo.CallDate;
import com.run.pojo.CallPeople;
import com.run.pojo.CallPhoneData;
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
public class ReadHbaseMapper extends TableMapper<CallPhoneData, Text> {

    CallPhoneData ke = new CallPhoneData();
    Text va = new Text();
    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        // 获取到rowkey
        String rowkey = new String(key.get());
        // 获取一行数据
        Cell[] cells = value.rawCells();
        // 获取的数据，通话时长，主叫名字和被叫名字
        String zname = ""; // 主打名字
        String bname = ""; // 被打名字
        String calllong = ""; // 电话时长
        String bphoneNum = ""; // 被打手机
        // 循环取出
        for(Cell cell : cells){
            // 取出行名称
            String lineName = new String(CellUtil.cloneQualifier(cell));
            // 判断时长
            if(lineName.equals("calllong")){
                calllong = new String(CellUtil.cloneValue(cell));
            }
            if(lineName.equals("callName")){
                zname = new String(CellUtil.cloneValue(cell));
            }
            if(lineName.equals("calledName")){
                bname = new String(CellUtil.cloneValue(cell));
            }
            if(lineName.equals("calledPhone")){
                bphoneNum = new String(CellUtil.cloneValue(cell));
            }
        }

        // 01_手机号_yyyyMMddhhmmss_1
        String[] split = rowkey.split("_");
        // 扔掉被叫，不然数据会翻倍
        if(split[3].equals("0")) return;
        // 截取主打电话号码
        String zphoneNum = split[1];
        // 截取时间年
        String year = split[2].substring(0,4);
        // 截取月
        String month = split[2].substring(4,6);
        // 截取日
        String day = split[2].substring(6,8);
        // 年
        CallDate callDateyear = new CallDate(year,"-1","-1");
        // 年 月
        CallDate callDatemonth = new CallDate(year,month,"-1");
        // 年 月 日
        CallDate callDateday = new CallDate(year,month,day);
        // 主叫
        CallPeople zcallPeople = new CallPeople(zname,zphoneNum);
        // 被叫
        CallPeople bcallPeople = new CallPeople(bname,bphoneNum);
        // 提交主打 年
        ke.setCallDate(callDateyear);
        ke.setCallPhone(zcallPeople);
        va.set(calllong);
        context.write(ke,va);
        // 提交主打 年月
        ke.setCallDate(callDatemonth);
        ke.setCallPhone(zcallPeople);
        va.set(calllong);
        context.write(ke,va);
        // 提交主打年月日
        ke.setCallDate(callDateday);
        ke.setCallPhone(zcallPeople);
        va.set(calllong);
        context.write(ke,va);

        // 提交被打年
        ke.setCallDate(callDateyear);
        ke.setCallPhone(bcallPeople);
        va.set(calllong);
        context.write(ke,va);
        // 提交被打年月
        ke.setCallDate(callDatemonth);
        ke.setCallPhone(bcallPeople);
        va.set(calllong);
        context.write(ke,va);
        // 提交被打年月日
        ke.setCallDate(callDateday);
        ke.setCallPhone(bcallPeople);
        va.set(calllong);
        context.write(ke,va);

    }
}
