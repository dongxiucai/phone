package com.run.test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MRTest {
    public static void main(String[] args) throws Exception{
        Configuration configuration = new Configuration();
        //
        DBConfiguration.configureDB(configuration,"com.mysql.jdbc.Driver","jdbc:mysql://192.168.80.103:3306/phoneLog","root", "123456");
        // 获取job
        Job job = Job.getInstance(configuration);
        job.setJarByClass(MRTest.class);
        // 设置map
        job.setMapperClass(Mapp.class);
        /*job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);*/
        // 设置re
        //job.setReducerClass(Redu.class);
        job.setNumReduceTasks(0);
        //
        FileInputFormat.setInputPaths(job,new Path("D:\\Demo\\hadoop\\ouput\\out2\\part-r-00000"));
        DBOutputFormat.setOutput(job,"mrtest","phone","month","llong");
        job.setOutputFormatClass(DBOutputFormat.class);
        // 提交
        boolean isSuccess = job.waitForCompletion(true);
        if(!isSuccess){
            throw  new IOException("Job running with error");
        }

    }
}

class Mapp extends Mapper<LongWritable,Text,BeanTest, BeanTest> {

    /*Text ke = new Text();
    Text va = new Text();*/
    BeanTest ke = new BeanTest();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //
        String[] split = value.toString().split("\t");
        String phone = split[0].split("_")[0];
        String month = split[0].split("_")[1];
        String llong = split[1];
        System.out.println("======: "+phone+" : "+" : "+llong);
        /*ke.set(num);
        va.set(llong);*/
        ke.setPhone(phone);
        ke.setMonth(month);
        ke.setLlong(llong);
        context.write(ke,null);
    }
}


/*class Redu extends Reducer<Text,Text,BeanTest,BeanTest>{
    //
    BeanTest b = new BeanTest();
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String v = "";
        v = values.iterator().next().toString();
        b.setNum(key.toString());
        b.setNum(v);
        context.write(b,null);
    }
}*/


// bean
class BeanTest implements DBWritable {

    private String phone; // 电话号码
    private String month; // 月份
    private String llong; // 通话时长

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getLlong() {
        return llong;
    }

    public void setLlong(String llong) {
        this.llong = llong;
    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1,this.phone);
        preparedStatement.setString(2,this.month);
        preparedStatement.setString(3,this.llong);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.phone = resultSet.getString(1);
        this.month = resultSet.getString(2);
        this.llong = resultSet.getString(3);
    }
}