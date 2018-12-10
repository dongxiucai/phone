package com.run.readHbase;

import com.run.dao.DimesionImpl;
import com.run.pojo.CallPhoneData;
import com.run.utils.JDBCInstance;
import com.run.utils.JDBCUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MysqlOutputFormat extends OutputFormat<CallPhoneData, NullWritable> {

    // 定义
    private static OutputCommitter committer = null;

    // 进入，
    @Override
    public RecordWriter<CallPhoneData, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        //初始化JDBC连接器对象
        Connection conn = null;
        conn = JDBCInstance.getInstance();
        try {
            // 通过设置该属性，启动手动事务
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            throw new RuntimeException(e.getMessage());
        }

        return new MyRecordWriter(conn);
    }


    // 校验文件
    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {
        //
    }

    // 提交代码
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if(committer == null){
            String name = context.getConfiguration().get(FileOutputFormat.OUTDIR);
            Path outputPath = name == null ? null : new Path(name);
            committer = new FileOutputCommitter(outputPath, context);
        }
        return committer;
    }
}




class MyRecordWriter extends RecordWriter<CallPhoneData,NullWritable>{
    private DimesionImpl dimesion = new DimesionImpl();
    private Connection conn = null; // jdbc连接
    private PreparedStatement preparedStatement = null;
    private String insertSQL = null;
    private int count = 0;
    private final int BATCH_SIZE = 5;

    public MyRecordWriter(Connection conn){
        this.conn = conn;
    }

    // 写数据
    @Override
    public void write(CallPhoneData key, NullWritable value) throws IOException, InterruptedException {
        // 写入到mysql中
        // tb_call
        // `id_date_contact` varchar(255) NOT NULL,	// 时间编号_电话编号
        //  `id_date_dimension` int(11) NOT NULL,		// 时间编号
        //  `id_contact` int(11) NOT NULL, 		// 电话编号
        //  `call_sum` int(11) NOT NULL,			// 总分钟
        //  `call_duration_sum` int(11) NOT NULL,		// 总次数

        try {
            int phone = dimesion.getDimensionID(key.getCallPhone());
            int date = dimesion.getDimensionID(key.getCallDate());
            String call_id = phone+"_"+date;
            // 写入数据库
            if(insertSQL == null){
                //mysql
                insertSQL = "INSERT INTO `tb_call` (`id_date_contact`, `id_date_dimension`, `id_contact`, `call_sum`, `call_duration_sum`) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `id_date_contact` = ?;";
            }
            if(preparedStatement == null){
                preparedStatement = conn.prepareStatement(insertSQL);
            }

            preparedStatement.setString(1,call_id);
            preparedStatement.setInt(2,date);
            preparedStatement.setInt(3,phone);
            preparedStatement.setInt(4,key.getCall_sum());
            preparedStatement.setInt(5,key.getCall_duration_sum());
            //
            preparedStatement.setString(6,call_id);
            preparedStatement.addBatch();
            count++;
            if(count >= BATCH_SIZE){
                preparedStatement.executeBatch();
                conn.commit();
                count = 0;
                preparedStatement.clearBatch();
            }
        } catch (Exception e){
            e.printStackTrace();
        }

    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        try {
            if(preparedStatement != null){
                preparedStatement.executeBatch();
                this.conn.commit();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            JDBCUtil.close(conn, preparedStatement, null);
        }
    }

}