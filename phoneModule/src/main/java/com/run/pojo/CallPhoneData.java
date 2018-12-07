package com.run.pojo;

import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CallPhoneData implements DBWritable {

    private String id; // id 手机号
    private String yearM; // 年月
    private String calllong; // 通话时长

    public CallPhoneData(){

    }


    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getYearM() {
        return yearM;
    }

    public void setYearM(String yearM) {
        this.yearM = yearM;
    }

    public String getCalllong() {
        return calllong;
    }

    public void setCalllong(String calllong) {
        this.calllong = calllong;
    }

    @Override
    public void write(PreparedStatement preparedStatement) throws SQLException {
        preparedStatement.setString(1,id);
        preparedStatement.setString(2,yearM);
        preparedStatement.setString(3,calllong);
    }

    @Override
    public void readFields(ResultSet resultSet) throws SQLException {
        this.id = resultSet.getString(1);
        this.yearM = resultSet.getString(2);
        this.calllong = resultSet.getString(3);
    }
}
