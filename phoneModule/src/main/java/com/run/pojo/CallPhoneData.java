package com.run.pojo;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CallPhoneData implements WritableComparable<CallPhoneData> {

    private CallDate callDate = new CallDate(); // 时间
    private CallPeople callPhone = new CallPeople(); // 电话
    private int call_sum; // 通话时长
    private int call_duration_sum; // 通话次数

    public CallDate getCallDate() {
        return callDate;
    }

    public void setCallDate(CallDate callDate) {
        this.callDate = callDate;
    }

    public CallPeople getCallPhone() {
        return callPhone;
    }

    public void setCallPhone(CallPeople callPhone) {
        this.callPhone = callPhone;
    }

    public int getCall_sum() {
        return call_sum;
    }

    public void setCall_sum(int call_sum) {
        this.call_sum = call_sum;
    }

    public int getCall_duration_sum() {
        return call_duration_sum;
    }

    public void setCall_duration_sum(int call_duration_sum) {
        this.call_duration_sum = call_duration_sum;
    }

    @Override
    public int compareTo(CallPhoneData o) {
        int res = this.callDate.compareTo(o.callDate);
        if(res != 0) return res;
        res = this.callPhone.compareTo(o.callPhone);
        return res;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.callDate.write(out);
        this.callPhone.write(out);
        out.writeInt(this.call_sum);
        out.writeInt(this.call_duration_sum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.callDate.readFields(in);
        this.callPhone.readFields(in);
        this.call_sum = in.readInt();
        this.call_duration_sum = in.readInt();
    }

    @Override
    public String toString() {
        return this.getCallPhone().getName()+"\t"+this.getCallPhone().getPhoneNum()+
                "\t"+this.getCallDate().getYear()+"\t"+this.getCallDate().getMonth()+"\t"+this.getCallDate().getDay()+
                "\t"+this.getCall_sum()+"\t"+this.getCall_duration_sum();
    }
}
