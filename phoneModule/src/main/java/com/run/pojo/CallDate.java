package com.run.pojo;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CallDate implements WritableComparable<CallDate> {

    private String year; // 年份
    private String month; // 月份
    private String day; // 日

    public CallDate(String year,String month,String day){
        this.year = year;
        this.month = month;
        this.day = day;
    }

    public CallDate(){

    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }

    public String getMonth() {
        return month;
    }

    public void setMonth(String month) {
        this.month = month;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    @Override
    public int compareTo(CallDate o) {
        return o.getYear().equals(this.getYear())&&o.getMonth().equals(this.getMonth())&&o.getDay().equals(this.getDay()) ? 0 : 1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.year);
        out.writeUTF(this.month);
        out.writeUTF(this.day);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readUTF();
        this.month = in.readUTF();
        this.day = in.readUTF();
    }
}
