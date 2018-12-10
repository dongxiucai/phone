package com.run.pojo;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class CallPeople implements WritableComparable<CallPeople> {

    private String name; // 姓名
    private String phoneNum; // 电话号码

    public CallPeople(String name,String phoneNum){
        this.name = name;
        this.phoneNum = phoneNum;
    }

    public CallPeople(){

    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPhoneNum() {
        return phoneNum;
    }

    public void setPhoneNum(String phoneNum) {
        this.phoneNum = phoneNum;
    }

    @Override
    public int compareTo(CallPeople o) {
        return o.name.equals(this.name)&&o.phoneNum.equals(this.phoneNum) ? 0 : 1;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.name);
        out.writeUTF(this.phoneNum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.name = in.readUTF();
        this.phoneNum = in.readUTF();
    }
}
