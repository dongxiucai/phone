package com.run.dao;

import com.run.pojo.CallDate;
import com.run.pojo.CallPeople;

public interface Dimesion {

    // 返回手机号姓名的编号
    public int getDimensionID(CallPeople people);
    // 返回时间表的编号
    public int getDimensionID(CallDate date);
}
