package com.run.test;

import java.util.ArrayList;
import java.util.List;

public class test {
    public static void main(String[] args) {
        String str = "01_手机号_yyyyMMddhhmmss_1";
        String[] split = str.split("_");
        String substring = split[2].substring(0, 6);
        System.out.println(substring);


        Long dateLong = 0L;
        // 相同手机号，年份月份
        // 统计通话时长，秒
        List<String> values = new ArrayList<>();
        values.add("0012");
        values.add("1234");
        values.add("0120");
        for(String calllong : values){
            // 装换
            Integer llong = Integer.valueOf(calllong.toString());
            System.out.println("ll: "+llong);
            dateLong =+ Long.valueOf(llong);
        }
        System.out.println("-----"+dateLong);
    }
}
