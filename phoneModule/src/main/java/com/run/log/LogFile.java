package com.run.log;

import java.io.*;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * 生产log日志
 */
public class LogFile {
    // 测试log
    static int size = 0;
    // 编号，姓名&手机
    static Map<Integer,String> nameAndPhone = new HashMap<Integer, String>();

    /**
     * 初始化数据
     * 加载数据
     * @param path
     */
    public static void jiazai(String path){
        // 加载数据
        // 存放姓名，手机号
        int length = 0;
        try {
            // 读取数据
            File file = new File(path);
            // 获取到缓冲字节输入流
            BufferedReader input = new BufferedReader(new FileReader(file));
            // 读取到一行数据
            String line = null;
            //System.out.println("==="+line);
            // 循环读取
            while((line = input.readLine()) != null ){
                // 长度+1
                length++;
                // line为 姓名 手机号
                String[] split = line.split(" ");
                // 存到Map中
                nameAndPhone.put(length,split[0]+"&"+split[1]);
            }
            // 全局变量
            size = length;
            // 关闭输出流
            input.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 随机打电话
     * @return
     */
    public static String suiji(){
        // 获取随机抽取两个数据
        Random ra = new Random();
        // 随机打电话方
        int r1 = ra.nextInt(size)+1;
        // 随机接电话方
        int r2 = ra.nextInt(size)+1;
        // 判断是否相同
        Boolean bl = (r1 == r2);
        while(bl){
            // 相同在随机
            if(r2 == r1){
                r2 = ra.nextInt(size)+1;
            }else {
                // 不相同
                bl = false;
            }
        }
        // 封装字符串
        // 取出打电话方
        String call = nameAndPhone.get(r1);
        String[] na_ph_ = call.split("&");
        // 获取接电话方
        String callee = nameAndPhone.get(r2);
        String[] na_ph_ee = callee.split("&");
        // 获取当前时间
        Date date = new Date();
        // 格式化时间
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        String forDate = sdf.format(date);
        // 获取时间长短
        //随机9999
        int cd = ra.nextInt(9999);
        DecimalFormat df = new DecimalFormat("0000");//设置格式
        String strCd = df.format(cd);//格式转换

        // 拼接最终的字符串
        // 打电话者 手机号 接电话者 手机号 年 月日 时间间隔
        String str = na_ph_[0]+"\t"+na_ph_[1]+"\t"+na_ph_ee[0]+"\t"+na_ph_ee[1]+"\t"+forDate+"\t"+strCd;
        // 返回
        return str;
    }

    // 输出到文件
    public static void outFile(String line, BufferedWriter out) throws Exception{
        // 写入一行
        out.write(line+"\n");
        out.flush();
    }


    public static void main(String[] args) throws Exception{
        // 输出文件
        // 文件输出流
        File file = new File(args[1]);
        if(!file.exists()){
            file.createNewFile();
        }
        // true append 追加数据
        BufferedWriter out = new BufferedWriter(new FileWriter(file,true));
        // 加载数据
        jiazai(args[0]);
        // 随机打电话
        while (true){
            //System.out.println(suiji());
            // 输入到文件
            outFile(suiji(),out);
            Thread.sleep(1000);
        }
        // 关闭输入流
        // out.close();
    }
}
