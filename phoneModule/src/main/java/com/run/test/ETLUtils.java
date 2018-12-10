package com.run.test;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ETLUtils {

    // 存放图片
    public static String detailsImg = "";

    // get page
    public static String page(String lines){
        return "";
    }

    // 酒店房间图片
    public static String imgUrls(String lines){
        // 去掉]
        String imgUrls = lines.replaceAll("\\]", "");
        // 去除空url
        imgUrls = imgUrls.replaceAll(" ,", "");
        if (imgUrls.substring(imgUrls.length() - 2, imgUrls.length() - 1).equals(",")) {
            imgUrls = imgUrls.substring(0, imgUrls.length() - 2);
        }
        if(imgUrls.equals("")){
           imgUrls = "-";
        }
        return imgUrls;
    }

    // 酒店房间平价
    public static String evaluate(String lines){
        // 去掉]
        String evaluate = lines.replaceAll("\\]", "");
        evaluate = evaluate.replaceAll("酒店房间平价\\[","");
        // 判断是否为空
        if (evaluate.equals("")) {
            // 赋值
            evaluate = "-";
        } else {
            // 不为空，过滤a标签
            // 按照<a id='xmy'  href=切分
            evaluate = evaluate.replaceAll("<a[^<]*?>", "");
            // 去除</a>
            evaluate = evaluate.replaceAll("</a>", "");
            // 去除 【xxxx】
            evaluate = evaluate.replaceAll("【[^<]*?】", "");
        }
        return evaluate;
    }


    // 酒店房间信息
    public static String information(String lines){
        // 去掉]
        String information = lines.replaceAll("\\]", "");
        information = information.replaceAll("酒店房间信息\\[","");
        // 判断是否为空
        if (information.equals("")) {
            // 赋值
            information = "-";
        } else {
            // 不为空
        }
        return information;
    }

    // 酒店房间详情,及其他
    public static String details(String lines){
        String details = lines;
        // 去掉div标签
        details = details.replaceAll("<div[^<]*?>", "").replaceAll("</div>", "");
        // 去掉p标签
        details = details.replaceAll("<p[^<]*?>", "").replaceAll("</p>", "");
        // 去掉span标签
        details = details.replaceAll("<span[^<]*?>", "").replaceAll("</span>", "");
        // 去掉【】
        details = details.replaceAll("【[^<]*?】", "");
        // h1 h3
        details = details.replaceAll("<h1[^<]*?>", "").replaceAll("</h1>","");
        details = details.replaceAll("<h3[^<]*?>", "").replaceAll("</h3>","");
        // b
        details = details.replaceAll("<b[^<]*?>", "").replaceAll("</b>","");
        // n
        details = details.replaceAll("<n[^<]*?>", "").replaceAll("</n>","");
        // strong
        details = details.replaceAll("<strong[^<]*?>", "").replaceAll("</strong>", "");
        // 提取img标签内的url
        String srcData = "";
        // img标签匹配正则
        String imgStr = "<(img|IMG)(.*?)(/>|></img>|>)";
        // 匹配
        Pattern p_img = Pattern.compile(imgStr);
        Matcher m_img = p_img.matcher(details);
        if(m_img.find()){
            // 匹配内容
            String imgData = m_img.group(2);
            // 匹配src
            String srcStr = "(src|SRC)=(\\\"|\\')(.*?)(\\\"|\\')";
            Pattern p_src = Pattern.compile(srcStr);
            Matcher m_src = p_src.matcher(imgData);
            if(m_src.find()){
                // 获取到src的值
                srcData = m_src.group(3);
            }
        }
        // 拼接详情imgurl 中间用 | 分割
        if(!srcData.equals("")){
            detailsImg = detailsImg + "|" + srcData;
        }
        // img
        details = details.replaceAll("<img[^<]*?>", "").replaceAll("</img>","");
        // 把<br>替换为\n
        details = details.replaceAll("<br>", "");
        return details;
    }

    // 初始化
    public static void initImg(){
        detailsImg = "";
    }
}
