package com.run.test;

import org.apache.hadoop.hbase.master.MetricsMasterFileSystemSource;

import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class test {
    public static void main(String[] args) {
        /*String str = "01_手机号_yyyyMMddhhmmss_1";
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
        System.out.println("-----"+dateLong);*/

//        String str = "酒店房间图片[http://pic.lvmama.com/uploads/pc/place_vst/hotels/1511/15111/_i_Mobile640_960_00005euB_480_320.jpg, , , , , , ]";
//        String[] split = str.split("\\[");
//        for(String s : split){
//            System.out.println(" ==== :  "+s);
//        }
//
//        String str1 = "http://pic.lvmama.com/uploads/pc/place_vst/hotels/1511/15111/_i_Mobile640_960_00005euB_480_320.jpg, wwwwwww, ";
//        String s = str1.replaceAll(" ,", "");
//        if(s.substring(s.length() - 2, s.length() - 1).equals(",")){
//            s = s.substring(0,s.length()-2);
//        }
//
//        System.out.println("=== : "+s);
//
//        String str2 = "酒店房间平价[]";
//        String[] split1 = str2.split("\\[");
//        String evaluate = split1[1].replaceAll("\\]", "");
//        boolean equals = evaluate.equals("");
//        System.out.println(equals);
//        System.out.println(evaluate);
//        for(String s1 : split1){
//            System.out.println("-----: "+s1);
//        }


        String pingjia = "很开心的一次<a id='xmy'  href='http://dujia.lvmama.com/tour/wenzhou107'>温州</a>旅行，住的酒店" +
                "也很舒服，推荐, " +
                "这家酒店在<a id='xmy'  href='http://ticket.lvmama.com/a-wenzhou107'>温州</a>市区中心城区，" +
                "周边交通便利，购物方便，我们住的是13层的行政房，就是马桶的地方太紧凑了点，" +
                "其他都不错?早餐嘛在四星级标准里还是值得的！, 早餐真的很赞，打扫也非常及时，而且" +
                "地理位置是我最喜欢的一个酒店，购物还是吃饭都很方便，每次来<a id='xmy'  href='http://ticket." +
                "lvmama.com/a-wenzhou107'>温州</a>只住这家酒店, 亲子房可以加床，下午16点多入住的，12点" +
                "后也是可以入住的！房间内部还可以的。有小孩子玩的滑滑梯，木马等，特别是外景很美，交通便利！" +
                "楼下人民路，离五马街近、开太百货商场等。, 酒店地理位置不错，离五马街，女人街很" +
                "近，楼下就是开泰百货，但是酒店设施比较陈旧。厕所是浴缸，看着脏脏的，洗澡不" +
                "方便！, 出差体验非常赞 就是卫生需要注意一下 特别是灯台位置灰尘比较多。工作人员也很" +
                "有礼貌 下次会继续关注哒, 一般, 【点评有奖第11季】房间有点小，毕竟很久的房子了,隔音也差，居然隔壁邻居投" +
                "诉我们用水声音大，只能呵呵了！只有浴缸，感觉还是淋浴房比较卫生，唯一的优点是地理位置方便，但大过年的出门也没几家开的" +
                "。顶楼的早餐餐厅空气不流通，很闷，老爸都头晕了，希望可以改善。, 还算不错，预订时没有看到平方数，一" +
                "个人住的话，还是觉得小了一点~, " +
                "【点评有奖第6季】还可以就是停车要钱还好钱也不多，就10元，还可以的值得吧算是";
        // 按照<a id='xmy'  切分
       /* // 去除</a>
        pingjia = pingjia.replaceAll("</a>", "");
        // 切分
        String[] split = pingjia.split("<a id='xmy'  href=");

        String pj = "";
        // 循环
        for(String spl : split){
               String[] split1 = spl.split(">");
               if(split1.length > 1){
                   pj = pj + split1[1];
               }else {
                   pj = pj + split1[0];
               }
        }
       System.out.println(pj);
        pj = pj.replaceAll("【.*】","");
        System.out.println(pj);*/


       /* String aa = "很开心的一次<a id='xmy'  href='http://dujia.lvmama.com/tour/wenzhou107'>温州</a>旅行，住的酒店";
        String s = pingjia.replaceAll("</a>", "");
        System.out.println(s);
        System.out.println("==========================");
        s = s.replaceAll("<a[^<]*?>", "");
        System.out.println(s);
        System.out.println("===================");
        s = s.replaceAll("【[^<]*?】", "");
        System.out.println(s);


        String page = "get page: http://hotels.lvmama.com/hotel/45422.html";
        String[] split = page.split("\\[");
        for (String s1 : split) {
            System.out.println("s: " + s1);
        }*/



        String str = "<img alt=\"\" src=\"http://pic.lvmama.com/uploads/pc/place2/2016-02-26/a0881e11-60ae-43d6-a57b-5ee0f5ae224f.jpg\" width=\"90%\">";

        // <(img|IMG)(.*?)(/>|></img>|>)
       // String img = "<img(.*?)(/>)>";
        String img = "<(img|IMG)(.*?)(/>|></img>|>)";
        Pattern p_img = Pattern.compile(img);
        Matcher m_img = p_img.matcher(str);
        boolean b = m_img.find();
        if(b){
            // 匹配内容
            String src = m_img.group(2);
            System.out.println("---: "+src);
            // 匹配src
            String src1 = "(src|SRC)=(\\\"|\\')(.*?)(\\\"|\\')";
            Pattern p_src = Pattern.compile(src1);
            Matcher m_src = p_src.matcher(src);
            if(m_src.find()){
                String group = m_src.group(3);
                System.out.println("===: "+group);
            }
        }


    }
}
