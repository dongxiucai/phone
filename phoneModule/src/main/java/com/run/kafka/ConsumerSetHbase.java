package com.run.kafka;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * kafka消费数据
 * 存储到hbase中
 */
public class ConsumerSetHbase {

    // topic
    private static final String TOPIC_NAME = "call";
    // 线程数量
    private static final Integer THREAD_SIZE = 2;


    public static void main(String[] args) throws Exception{
        // 配置相应属性
        Properties prop = new Properties();
        // 配置zk信息
        prop.put("zookeeper.connect", "spark101:2181,spark102:2181,spark103:2181");
        // 配置消费组
        prop.put("group.id","phone");
        // 配置从头消费数据
        //prop.put("auto.offset.reset","smallest");
        //
        ConsumerConfig config = new ConsumerConfig(prop);
        // 创建消费者
        ConsumerConnector con = kafka.consumer.Consumer.createJavaConsumerConnector(config);
        // 创建map，主要存储Topic信息
        HashMap<String,Integer> map = new HashMap<>();
        map.put(TOPIC_NAME,THREAD_SIZE);
        // 创建获取信息
        Map<String, List<KafkaStream<byte[],byte[]>>> message = con.createMessageStreams(map);
        List<KafkaStream<byte[],byte[]>> kafkaStrems = message.get(TOPIC_NAME);
        // 循环接受map内的Topic数据
        for(KafkaStream<byte[],byte[]> stream : kafkaStrems){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    for(MessageAndMetadata<byte[],byte[]> m : stream){
                        // 获得log日志消息
                        String log = new String(m.message());
                        // 把消息存储到hbase上
                        // 初始化
                        HbaseUtil.init("spark101,spark102,spark103","2181");
                        // 存储set
                        try {
                            // 判断log不为空
                            if(StringUtils.isNotBlank(log)){
                                HbaseUtil.setLog(log);
                                System.out.println("log: "+log);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
            }).start();
        }
    }
}
