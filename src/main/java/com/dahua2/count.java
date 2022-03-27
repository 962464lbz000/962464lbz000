package com.dahua2;



import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.*;

public class count {


    public static void main(String[] args) {
        Properties props = new Properties();
        // 定义kakfa 服务的地址，不需要将所有broker指定上
        props.put("bootstrap.servers", "192.168.139.130:9092");
        // 制定consumer group
        props.put("group.id", "test");
        // 是否自动确认offset
        props.put("enable.auto.commit", "true");
        // 自动确认offset的时间间隔
        props.put("auto.commit.interval.ms", "1000");
        // key的序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // value的序列化类
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 定义consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        //设置消费者订阅一个主题
        consumer.subscribe(Collections.singletonList("first"));

        HashMap<String, Integer> stringIntegerHashMap = new HashMap<String, Integer>(1);

        long offset = -1;

        //使用无线循环来持续监听
        while (true) {
            ConsumerRecords<String, String> poll = consumer.poll(100);
            for (ConsumerRecord<String, String> consumerRecord : poll) {
                offset = consumerRecord.offset();
                if (offset > 0) {

                    //判断监听到的词是否是map的key如果是 就将value+1 否则就初始化为1
                    if (stringIntegerHashMap.containsKey(consumerRecord.value())) {
                        stringIntegerHashMap.put(consumerRecord.value(), stringIntegerHashMap.get(consumerRecord.value()) + 1);
                    } else {
                        stringIntegerHashMap.put(consumerRecord.value(), 1);
                    }
                }
            }
            if (offset > 0) {
                //遍历集合
                System.out.println("-------------------------------------");
                for (String str : stringIntegerHashMap.keySet()) {
                    //重点 有不知名特殊字符 导致在控制台显示不出来，要去掉
                    StringBuffer sb = new StringBuffer(str.trim());
                    System.out.println("数据：" + sb + "  " + stringIntegerHashMap.get(str));
                }
                System.out.println("-----------------------------------------");
                //偏移量初始化
                offset = -1;
            }
        }
    }

}





