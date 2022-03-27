package com.dahua1;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

public class CustomNewConsumer {

    public static void main(String[] args) {
        Properties props = new Properties();
        // 定义kakfa 服务的地址，不需要将所有broker指定上
        props.put("bootstrap.servers", "192.168.139.130:9092");
        // 制定consumer group
        props.put("group.id", UUID.randomUUID().toString());
        // 是否自动确认offset
        props.put("enable.auto.commit", "true");
        // 自动确认offset的时间间隔
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");
        // key的序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // value的序列化类
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 消费数据。
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        // 订阅主题。
        consumer.subscribe(Arrays.asList("dahua"));

        while(true){
            ConsumerRecords<String, String> records = consumer.poll(100);
            for(ConsumerRecord<String,String> record : records){
                System.out.println("offset:"+record.offset()+"\t"+"key:"+record.key()+"\t"+"value:"+record.value());
            }
        }




    }
}
