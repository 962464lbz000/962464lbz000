package com.dahua;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;



import java.util.Properties;

/*
自定义分区生产者
 */

public class ProducerPartition {


    public static void main(String[] args) {

        Properties props = new Properties();
        // Kafka服务端的主机名和端口号
        props.put("bootstrap.servers", "192.168.139.130:9092");
        // 等待所有副本节点的应答// 副本应答：0 1 all
        props.put("acks", "all");
        // 消息发送最大尝试次数
        props.put("retries", 0);
        // 一批消息处理大小
        props.put("batch.size", 16384);
        // 请求延时
        props.put("linger.ms", 1);
        // 发送缓存区内存大小
        props.put("buffer.memory", 33554432);
        // key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 指定分区
        props.put("partitioner.class", "com.dahua.MyPartition");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        for(int i=0;i<50;i++){
            producer.send(new ProducerRecord<String, String>("dahua",Integer.toString(i),"nginx--"+i));
        }

        producer.close();
    }



    }


