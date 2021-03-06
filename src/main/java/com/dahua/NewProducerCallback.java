package com.dahua;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;


/*
创建生产者带回调函数（新API）
 */
public class NewProducerCallback {





    public static void main(String[] args) {
        Properties props = new Properties();
        // Kafka服务端的主机名和端口号
        props.put("bootstrap.servers", "192.168.139.130:9092");
        // 等待所有副本节点的应答
        props.put("acks", "all");
        // 消息发送最大尝试次数
        props.put("retries", 0);
        // 一批消息处理大小
        props.put("batch.size", 16384);
        // 增加服务端请求延时
        props.put("linger.ms", 1);
        // 发送缓存区内存大小
        props.put("buffer.memory", 33554432);
        // key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String,String>(props);

        for(int i=0;i<50;i++){
            kafkaProducer.send(new ProducerRecord<String, String>("dahua","hello"+i), new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(recordMetadata !=null){
                        System.out.println(recordMetadata.partition()+"-"+recordMetadata.offset());
                    }
                }
            });
        }

        kafkaProducer.close();

    }




}
