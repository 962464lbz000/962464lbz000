package com.dahua;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.awt.*;
import java.util.Properties;

/*
过时生产者API
 */



public class OldProducerAPI {

    public static void main(String[] args) {
        Properties properties =new Properties();

        //kafka broker
        properties.put("metadata.broker.list","192.168.139.130:9092");
        // 写入时的响应码： ack的值：
        properties.put("request.required.acks", "0");
        // 写入时的序列化。
        properties.put("serializer.class", "kafka.serializer.StringEncoder");


        Producer<Integer,String> producer=new Producer<Integer,String>(new ProducerConfig(properties));
        KeyedMessage<Integer,String> message=new KeyedMessage<Integer, String>("dahua","hellokafka");
        producer.send(message);



    }


}
