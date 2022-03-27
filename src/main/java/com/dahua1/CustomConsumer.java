package com.dahua1;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 消费者高级API
 */
public class CustomConsumer {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put("group.id", "g1");
        properties.put("zookeeper.connect", "192.168.139.130:2181");

        properties.put("zookeeper.session.timeout.ms", "500");
        properties.put("zookeeper.sync.time.ms", "250");
        properties.put("auto.commit.interval.ms", "1000");


        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(properties));
        HashMap<String, Integer> topicCount = new HashMap<String, Integer>();
        topicCount.put("dahua",1);


        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCount);
        KafkaStream<byte[], byte[]> stream = consumerMap.get("dahua").get(0);

        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
        while(iterator.hasNext()){
            System.out.println(new String(iterator.next().message()));
        }


    }
}
