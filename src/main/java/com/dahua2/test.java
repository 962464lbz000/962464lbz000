package com.dahua2;

import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.*;

public class test {
    /**
     * broker list
     */
    private static final String BROKER_LIST = "192.168.139.130:9092,192.168.139.131:9092,192.168.139.132:9092";

    /**
     * 连接超时时间：1min
     */
    private static final int TIME_OUT = 60 * 1000;
    /**
     * 读取消息缓存区大小：1M
     */
    private static final int BUFFER_SIZE = 1024 * 1024;
    /**
     * 每次获取消息的条数
     */
    private static final int FETCH_SIZE = 100000;
    /**
     * 发生错误时重试的次数
     *///
    private static final int RETRIES_TIME = 3;
    /**
     * 允许发生错误的最大次数
     */
    private static final int MAX_ERROR_NUM = 3;

    /**
     * 获取指定主题指定分区的元数据
     */
    private PartitionMetadata fetchPartitionMetadata(List<String> brokerList, String topic, int partitionId) {
        SimpleConsumer consumer = null;
        TopicMetadataRequest metadataRequest = null;
        TopicMetadataResponse metadataResponse = null;
        List<TopicMetadata> topicMetadatas = null;

        try{
            /*
             * 循环是因为不确定传入的partition的leader节点是哪个
             */
            for(String host : brokerList) {
                // 1. 构建一个消费者SimpleConsumer，它是获取元数据的执行者
                String[] hostsAndPorts = host.split(":");
                // 最后一个参数是 clientId
                consumer = new SimpleConsumer(hostsAndPorts[0], Integer.parseInt(hostsAndPorts[1]),
                        TIME_OUT, BUFFER_SIZE, topic + "-" + "0" + "-" + "client");

                // 2. 构造请求主题元数据信息的请求 TopicMetadateRequest
                metadataRequest = new TopicMetadataRequest(Arrays.asList(topic));

                // 3. 通过send()正式与代理通信，发送TopicMetadateRequest请求获取元数据
                try {
                    metadataResponse = consumer.send(metadataRequest);
                } catch (Exception e) {
                    //有可能与代理失去连接
                    System.out.println("get TopicMetadataResponse failed!");
                    e.printStackTrace();
                    continue;
                }

                // 4. 获取主题元数据TopicMetadata列表，每个主题的每个分区的元数据信息对应一个TopicMetadata对象
                topicMetadatas = metadataResponse.topicsMetadata();

                // 5. 遍历主题元数据信息列表
                for(TopicMetadata topicMetadata : topicMetadatas) {

                    //6. 获取当前分区对应元数据信息PartitionMetadata
                    for(PartitionMetadata partitionMetadata : topicMetadata.partitionsMetadata()) {
                        if(partitionMetadata.partitionId() != partitionId) {
                            continue;
                        } else {
                            return partitionMetadata;
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("Fetch PartitionMetadata failed!");
            e.printStackTrace();
        } finally {
            if(consumer != null) {
                consumer.close();
            }
        }

        return null;

    }

    /**
     * 根据分区的元数据信息获取它的leader节点
     */
    private String getLeader(PartitionMetadata metadata) {
        if(metadata.leader() == null) {
            System.out.println("can not find partition" + metadata.partitionId() + "'s leader!");
            return null;
        }
        return metadata.leader().host()+"_"+metadata.leader().port();
    }

    /**
     * 获取指定主题指定分区的消费偏移量
     */
    private long getOffset(SimpleConsumer consumer, String topic, int partition, long beginTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        /*
         * PartitionOffsetRequestInfo(beginTime, 1)用于配置获取offset的策略
         * beginTime有两个值可以取
         *     kafka.api.OffsetRequest.EarliestTime()，获取最开始的消费偏移量，不一定是0，因为segment会删除
         *     kafka.api.OffsetRequest.LatestTime()，获取最新的消费偏移量
         */
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(beginTime, 1));
        // 构造获取offset的请求
        OffsetRequest request = new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);
        if(response.hasError()) {
            System.out.println("get offset failed!" + response.errorCode(topic, partition));
            return -1;
        }
        long[] offsets = response.offsets(topic, partition);
        if(offsets == null || offsets.length == 0) {
            System.out.println("get offset failed! offsets is null");
            return -1;
        }
        return offsets[0];
    }


    /**
     * 重新寻找partition的leader节点的方法
     */
    private String findNewLeader(List<String> brokerList, String oldLeader, String topic, int partition) throws Exception {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = fetchPartitionMetadata(brokerList, topic, partition);
            if (metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                // 这里考虑到 zookeeper 还没有来得及重新选举 leader 或者在故障转移之前挂掉的 leader 又重新连接的情况
                goToSleep = true;
            } else {
                return metadata.leader().host();
            }
            if (goToSleep) {
                Thread.sleep(1000);
            }
        }
        System.out.println("Unable to find new leader after Broker failure!");
        throw new Exception("Unable to find new leader after Broker failure!");
    }


    /**
     * 处理数据的方法
     */
    public void consume(List<String> brokerList, String topic, int partition) {
        SimpleConsumer consumer = null;
        try {
            // 1. 获取分区元数据信息
            PartitionMetadata metadata = fetchPartitionMetadata(brokerList,topic, partition);
            if(metadata == null) {
                System.out.println("can not find metadata!");
                return;
            }
            // 2. 找到分区的leader节点
            String leaderBrokerAndPort = getLeader(metadata);
            String[] brokersAndPorts = leaderBrokerAndPort.split("_");
            String leaderBroker = brokersAndPorts[0];
            int  port = Integer.parseInt(brokersAndPorts[1]);
            String clientId = topic + "-" + partition + "-" + "client";

            // 3. 创建一个消费者用于消费消息
            consumer = new SimpleConsumer(leaderBroker ,port ,TIME_OUT, BUFFER_SIZE, clientId);

            // 4. 配置获取offset的策略为，获取分区最开始的消费偏移量
//            System.out.println(kafka.api.OffsetRequest.EarliestTime());
            // 当前的数据偏移量。
            long offset = getOffset(consumer, topic, partition, kafka.api.OffsetRequest.EarliestTime(), clientId);
//            System.out.println("**************"+offset);



            int errorCount = 0;
            kafka.api.FetchRequest request = null;
            FetchResponse response = null;
            HashMap<String,Integer> map = new HashMap<String, Integer>();
            HashMap<String,Integer> map2= new HashMap<String, Integer>();
            while(offset > -1) {
                // 运行过程中，可能因为处理错误，把consumer置为 null，所以这里需要再实例化
                if(consumer == null) {
                    consumer = new SimpleConsumer(leaderBroker ,port , TIME_OUT, BUFFER_SIZE, clientId);
                }
                // 5. 构建获取消息的request
                request = new FetchRequestBuilder().clientId(clientId).addFetch(topic, partition, offset, FETCH_SIZE).build();
                // 6. 获取响应并处理
                response = consumer.fetch(request);
                if(response.hasError()) {
                    errorCount ++;
                    if(errorCount > MAX_ERROR_NUM) {
                        break;
                    }
                    short errorCode = response.errorCode(topic, partition);

                    if(ErrorMapping.OffsetOutOfRangeCode() == errorCode) {
                        // 如果是因为获取到的偏移量无效，那么应该重新获取
                        // 这里简单处理，改为获取最新的消费偏移量
                        offset = getOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime(), clientId);
                        continue;
                    } else if (ErrorMapping.OffsetsLoadInProgressCode() == errorCode) {
                        Thread.sleep(300000);
                        continue;
                    } else {
                        consumer.close();
                        consumer = null;
                        // 更新leader broker
                        leaderBroker = findNewLeader(brokerList, leaderBroker, topic, partition);
                        continue;
                    }
                    // 如果没有错误
                } else {
                    // 清空错误记录
                    errorCount = 0;
                    long fetchCount = 0;
                    // 处理消息
                    for(MessageAndOffset messageAndOffset : response.messageSet(topic, partition)) {
                        long currentOffset = messageAndOffset.offset();
                        if(currentOffset < offset) {
                            System.out.println("get an old offset[" + currentOffset + "], excepted offset is offset[" + offset + "]");
                            continue;
                        }
                        offset = messageAndOffset.nextOffset();
                        ByteBuffer payload = messageAndOffset.message().payload();
                        byte[] bytes = new byte[payload.limit()];
                        payload.get(bytes);
                        String key = new String(bytes, "UTF-8");
                        if(map.containsKey(key)){
                            map.put(key, map.get(key)+1);
                        }else{
                            map.put(key, 1);
                        }

                        if (map2.get(key)==null){
                            map2.put(key,1);
                            System.out.println(key.trim()+"\t" +map2.get(key));
                        }else {
                            int count1=  map2.get(key)+1;
                            map2.put(key,count1);
                            System.out.println(key.trim()+"\t" +map2.get(key));
                        }


                    }

                    if (fetchCount == 0) {
                        Thread.sleep(1000);
                    }
                }

            }
            for (Map.Entry<String, Integer> entry : map2.entrySet()) {
                String key2 = entry.getKey();
                Integer value = entry.getValue();
                System.out.println(key2.trim() + ":" + value);
            }
        } catch (Exception e) {
            System.out.println("exception occurs when consume message");
            e.printStackTrace();
        } finally {
            if (consumer != null) {
                consumer.close();
            }
        }
    }

    public static void main(String[] args) {
        test lowConsumerAPI = new test();
        lowConsumerAPI.consume(Arrays.asList(BROKER_LIST.split(",")), "first", 0);
    }

}
