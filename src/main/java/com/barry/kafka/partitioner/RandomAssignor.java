package com.barry.kafka.partitioner;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

/**
 * @author borui.shao@earlydata.com
 * Date 2020/8/21 8:42 AM
 * Version 1.0
 * Describe 消费者订阅分区器 策略：随机分配
 **/

public class RandomAssignor extends AbstractPartitionAssignor {
    /**
     *
     * @param partitionsPerTopic Map<topic, numPartitionsForTopic>
     * @param subscriptions Map<consumerId, subscript info>
     * @return  Map<consumerId, List<TopicPartition>>
     */
    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {
        Map<String, List<String>> consumerPerTopic = consumerPerTopic(subscriptions);
        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        for (String memberId : subscriptions.keySet()) {
            assignment.put(memberId, new ArrayList<>());
        }

        for (Map.Entry<String, List<String>> topicEntry : consumerPerTopic.entrySet()) {
            String topic = topicEntry.getKey();
            List<String> consumersForTopic = topicEntry.getValue();
            int consumerSize = consumersForTopic.size();
            Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
            if (numPartitionsForTopic == null){
                continue;
            }
            //看这里源码可以发现，分区号一定要是默认的从0开始累加的才能生效，如果自定义了分区号，就没用了。
            List<TopicPartition> partitions = AbstractPartitionAssignor.partitions(topic, numPartitionsForTopic);
            for (TopicPartition partition : partitions) {
                int rand = new Random().nextInt(consumerSize);
                String randomConsumer = consumersForTopic.get(rand);
                assignment.get(randomConsumer).add(partition);
            }
        }
        return assignment;
    }

    /**
     * 注册这个分配器的名字 不能重复
     */
    @Override
    public String name() {
        return "random";
    }

    /**
     * 获取每个topic对应的消费者列表
     *
     * @param consumerMetadata Map<consumerId, subscript info>
     * @return Map<topic, List < consumer_id>>
     */
    private Map<String, List<String>> consumerPerTopic(Map<String, Subscription> consumerMetadata) {
        Map<String, List<String>> res = new HashMap<>();
        for (Map.Entry<String, Subscription> subscriptionEntry : consumerMetadata.entrySet()) {
            String consumerId = subscriptionEntry.getKey();
            for (String topic : subscriptionEntry.getValue().topics()) {
                put(res, topic, consumerId);
            }
        }
        return res;
    }
}
