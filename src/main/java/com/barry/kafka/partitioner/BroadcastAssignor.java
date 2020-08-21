package com.barry.kafka.partitioner;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author borui.shao@earlydata.com
 * Date 2020/8/21 9:11 AM
 * Version 1.0
 * Describe 消费者订阅分区器 策略：组内广播，打破kafka一个分区只能被一个消费者消费的禁忌。
 *  默认的位移提交会相互覆盖，导致丢数据或重复消费。
 *  可以把每个消费者的位移保存到外部来实现安全管理位移。
 **/

public class BroadcastAssignor extends AbstractPartitionAssignor {
    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {
        Map<String, List<String>> consumerPerTopic = consumerPerTopic(subscriptions);
        Map<String, List<TopicPartition>> assignment = new HashMap<>();

        subscriptions.keySet().forEach(memberId -> {
            assignment.put(memberId, new ArrayList<>());
        });

        consumerPerTopic.entrySet().forEach(topicEntry -> {
            String topic = topicEntry.getKey();
            List<String> members = topicEntry.getValue();
            Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
            if (numPartitionsForTopic == null || members.isEmpty()) {
                return;
            }
            List<TopicPartition> partitions =
                    AbstractPartitionAssignor.partitions(topic, numPartitionsForTopic);

            if (!partitions.isEmpty()){
                members.forEach(memberId->{
                    assignment.get(memberId).addAll(partitions);
                });
            }
        });
        return assignment;
    }

    @Override
    public String name() {
        return "broadcast";
    }

    /**
     * 获取每个topic对应的消费者列表
     *
     * @param consumerMetadata
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
