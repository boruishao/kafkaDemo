package com.barry.kafka.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author borui.shao@earlydata.com
 * Date 2020/7/26 1:20 PM
 * Version 1.0
 * Describe TODO
 **/

public class ControlOffset {

    /**
     * 从指定的offset 开始消费，如果offset 在日志中找不到，则会走 auto.offset.reset 的规则
     * 实际应用中，这个offset可以存储到第三方数据库中，
     * 当然在消费提交位移的时候也要吧offset写到数据库中
     *
     * @param consumer
     */
    public void seekPointOffset(KafkaConsumer<String, String> consumer, long pointOffset) {
        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0) {
            //consumer 从poll方法中拉取元数据，但是从调用方法到获取元数据需要一定时间，
            //所以反复调用，并检查是否拉取到 分区等元数据了
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }
        for (TopicPartition topicPartition : assignment) {
            consumer.seek(topicPartition, pointOffset);
        }
        KafkaConsumerAnalysis.receive(consumer);
    }

    /**
     * 通过seek的方式，指定从开始或者结尾处消费
     *
     * @param consumer
     * @param beginOrEnd
     */
    public void seekToBeginOrEnd(KafkaConsumer<String, String> consumer, String beginOrEnd) {
        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0) {
            //consumer 从poll方法中拉取元数据，但是从调用方法到获取元数据需要一定时间，
            //所以反复调用，并检查是否拉取到 分区等元数据了
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }
        /***********************方式一********************************************/
        Map<TopicPartition, Long> offsets;
//        if ("begin".equals(beginOrEnd)) {
//            offsets = consumer.beginningOffsets(assignment);
//        } else {
//            offsets = consumer.endOffsets(assignment);
//        }
//
//        for (TopicPartition topicPartition : assignment) {
//            consumer.seek(topicPartition, offsets.get(assignment));
//        }
        /***********************方式二*******************************************/
        if ("begin".equals(beginOrEnd)) {
            consumer.seekToBeginning(assignment);
        } else {
            consumer.seekToEnd(assignment);
        }

        KafkaConsumerAnalysis.receive(consumer);
    }

    /**
     * 跳转到 给定时间戳之后第一条的位置
     *
     * @param consumer
     * @param timeStamp
     */
    public void seekToTimeStamp(KafkaConsumer<String, String> consumer, long timeStamp) {
        Set<TopicPartition> assignment = new HashSet<>();
        while (assignment.size() == 0) {
            //consumer 从poll方法中拉取元数据，但是从调用方法到获取元数据需要一定时间，
            //所以反复调用，并检查是否拉取到 分区等元数据了
            consumer.poll(Duration.ofMillis(100));
            assignment = consumer.assignment();
        }
        //搜索对象，key是分区，值是要查找的时间戳
        Map<TopicPartition, Long> timeSearch = new HashMap<>();
        for (TopicPartition topicPartition : assignment) {
            timeSearch.put(topicPartition, timeStamp);
        }
        //搜索offset 通过指定的时间戳
        Map<TopicPartition, OffsetAndTimestamp> offsets = consumer.offsetsForTimes(timeSearch);

        //把找到的offset 通过seek指定位移的方式，指定
        for (TopicPartition topicPartition : assignment) {

            OffsetAndTimestamp offsetAndTimestamp = offsets.get(topicPartition);
            if (offsetAndTimestamp != null) {
                consumer.seek(topicPartition, offsetAndTimestamp.offset());
            }
        }

        KafkaConsumerAnalysis.receive(consumer);
    }

}
