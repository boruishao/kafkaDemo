package com.barry.kafka.interceptor;

import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author borui.shao@earlydata.com
 * Date 2020/7/31 8:01 AM
 * Version 1.0
 * Describe 超出10秒中的数据，将会被拦截
 **/

public class ConsumerTtlInterceptor implements ConsumerInterceptor<String, String> {

    public static final long EXPIRE_INTERVAL = 10 * 1000;

    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        long now = System.currentTimeMillis();
        Map<TopicPartition, List<ConsumerRecord<String, String>>> newRecords = new HashMap<>();
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, String>> tpRecords = records.records(partition);
            List<ConsumerRecord<String, String>> newTpRecords = new ArrayList<>();
            for (ConsumerRecord<String, String> tpRecord : tpRecords) {
                if (now - tpRecord.timestamp() < EXPIRE_INTERVAL) {
                    newTpRecords.add(tpRecord);
                }
            }
            if (!newTpRecords.isEmpty()) {
                newRecords.put(partition, newTpRecords);
            }
        }

        return new ConsumerRecords<>(newRecords);
    }

    @Override
    public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
        offsets.forEach((tp, offset) -> {
            System.out.println("on commit ttl interceptor : " + tp + ":" + offset.offset());
        });
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
