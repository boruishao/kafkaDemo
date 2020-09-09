package com.barry.kafka.interceptor;

import com.barry.kafka.BytesUtils;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author borui.shao@earlydata.com
 * Date 2020/9/9 9:02 PM
 * Version 1.0
 * Describe TODO
 **/

public class ConsumerHeaderTtlInterceptor implements ConsumerInterceptor<String, String> {


    @Override
    public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
        long now = System.currentTimeMillis();
        Map<TopicPartition, List<ConsumerRecord<String, String>>> newRecords = new HashMap<>();
        for (TopicPartition partition : records.partitions()) {
            List<ConsumerRecord<String, String>> tpRecords = records.records(partition);
            List<ConsumerRecord<String, String>> newTpRecords = new ArrayList<>();
            for (ConsumerRecord<String, String> tpRecord : tpRecords) {
                Headers headers = tpRecord.headers();
                long ttl = -1;
                for (Header header : headers) {
                    if (header.key().equalsIgnoreCase("ttl")) {
                        ttl = BytesUtils.bytes2Long(header.value());
                    }
                }
                if (ttl > 0 && now - tpRecord.timestamp() < ttl * 1000) {
                    newTpRecords.add(tpRecord);
                } else if (ttl < 0) {
                    //未设置超时，不需要判断
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