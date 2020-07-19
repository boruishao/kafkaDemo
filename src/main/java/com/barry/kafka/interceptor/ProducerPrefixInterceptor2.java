package com.barry.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author boruiShao
 * Date 2020/7/18 4:49 PM
 * Version 1.0
 * Describe TODO
 **/

public class ProducerPrefixInterceptor2 implements ProducerInterceptor<String,String> {
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String modifiedValue = "prefix2-" + record.value();
        return new ProducerRecord<>(record.topic(), record.partition(),
                record.timestamp(), record.key(),
                modifiedValue, record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
