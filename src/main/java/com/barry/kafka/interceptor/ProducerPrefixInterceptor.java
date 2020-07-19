package com.barry.kafka.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * @author boruiShao
 * Date 2020/7/18 4:28 PM
 * Version 1.0
 * Describe 在发送时加上前缀，并计算发送的成功率
 **/

public class ProducerPrefixInterceptor implements ProducerInterceptor<String, String> {

    private volatile long sendSuccess = 0;
    private volatile long sendFailure = 0;


    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        String modifiedValue = "prefix1-" + record.value();
        return new ProducerRecord<>(record.topic(), record.partition(),
                record.timestamp(), record.key(),
                modifiedValue, record.headers());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            sendSuccess++;
        } else {
            sendFailure++;
        }
    }

    @Override
    public void close() {
        double successRatio = (double) sendSuccess / (sendSuccess + sendFailure);
        System.out.println("the success ratio is " + String.format("%f",successRatio*100) + "%");
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
