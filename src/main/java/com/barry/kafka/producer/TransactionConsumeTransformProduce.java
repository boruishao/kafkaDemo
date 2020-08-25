package com.barry.kafka.producer;

import com.barry.kafka.consumer.KafkaConsumerAnalysis;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.time.Duration;
import java.util.*;

/**
 * @author borui.shao@earlydata.com
 * Date 2020/8/25 9:16 PM
 * Version 1.0
 * Describe 一个事务的例子 模仿流式处理 source - transform - sink 在一个事务中
 **/

public class TransactionConsumeTransformProduce {
    public static final String sourceTopic = "topic-source";
    public static final String sinkTopic = "topic-sink";

    public static void main(String[] args) {

        Properties consumerConf = KafkaConsumerAnalysis.initConf();
        Properties producerConf = KafkaProducerAnalysis.initConf();
        producerConf.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactionalId");
        consumerConf.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConf);
        consumer.subscribe(Arrays.asList(sourceTopic));
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerConf);

        //初始化事务
        producer.initTransactions();

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            if (!records.isEmpty()) {
                Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
                //开启事务
                producer.beginTransaction();
                try {
                    for (TopicPartition partition : records.partitions()) {
                        List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                        for (ConsumerRecord<String, String> record : partitionRecords) {

                            System.out.println("topic is " + record.topic() + ", partition is " +
                                    record.partition() + ", offset is " + record.offset());
                            System.out.println("key is " + record.key() + ", value is " + record.value());

                            //do some logic process
                            ProducerRecord<String, String> producerRecord =
                                    new ProducerRecord<>(sinkTopic, record.key(), record.value());
                            try {
                                producer.send(producerRecord,
                                        (RecordMetadata metadata, Exception e) -> {
                                            if (e != null) {
                                                e.printStackTrace();
                                            } else {
                                                System.out.println("Async : " + metadata.topic() + " - " +
                                                        metadata.partition() + " - " + metadata.offset());

                                            }
                                        }
                                );
                            } catch (Exception e) {
                                e.printStackTrace();
                            }

                        }
                        long lastConsumedOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                        offsets.put(partition, new OffsetAndMetadata(lastConsumedOffset + 1));
                    }
                    //提交消费位移
                    producer.sendOffsetsToTransaction(offsets, KafkaConsumerAnalysis.groupId);
                    //提交事务
                    producer.commitTransaction();
                } catch (ProducerFencedException e) {
                    //终止事务
                    producer.abortTransaction();
                }


            }
        }

    }
}
