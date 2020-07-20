package com.barry.kafka.consumer;

import com.barry.kafka.bean.Company;
import com.barry.kafka.serializer.CompanyDeserialier;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * @author boruiShao
 * Date 2020/7/19 6:13 PM
 * Version 1.0
 * Describe TODO
 **/

public class KafkaConsumerAnalysis {

    public static final String brokerList = "localhost:9092,localhost:9093,localhost:9094";
    public static final String topic = "PARSE";
    public static final String groupId = "group-demo";
    public static final AtomicBoolean isRunning = new AtomicBoolean(true);
    public static final String clientId = "consumer.client.id.demo";

    public static Properties initConf() {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return properties;
    }

    public static void main(String[] args) {
//        normalRec();
//        assignPartition();
        deserRec();
    }

    private static void deserRec() {
        Properties properties = initConf();
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CompanyDeserialier.class.getName());
        KafkaConsumer<String, Company> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Pattern.compile("PAR.*"));
        receiveComp(consumer);

    }

    private static void receiveComp(KafkaConsumer<String, Company> consumer) {
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, Company> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, Company> record : records) {
                    System.out.println("topic is " + record.topic() + ", partition is " +
                            record.partition() + ", offset is " + record.offset());

                    System.out.println("key is " + record.key() + ", value is " + record.value());
                    //todo
                }
            }
        }catch (Exception e ){
            e.printStackTrace();
        }finally {
            consumer.close();
        }
    }

    private static void normalRec() {
        Properties properties = initConf();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));

        receive(consumer);
    }

    private static void receive(KafkaConsumer<String, String> consumer) {
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("topic is " + record.topic() + ", partition is " +
                            record.partition() + ", offset is " + record.offset());

                    System.out.println("key is " + record.key() + ", value is " + record.value());
                    //todo
                }
            }
        }catch (Exception e ){
            e.printStackTrace();
        }finally {
            consumer.close();
        }
    }

    private static void assignPartition(){
        Properties properties = initConf();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        List<TopicPartition> partitions = new ArrayList<>();
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        if(partitionInfos != null){
            for (PartitionInfo partitionInfo : partitionInfos) {
                partitions.add(new TopicPartition(topic,partitionInfo.partition()));
            }
        }
        consumer.assign(partitions);

        receive(consumer);

    }



}
