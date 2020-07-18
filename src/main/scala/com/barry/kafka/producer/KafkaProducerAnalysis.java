package com.barry.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * @author boruiShao
 * Date 2020/7/14 9:06 PM
 * Version 1.0
 * Describe TODO
 **/

public class KafkaProducerAnalysis {

    public static final String brokerList = "localhost:9092,localhost:9093,localhost:9094";
    public static final String topic = "PARSE";
    public static final String ClientId = "producer.demo";

    public static Properties initConf() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, ClientId);
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);
        return properties;
    }

    public static void main(String[] args) {
        Properties prop = initConf();
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "Hello,Kafka");

        /****************************sync*********************************************/
        try {
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get(1L, TimeUnit.SECONDS);
            System.out.println(metadata.topic() + " - " + metadata.partition() + " - " + metadata.offset());
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            e.printStackTrace();
        }


        /****************************async*********************************************/
        try {
            Future<RecordMetadata> future = producer.send(record,
                    (RecordMetadata metadata, Exception e) -> {
                        if (e == null) {
                            e.printStackTrace();
                        } else {
                            System.out.println(metadata.topic() + " - " +
                                    metadata.partition() + " - " + metadata.offset());

                        }
                    }
            );
            RecordMetadata metadata = future.get(1L, TimeUnit.SECONDS);
            System.out.println(metadata.topic() + " - " + metadata.partition() + " - " + metadata.offset());
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            e.printStackTrace();
        }

        producer.close(Duration.ofSeconds(1, 100));

    }

}
