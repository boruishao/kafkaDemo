package com.barry.kafka.producer;

import com.barry.kafka.bean.Company;
import com.barry.kafka.interceptor.ProducerPrefixInterceptor;
import com.barry.kafka.interceptor.ProducerPrefixInterceptor2;
import com.barry.kafka.partitioner.DemoPartitioner;
import com.barry.kafka.serializer.CompanySerializer;
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

    public static void main(String[] args) throws Exception {

        while (true) {
            Thread.sleep(1000);
            commonStringSend();
//            defineSerSend();
//            definePartitionSend();
//        defineInterceptorSend();
        }

    }

    /**
     * 同步和异步发送
     */
    private static void commonStringSend() {
        Properties prop = initConf();
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "Hello,Kafka");
        ProducerRecord<String, String> ttlRecord =
                new ProducerRecord<>(topic, 0, System.currentTimeMillis() - 1000, null, "Hello,ttl Kafka");

        /****************************sync*********************************************/
        try {
            Future<RecordMetadata> future = producer.send(ttlRecord);
            //设置超时时间
            RecordMetadata metadata = future.get(1L, TimeUnit.SECONDS);
            System.out.println("sync : " + metadata.topic() + " - " + metadata.partition() + " - " + metadata.offset());
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            e.printStackTrace();
        }

        /****************************async*********************************************/
        try {
              producer.send(record,
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

        producer.close(Duration.ofSeconds(1, 100));
    }

    private static void defineSerSend() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CompanySerializer.class.getName());

        KafkaProducer<String, Company> producer = new KafkaProducer<>(properties);
        Company company = Company.builder().name("earlydata").address("xinhuiroad468").build();
        ProducerRecord<String, Company> record = new ProducerRecord<>(topic, "company", company);
        try {
            RecordMetadata metadata = producer.send(record).get();
            System.out.println(metadata.topic() + " - " + metadata.partition() + " - " + metadata.offset());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        producer.close();
    }

    private static void definePartitionSend() {
        Properties prop = initConf();
        prop.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, DemoPartitioner.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "Hello,Kafka Partition");

        try {
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get(1L, TimeUnit.SECONDS);
            System.out.println(metadata.topic() + " - " + metadata.partition() + " - " + metadata.offset());
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            e.printStackTrace();
        }
    }

    /**
     * 拦截器的执行顺序，按照配置中的先后顺序执行
     */
    private static void defineInterceptorSend() {
        Properties prop = initConf();
        prop.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                ProducerPrefixInterceptor.class.getName() + "," +
                        ProducerPrefixInterceptor2.class.getName()
        );
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, "Hello,Kafka Interceptor");

        try {
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get(1L, TimeUnit.SECONDS);
            System.out.println(metadata.topic() + " - " + metadata.partition() + " - " + metadata.offset());
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            e.printStackTrace();
        }
        producer.close();
    }

}
