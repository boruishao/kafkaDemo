package com.barry.kafka.consumer;

import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import sun.nio.ch.ThreadPool;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author borui.shao@earlydata.com
 * Date 2020/8/1 11:42 AM
 * Version 1.0
 * Describe 除了下面两种，还有一种多线程实现是 多个消费者 在同一个分区中，同时消费不同数据，
 * 但这种方式对于提交位移和消费顺序的处理极为复杂，顾不推荐。
 **/

public class FirstMtiConsumerThreadDemo {

    public static void main(String[] args) {
        Properties props = KafkaConsumerAnalysis.initConf();
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        int consumerThreadNum = 2;
        for (int i = 0; i < consumerThreadNum; i++) {
            new KafkaConsumerThread2(props, KafkaConsumerAnalysis.topic,2).start();
        }
    }

    /**
     * 第一种多线程实现，多个消费者，每个消费者内部是一个单线程
     */
    public static class KafkaConsumerThread extends Thread {
        private KafkaConsumer<String, String> kafkaConsumer;

        public KafkaConsumerThread(Properties prop, String topic) {
            this.kafkaConsumer = new KafkaConsumer<>(prop);
            this.kafkaConsumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            KafkaConsumerAnalysis.receive(kafkaConsumer);
        }
    }

    /**
     * 第二种多线程实现，多个消费者，每个消费者 以多个线程同时消费消息
     */
    public static class KafkaConsumerThread2 extends Thread {
        private KafkaConsumer<String, String> kafkaConsumer;
        private ExecutorService executorService;
        private int threadNum;

        public KafkaConsumerThread2(Properties prop, String topic, int threadNum) {
            this.kafkaConsumer = new KafkaConsumer<>(prop);
            this.kafkaConsumer.subscribe(Collections.singletonList(topic));
            this.threadNum = threadNum;
            this.executorService = new ThreadPoolExecutor(threadNum, threadNum, 0L,
                    TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1000),
                    new ThreadPoolExecutor.CallerRunsPolicy());
        }

        @Override
        public void run() {
            try {

                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
                    if (!records.isEmpty()) {
                        executorService.submit(new RecordHandler(records));
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                kafkaConsumer.close();
            }

        }
    }

    public static class RecordHandler extends Thread{
        public final ConsumerRecords<String,String> records;

        public RecordHandler(ConsumerRecords<String, String> records) {
            this.records = records;
        }

        @SneakyThrows
        @Override
        public void run() {
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("topic is " + record.topic() + ", partition is " +
                        record.partition() + ", offset is " + record.offset() +
                        ", time is " + new Timestamp(record.timestamp()));

                System.out.println("key is " + record.key() + ", value is " + record.value());
                //todo
                Thread.sleep(500);
            }
            System.out.println("this time fetch record: " + records.count());
            System.out.println("thread name : " + Thread.currentThread().getName()+
                    " thread id : "+ Thread.currentThread().getId());
        }
    }

}
