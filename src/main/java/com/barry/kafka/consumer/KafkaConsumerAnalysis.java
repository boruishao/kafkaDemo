package com.barry.kafka.consumer;

import com.barry.kafka.bean.Company;
import com.barry.kafka.serializer.CompanyDeserialier;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
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
//        deserializeRec();
//        assignCommitted();
        manuallyCommitRec();
    }

    /**
     * 自定义序列化器
     */
    private static void deserializeRec() {
        Properties properties = initConf();
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, CompanyDeserialier.class.getName());
        KafkaConsumer<String, Company> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Pattern.compile("PAR.*"));
        receiveCompany(consumer);
    }

    /**
     * 通过自定义序列化器
     *
     * @param consumer
     */
    private static void receiveCompany(KafkaConsumer<String, Company> consumer) {
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
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    private static void normalRec() {
        Properties properties = initConf();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        receiveByPartition(consumer);
    }

    private static void manuallyCommitRec() {
        Properties conf = initConf();
        conf.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(conf);
        consumer.subscribe(Arrays.asList(topic));
        receiveManuallyCommit(consumer);

    }

    /**
     * 通过assign，直接指定消费哪个分区
     */
    private static void assignPartition() {
        Properties properties = initConf();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        List<TopicPartition> partitions = new ArrayList<>();
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
        if (partitionInfos != null) {
            for (PartitionInfo partitionInfo : partitionInfos) {
                partitions.add(new TopicPartition(topic, partitionInfo.partition()));
            }
        }
        consumer.assign(partitions);
        receive(consumer);
    }

    /**
     * 用来对比 lastConsumeOffset 最后消费位移 position 提交位移 和 committed 已经提交位移 的区别
     */
    private static void assignCommitted() {
        Properties properties = initConf();
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        TopicPartition tp = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(tp));
        long lastConsumeOffset = -1;
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
            if (records.isEmpty()) {
                break;
            }
            List<ConsumerRecord<String, String>> partitionRec = records.records(tp);
            lastConsumeOffset = partitionRec.get(partitionRec.size() - 1).offset();
            consumer.commitSync();
        }
        System.out.println("consumed offset is " + lastConsumeOffset);
        OffsetAndMetadata offsetAndMetadata = consumer.committed(tp);
        System.out.println("committed offset is " + offsetAndMetadata.offset());
        long position = consumer.position(tp);
        System.out.println("the offset of the next record is " + position);

    }

    private static void receive(KafkaConsumer<String, String> consumer) {
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.println("topic is " + record.topic() + ", partition is " +
                            record.partition() + ", offset is " + record.offset());

                    System.out.println("key is " + record.key() + ", value is " + record.value());
                    //todo
                    Thread.sleep(500);
                }
                System.out.println("this time fetch record: " + records.count());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    /**
     * 手动提交位移 同步模式 / 异步模式
     *
     * @param consumer
     */
    private static void receiveManuallyCommit(KafkaConsumer<String, String> consumer) {
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));
                /***************************commit start ************************************************/
//                final int minBatchSize = 200;
//                ArrayList<ConsumerRecord> buffer = new ArrayList<>();
//
//                for (ConsumerRecord<String, String> record : records) {
//                    long offset = record.offset();
//                    System.out.println("topic is " + record.topic() + ", partition is " +
//                            record.partition() + ", offset is " + offset);
//
//                    System.out.println("key is " + record.key() + ", value is " + record.value());
//                    Thread.sleep(500);
//                    buffer.add(record);
//                }
//                /***************************同步第一种 ************************************************/
//                consumer.commitSync();
//                /***************************同步第二种 达到一定缓存数量在提交 *****************************/
//                if (buffer.size() > minBatchSize) {
//                    consumer.commitSync();
//                    buffer.clear();
//                }
//                /***************************同步第三种 指定提交位移数 粒度到每一条***********************************/
//                /***************************每消费一条提交一次，效率极低 ***************************************/
//                for (ConsumerRecord<String, String> record : records) {
//                    long offset = record.offset();
//                    System.out.println("topic is " + record.topic() + ", partition is " +
//                            record.partition() + ", offset is " + offset);
//                    System.out.println("key is " + record.key() + ", value is " + record.value());
//                    Thread.sleep(500);
//                    TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
//                    consumer.commitSync(
//                            Collections.singletonMap(topicPartition, new OffsetAndMetadata(offset + 1)));
//                }
                /***************************同步第四种 指定提交位移数 粒度到分区***********************************/
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {
                        long offset = record.offset();
                        System.out.println("topic is " + record.topic() + ", partition is " +
                                record.partition() + ", offset is " + offset);

                        System.out.println("key is " + record.key() + ", value is " + record.value());
                        Thread.sleep(500);
                    }
                    long lastConsumedOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    consumer.commitSync();
                    Collections.singletonMap(partition,
                            new OffsetAndMetadata(lastConsumedOffset + 1));
                }
//                /***************************异步第一种 ************************************************/
//                consumer.commitAsync((offsets, exception) -> {
//                    if (exception == null) {
//                        System.out.println(offsets);
//                    } else {
//                        System.out.println("fail to commit offset " + offsets);
//                        exception.printStackTrace();
//                    }
//                });
//                /***************************异步第二种 细粒度到每一条************************************************/
//                for (ConsumerRecord<String, String> record : records) {
//                    long offset = record.offset();
//                    System.out.println("topic is " + record.topic() + ", partition is " +
//                            record.partition() + ", offset is " + offset);
//                    System.out.println("key is " + record.key() + ", value is " + record.value());
//                    Thread.sleep(500);
//                    TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
//                    consumer.commitAsync(
//                            Collections.singletonMap(topicPartition, new OffsetAndMetadata(offset + 1)),
//                            (offsets, exception) -> {
//                                if (exception == null) {
//                                    System.out.println(offsets);
//                                } else {
//                                    System.out.println("fail to commit offset " + offsets);
//                                    exception.printStackTrace();
//                                }
//                            });
//                }

                /***************************commit end ************************************************/
                System.out.println("this time fetch record: " + records.count());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    /**
     * 从record的角度，指定处理哪些partition，上面的assign 是从consumer的角度，限制能消费哪些分区，
     * 也可以从topic的角度指定处理哪些record
     *
     * @param consumer
     */
    private static void receiveByPartition(KafkaConsumer<String, String> consumer) {
        try {
            while (isRunning.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                for (TopicPartition pt : records.partitions()) {
                    System.out.println("pt is " + pt.partition());
                    for (ConsumerRecord<String, String> record : records.records(pt)) {
                        System.out.println("topic is " + record.topic() + ", partition is " +
                                record.partition() + ", offset is " + record.offset());
                        System.out.println("key is " + record.key() + ", value is " + record.value());
                        //todo
                    }
                }

            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }


}
