package com.barry.kafka.broker;

import kafka.admin.TopicCommand;
import org.apache.kafka.common.config.TopicConfig;

/**
 * @author borui.shao@earlydata.com
 * Date 2020/8/3 8:55 AM
 * Version 1.0
 * Describe TODO
 **/

public class KafkaCommandDemo {
    public static final String zookeeperList = "localhost:2181";

    public static void main(String[] args) {
        String topicName = "topic-create-api";
        createTopic(topicName);
        showTopics();
    }

    public static void createTopic(String topicName) {
        String[] args = new String[]{
                "--zookeeper",zookeeperList,
                "--create","--replication-factor","1",
                "--partitions","1",
                "--topic",topicName
        };
        TopicCommand.main(args);
    }

    public static void showTopics() {
        String[] args = new String[]{
                "--zookeeper",zookeeperList,
                "--list"
        };
        TopicCommand.main(args);
    }





    }
