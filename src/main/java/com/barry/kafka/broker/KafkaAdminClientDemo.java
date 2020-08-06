package com.barry.kafka.broker;

import com.sun.org.apache.xpath.internal.axes.PredicatedNodeTest;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.internals.Topic;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * @author borui.shao@earlydata.com
 * Date 2020/8/6 9:03 AM
 * Version 1.0
 * Describe kafkaAdminClient 实现对topic 的 曾删改查
 **/

public class KafkaAdminClientDemo {

    private final String brokerList = "localhost:9092,localhost:9093,localhost:9094";
    private final String topicName = "topic-admin";
    private final int timeout = 30000;

    public static void main(String[] args) {
        KafkaAdminClientDemo admin = new KafkaAdminClientDemo();
//        admin.adminCreateTopic();
        admin.adminDeleteTopic();
        admin.adminListTopic();
    }

    public void adminListTopic(){
        Properties prop = initConf();
        AdminClient client = createAdminClient(prop);
        listTopics(client);
        closeClient(client);
    }

    public void adminCreateTopic(){
        Properties prop = initConf();
        AdminClient client = createAdminClient(prop);
        createTopic(client,topicName);
        closeClient(client);
    }

    public void adminDeleteTopic(){
        Properties prop = initConf();
        AdminClient client = createAdminClient(prop);
        deleteTopic(client,topicName);
        closeClient(client);
    }

    private Properties initConf() {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, timeout);
        return properties;
    }

    private AdminClient createAdminClient(Properties prop) {
        return AdminClient.create(prop);
    }

    private void closeClient(AdminClient client) {
        if (client != null) {
            client.close();
        }
    }

    private void createTopic(AdminClient client, String topicName) {
        /******************case 1 sample create topic *****************************************/
//        NewTopic newTopic = new NewTopic(topicName, 4, (short) 2);
        /******************case 2 specify some configs*****************************************/
        Map<Integer, List<Integer>> replicasAssignments = new HashMap<>();
        replicasAssignments.put(0,Arrays.asList(0,1));
        replicasAssignments.put(1,Arrays.asList(1,2));
        replicasAssignments.put(2,Arrays.asList(2,0));
        replicasAssignments.put(3,Arrays.asList(0,1));
        NewTopic newTopic = new NewTopic(topicName, replicasAssignments);

        Map<String,String> configs = new HashMap<>();
        configs.put(TopicConfig.CLEANUP_POLICY_CONFIG,TopicConfig.CLEANUP_POLICY_COMPACT);
        newTopic.configs(configs);

        CreateTopicsResult result = client.createTopics(Collections.singleton(newTopic));
        try {
            result.all().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

    }

    private void listTopics(AdminClient client){
        ListTopicsResult listTopicsResult = client.listTopics();
        try {
            Collection<TopicListing> topicListings = listTopicsResult.listings().get();
            for (TopicListing topicListing : topicListings) {
                System.out.println(topicListing);
            }
            System.out.println("--------------");
            Set<String> strings = listTopicsResult.names().get();
            for (String string : strings) {
                System.out.println(string);
            }
            System.out.println("--------------");
            Map<String, TopicListing> stringTopicListingMap = listTopicsResult.namesToListings().get();
            for (Map.Entry<String, TopicListing> ste : stringTopicListingMap.entrySet()) {
                System.out.println(ste.getKey() + ":" + ste.getValue());
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

    }

    private void deleteTopic(AdminClient client, String topicName){
        DeleteTopicsResult deleteTopicsResult = client.deleteTopics(Collections.singleton(topicName));
        try {
            deleteTopicsResult.all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }


}
