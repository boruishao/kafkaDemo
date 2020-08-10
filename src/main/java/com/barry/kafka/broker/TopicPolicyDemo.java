package com.barry.kafka.broker;

import org.apache.kafka.common.errors.PolicyViolationException;
import org.apache.kafka.server.policy.CreateTopicPolicy;

import java.util.Map;

/**
 * @author borui.shao@earlydata.com
 * Date 2020/8/8 10:22 AM
 * Version 1.0
 * Describe 这个实现需要配置在server.properties 的 create.topic.policy.class.name
 * 经测试，需要把这个类放到kafka 源码里才能生效。
 * 还用一种方式是打成jar包，然后配置到CLASS_PATH中，但是没研究明白CLASS_PATH是啥，直接放到kafka/libs下是没用的。
 **/

public class TopicPolicyDemo implements CreateTopicPolicy {
    @Override
    public void validate(RequestMetadata requestMetadata) throws PolicyViolationException {
        Integer partitions = requestMetadata.numPartitions();
        Short replicas = requestMetadata.replicationFactor();
        if (partitions != null || replicas != null) {
            if (partitions < 5) {
                throw new PolicyViolationException(
                        "Topic should have at least 5 partitions, received:" + partitions);
            }
            if (replicas < 2) {
                throw new PolicyViolationException(
                        "Topic should have at least 2 replicas, received:" + replicas);
            }

        }
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
