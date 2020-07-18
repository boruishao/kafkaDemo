package com.barry.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author boruiShao
 * Date 2020/7/18 4:01 PM
 * Version 1.0
 * Describe TODO
 **/

public class DemoPartitioner implements Partitioner {

    private final AtomicInteger counter =  new AtomicInteger(0);

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value,
                         byte[] valueBytes, Cluster cluster) {
        Integer numPart = cluster.partitionCountForTopic(topic);
        if(keyBytes ==null){
            return counter.getAndIncrement() % numPart;
        }else {
            return Utils.toPositive(Utils.murmur2(keyBytes)) % numPart;
        }
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
