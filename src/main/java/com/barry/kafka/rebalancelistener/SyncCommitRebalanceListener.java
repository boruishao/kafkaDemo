package com.barry.kafka.rebalancelistener;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;

import java.util.Collection;

/**
 * @author borui.shao@earlydata.com
 * Date 2020/7/29 9:30 AM
 * Version 1.0
 * Describe 最好使用匿名内部类来实现这个接口，这样方便和 consumer，offset 进行交互
 **/

public class SyncCommitRebalanceListener implements ConsumerRebalanceListener {
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.println("重新分配前");
        System.out.println(partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.println("重新分配后");
        System.out.println(partitions);
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {

    }
}
