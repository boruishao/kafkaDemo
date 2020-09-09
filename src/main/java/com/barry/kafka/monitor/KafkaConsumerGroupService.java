package com.barry.kafka.monitor;

import com.barry.kafka.consumer.KafkaConsumerAnalysis;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * @author borui.shao@earlydata.com
 * Date 2020/9/3 10:05 AM
 * Version 1.0
 * Describe 手动计算lag
 * 1.
 * 2.
 * 3.
 **/

@Slf4j
public class KafkaConsumerGroupService {
    private String brokerList;
    private AdminClient adminClient;
    private KafkaConsumer<String, String> consumer;

    public KafkaConsumerGroupService(String brokerList) {
        this.brokerList = brokerList;
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        KafkaConsumerGroupService service = new KafkaConsumerGroupService("192.168.3.136:9092");
        service.init();
        List<PartitionAssignmentState> list = service.collectGroupAssignment(KafkaConsumerAnalysis.groupId);
        ConsumerGroupUtils.printPasList(list);
        service.close();
    }

    private void init() {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        adminClient = AdminClient.create(properties);
        consumer = ConsumerGroupUtils.createNewConsumer(brokerList, "group-demo");
    }

    public void close() {
        if (adminClient != null) {
            adminClient.close();
        }
        if (consumer != null) {
            consumer.close();
        }
    }

    public List<PartitionAssignmentState> collectGroupAssignment(String group)
            throws ExecutionException, InterruptedException {
        DescribeConsumerGroupsResult groupsResult =
                adminClient.describeConsumerGroups(Collections.singleton(group));
        ConsumerGroupDescription description = groupsResult.all().get().get(group);
        List<TopicPartition> assignedTps = new ArrayList<>();
        List<PartitionAssignmentState> rowsWithConsumer = new ArrayList<>();
        Collection<MemberDescription> members = description.members();
        if (members != null) {
            ListConsumerGroupOffsetsResult offsetsResult = adminClient.listConsumerGroupOffsets(group);
            Map<TopicPartition, OffsetAndMetadata> offsets = offsetsResult.partitionsToOffsetAndMetadata().get();
            if (offsets != null && !offsets.isEmpty()) {
                String stage = description.state().toString();
                if (stage.equals("Stable")) {
                    rowsWithConsumer = getRowsWithConsumer(description, offsets, members, assignedTps, group);
                }
            }
            rowsWithConsumer.addAll(getRowsWithoutConsumer(description, offsets, assignedTps, group));
        }
        return rowsWithConsumer;
    }

    /**
     * 没有消费者成员信息的处理
     */
    private List<PartitionAssignmentState> getRowsWithoutConsumer(ConsumerGroupDescription description,
                                                                  Map<TopicPartition, OffsetAndMetadata> offsets,
                                                                  List<TopicPartition> assignedTps,
                                                                  String group) {
        Set<TopicPartition> tpSet = offsets.keySet();

        return tpSet.stream()
                //排除有消费者成员信息的topic
                .filter(tp -> !assignedTps.contains(tp))
                .map(tp -> {
                    long logSize = 0;
                    Long endOffset = consumer.endOffsets(Collections.singleton(tp)).get(tp);
                    if (endOffset != null) {
                        logSize = endOffset;
                    }
                    long offset = offsets.get(tp).offset();
                    return PartitionAssignmentState.builder().group(group)
                            .coordinator(description.coordinator()).topic(tp.topic()).partition(tp.partition())
                            .logSize(logSize).lag(getLag(offset, logSize)).offset(offset).build();
                }).sorted(Comparator.comparing(PartitionAssignmentState::getPartition))
                .collect(Collectors.toList());
    }

    /**
     * 有消费者成员信息的处理
     */
    private List<PartitionAssignmentState> getRowsWithConsumer(ConsumerGroupDescription description,
                                                               Map<TopicPartition, OffsetAndMetadata> offsets,
                                                               Collection<MemberDescription> members,
                                                               List<TopicPartition> assignedTps,
                                                               String group) {
        List<PartitionAssignmentState> rowsWithConsumer = new ArrayList<>();
        for (MemberDescription member : members) {
            MemberAssignment assignment = member.assignment();
            if (assignment == null) {
                continue;
            }
            Set<TopicPartition> tpSet = assignment.topicPartitions();
            if (tpSet.isEmpty()) {
                rowsWithConsumer.add(PartitionAssignmentState.builder()
                        .group(group).coordinator(description.coordinator())
                        .consumerId(member.consumerId()).host(member.host())
                        .clientId(member.clientId())
                        .build());
            } else {
                Map<TopicPartition, Long> logSizes = consumer.endOffsets(tpSet);
                assignedTps.addAll(tpSet);
                List<PartitionAssignmentState> tempList = tpSet.stream().sorted(Comparator.comparing(TopicPartition::partition))
                        .map(tp -> getPasWithConsumer(logSizes, offsets, tp, group, member, description))
                        .collect(Collectors.toList());
                rowsWithConsumer.addAll(tempList);
            }

        }
        return rowsWithConsumer;
    }


    private PartitionAssignmentState getPasWithConsumer(
            Map<TopicPartition, Long> logSizes,
            Map<TopicPartition, OffsetAndMetadata> offsets,
            TopicPartition tp, String group,
            MemberDescription member,
            ConsumerGroupDescription description) {
        Long logSize = logSizes.get(tp);
        if (offsets.containsKey(tp)) {
            long offset = offsets.get(tp).offset();
            long lag = getLag(offset, logSize);
            return PartitionAssignmentState.builder().group(group)
                    .coordinator(description.coordinator()).lag(lag)
                    .topic(tp.topic()).partition(tp.partition())
                    .offset(offset).consumerId(member.consumerId())
                    .host(member.host()).clientId(member.clientId())
                    .logSize(logSize).build();

        } else {
            return PartitionAssignmentState.builder()
                    .group(group).coordinator(description.coordinator())
                    .topic(tp.topic()).partition(tp.partition())
                    .consumerId(member.consumerId()).host(member.host())
                    .clientId(member.clientId()).logSize(logSize)
                    .build();

        }
    }

    private static long getLag(long offset, long logSize) {
        long lag = logSize - offset;
        return lag < 0 ? 0 : lag;
    }

}

class ConsumerGroupUtils {

    /**
     * 以默认配置创建consumer
     *
     * @param brokerList
     * @param groupId
     * @return
     */
    static KafkaConsumer<String, String> createNewConsumer(String brokerList, String groupId) {
        Properties prop = new Properties();
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        prop.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        prop.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        prop.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        prop.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        prop.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<String, String>(prop);
    }

    //打印结果，可以以返回值的形式返回给前端
    static void printPasList(List<PartitionAssignmentState> list) {
        System.out.println(String.format("%-40s %-10s %-15s %-15s %-10s %-50s%-30s %s",
                "TOPIC", "PARTITION", "CURRENT-OFFSET", "LOG-END-OFFSET", "LAG",
                "CONSUMER-ID", "HOST", "CLIENT-ID"));
        list.forEach(item -> {
            System.out.println(String.format("%-40s %-10s %-15s %-15s %-10s %-50s%-30s %s",
                    item.getTopic(), item.getPartition(), item.getOffset(), item.getLogSize(), item.getLag(),
                    Optional.ofNullable(item.getConsumerId()).orElse("-"),
                    Optional.ofNullable(item.getHost()).orElse("-"),
                    Optional.ofNullable(item.getClientId()).orElse("-")
            ));
        });
    }

}

/**
 * 用于展示结果的Bean
 */
@Data
@Builder
class PartitionAssignmentState {
    private String group;
    private Node coordinator;
    private String topic;
    private int partition;
    private long offset;
    private long lag;
    private String consumerId;
    private String host;
    private String clientId;
    private long logSize;
}