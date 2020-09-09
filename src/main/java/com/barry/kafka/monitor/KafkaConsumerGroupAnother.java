package com.barry.kafka.monitor;

import com.barry.kafka.consumer.KafkaConsumerAnalysis;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.scala.DefaultScalaModule;
import kafka.admin.ConsumerGroupCommand;
import lombok.Builder;
import lombok.Data;
import scala.collection.immutable.HashMap;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * @author borui.shao@earlydata.com
 * Date 2020/9/9 9:13 AM
 * Version 1.0
 * Describe TODO
 **/

public class KafkaConsumerGroupAnother {

    public static void main(String[] args) throws JsonProcessingException {
        String[] args2 = {"--describe", "--bootstrap-server", "192.168.3.136:9092", "--group", KafkaConsumerAnalysis.groupId};
        ConsumerGroupCommand.ConsumerGroupCommandOptions options = new ConsumerGroupCommand.ConsumerGroupCommandOptions(args2);
        ConsumerGroupCommand.ConsumerGroupService consumerGroupService = new ConsumerGroupCommand.ConsumerGroupService(options, new HashMap<>());
        ObjectMapper mapper = new ObjectMapper();

        mapper.registerModule(new DefaultScalaModule());
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        String source = mapper.writeValueAsString(consumerGroupService.collectGroupOffsets(KafkaConsumerAnalysis.groupId)._2.get());
        List<PartitionAssignmentStateAnother> target =
                mapper.readValue(source, getCollectionType(mapper, List.class, PartitionAssignmentStateAnother.class));

        target.sort(Comparator.comparingInt(PartitionAssignmentStateAnother::getPartition));
        printPasList(target);
    }

    public static JavaType getCollectionType(ObjectMapper mapper,
                                             Class<?> collectionClass,
                                             Class<?>... elementClasses) {
        return mapper.getTypeFactory().constructParametricType(collectionClass, elementClasses);
    }

    //打印结果，可以以返回值的形式返回给前端
    static void printPasList(List<PartitionAssignmentStateAnother> list) {
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

@Data
@Builder
class PartitionAssignmentStateAnother {
    private String group;

    private String topic;
    private int partition;
    private long offset;
    private long lag;
    private String consumerId;
    private String host;
    private String clientId;
    private long logSize;

    @Data
    public static class Node {
        public int id;
        public String idString;
        public String host;
        public int port;
        public String rack;
    }
}