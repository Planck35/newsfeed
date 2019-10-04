package com.martinkl.samza.newsfeed;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.descriptors.TaskApplicationDescriptor;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.task.StreamTaskFactory;

public class GenerateFollowsApplication implements TaskApplication {
    private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("172.31.11.123:2181");
    private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("172.31.11.123:9092,172.31.1.131:9092,172.31.8.63:9092");
    private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

    @Override
    public void describe(TaskApplicationDescriptor taskApplicationDescriptor) {
        // Define a system descriptor for Kafka.
        KafkaSystemDescriptor kafkaSystemDescriptor =
                new KafkaSystemDescriptor("kafka").withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
                        .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
                        .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

        KafkaOutputDescriptor kafkaOutputDescriptor =
                kafkaSystemDescriptor.getOutputDescriptor("newsfeed-follows", new JsonSerde<>());

        taskApplicationDescriptor.withDefaultSystem(kafkaSystemDescriptor);

        taskApplicationDescriptor.withOutputStream(kafkaOutputDescriptor);

        taskApplicationDescriptor.withTaskFactory((StreamTaskFactory) () -> new GenerateFollowsTask());
    }
}
