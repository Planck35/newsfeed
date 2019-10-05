package com.cloudcomputing.samza.nycabs.application;

import com.cloudcomputing.samza.nycabs.DriverMatchTask;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import org.apache.samza.application.TaskApplication;
import org.apache.samza.application.descriptors.TaskApplicationDescriptor;
import org.apache.samza.serializers.JsonSerde;
import org.apache.samza.system.kafka.descriptors.KafkaInputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaOutputDescriptor;
import org.apache.samza.system.kafka.descriptors.KafkaSystemDescriptor;
import org.apache.samza.task.StreamTaskFactory;

public class DriverMatchTaskApplication implements TaskApplication {
    private static final List<String> KAFKA_CONSUMER_ZK_CONNECT = ImmutableList.of("172.31.2.31:2181");
    private static final List<String> KAFKA_PRODUCER_BOOTSTRAP_SERVERS = ImmutableList.of("172.31.10.99:9092,172.31.7.54:9092,172.31.2.31:9092");
    private static final Map<String, String> KAFKA_DEFAULT_STREAM_CONFIGS = ImmutableMap.of("replication.factor", "1");

    @Override
    public void describe(TaskApplicationDescriptor taskApplicationDescriptor) {
        // Define a system descriptor for Kafka.
        KafkaSystemDescriptor kafkaSystemDescriptor =
                new KafkaSystemDescriptor("kafka").withConsumerZkConnect(KAFKA_CONSUMER_ZK_CONNECT)
                        .withProducerBootstrapServers(KAFKA_PRODUCER_BOOTSTRAP_SERVERS)
                        .withDefaultStreamConfigs(KAFKA_DEFAULT_STREAM_CONFIGS);

        // Output descriptor for the match-stream topic.
        KafkaOutputDescriptor kafkaOutputDescriptor =
                kafkaSystemDescriptor.getOutputDescriptor("match-stream", new JsonSerde<>());

        taskApplicationDescriptor.withDefaultSystem(kafkaSystemDescriptor);


        // Set the output.
        taskApplicationDescriptor.withOutputStream(kafkaOutputDescriptor);

        taskApplicationDescriptor.withTaskFactory((StreamTaskFactory) () -> new DriverMatchTask());
    }
}
