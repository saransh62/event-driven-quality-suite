package com.eventsuite.e2e;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Consumer lag = sum over partitions of (log-end offset - group's committed offset).
 * Read with AdminClient, the same numbers kafka-consumer-groups.sh --describe prints.
 */
final class ConsumerLag implements AutoCloseable {

    private final Admin admin;

    ConsumerLag(String bootstrap) {
        this.admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap));
    }

    long total(String group, String topic) {
        try {
            Map<TopicPartition, OffsetAndMetadata> committed = admin.listConsumerGroupOffsets(group)
                    .partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
            Map<TopicPartition, OffsetSpec> latest = admin.describeTopics(java.util.List.of(topic))
                    .allTopicNames().get(10, TimeUnit.SECONDS).get(topic).partitions().stream()
                    .map(p -> new TopicPartition(topic, p.partition()))
                    .collect(Collectors.toMap(Function.identity(), tp -> OffsetSpec.latest()));
            return admin.listOffsets(latest).all().get(10, TimeUnit.SECONDS).entrySet().stream()
                    .mapToLong(e -> {
                        OffsetAndMetadata c = committed.get(e.getKey());
                        long committedOffset = c == null ? 0 : c.offset();   // never consumed: whole partition is lag
                        return Math.max(0, e.getValue().offset() - committedOffset);
                    })
                    .sum();
        } catch (Exception e) {
            throw new IllegalStateException("Could not compute lag for group " + group, e);
        }
    }

    @Override
    public void close() {
        admin.close();
    }
}
