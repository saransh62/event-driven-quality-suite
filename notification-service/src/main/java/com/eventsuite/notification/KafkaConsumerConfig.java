package com.eventsuite.notification;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfig {

    /**
     * Failed records are retried {@code maxRetries} times (in-memory, same consumer) and then
     * published to "<topic>.DLT" on the same partition. Spring Boot wires this bean into the
     * auto-configured listener container factory.
     */
    @Bean
    DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template,
                                     @Value("${app.retry.backoff-ms}") long backoffMs,
                                     @Value("${app.retry.max-retries}") long maxRetries) {
        DeadLetterPublishingRecoverer toDlt = new DeadLetterPublishingRecoverer(template,
                (record, ex) -> new TopicPartition(record.topic() + ".DLT", record.partition()));
        return new DefaultErrorHandler(toDlt, new FixedBackOff(backoffMs, maxRetries));
    }

    @Bean
    NewTopic deadLetterTopic(@Value("${app.topics.orders-created}") String topic) {
        return TopicBuilder.name(topic + ".DLT").partitions(3).replicas(1).build();
    }
}
