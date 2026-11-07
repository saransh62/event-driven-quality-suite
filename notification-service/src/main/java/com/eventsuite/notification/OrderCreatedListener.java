package com.eventsuite.notification;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderCreatedListener {

    private final ObjectMapper mapper;
    private final NotificationProcessor processor;

    public OrderCreatedListener(ObjectMapper mapper, NotificationProcessor processor) {
        this.mapper = mapper;
        this.processor = processor;
    }

    /**
     * Any exception thrown here goes to the DefaultErrorHandler (see KafkaConsumerConfig):
     * retried, then published to orders.created.DLT. The offset is committed only after the
     * listener returns (or the record is dead-lettered), which gives at-least-once delivery.
     */
    @KafkaListener(topics = "${app.topics.orders-created}")
    public void onOrderCreated(ConsumerRecord<String, String> record) throws JsonProcessingException {
        OrderCreatedEvent event = mapper.readValue(record.value(), OrderCreatedEvent.class);
        if (event.eventId() == null || event.orderId() == null) {
            throw new MalformedEventException("eventId and orderId are required: " + record.value());
        }
        processor.process(event);
    }

    public static class MalformedEventException extends RuntimeException {
        public MalformedEventException(String message) {
            super(message);
        }
    }
}
