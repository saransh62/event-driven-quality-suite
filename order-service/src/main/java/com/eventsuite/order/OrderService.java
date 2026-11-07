package com.eventsuite.order;

import com.eventsuite.order.OrderDtos.CreateOrderRequest;
import com.eventsuite.order.OrderDtos.OrderCreatedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class OrderService {

    private final OrderRepository repository;
    private final KafkaTemplate<String, String> kafka;
    private final ObjectMapper mapper;
    private final String topic;

    public OrderService(OrderRepository repository, KafkaTemplate<String, String> kafka, ObjectMapper mapper,
                        @Value("${app.topics.orders-created}") String topic) {
        this.repository = repository;
        this.kafka = kafka;
        this.mapper = mapper;
        this.topic = topic;
    }

    /**
     * Saves the order and publishes OrderCreated (key = orderId) inside the DB transaction.
     * The send is awaited, so a broker failure rolls the insert back instead of losing the event.
     * (A transactional outbox would remove the remaining dual-write gap; see DESIGN.md.)
     */
    @Transactional
    public Order create(CreateOrderRequest req) {
        Order order = repository.save(new Order(UUID.randomUUID(), req.customerEmail(), req.product(),
                req.quantity(), req.amount()));
        String key = order.getId().toString();
        try {
            String payload = mapper.writeValueAsString(OrderCreatedEvent.from(order));
            kafka.send(topic, key, payload).get(10, TimeUnit.SECONDS);
        } catch (JsonProcessingException | ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Failed to publish OrderCreated for " + key, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing OrderCreated", e);
        }
        return order;
    }

    @Transactional(readOnly = true)
    public Optional<Order> find(UUID id) {
        return repository.findById(id);
    }

    @Transactional(readOnly = true)
    public List<Order> findByStatus(Order.Status status) {
        return repository.findByStatusOrderByCreatedAt(status);
    }
}
