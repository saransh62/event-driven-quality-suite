package com.eventsuite.it;

import com.eventsuite.notification.Notification;
import com.eventsuite.notification.NotificationRepository;
import com.eventsuite.notification.NotificationServiceApplication;
import com.github.tomakehurst.wiremock.WireMockServer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.core.WireMockConfiguration.options;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * notification-service against real Kafka + PostgreSQL. order-service is replaced by WireMock:
 * this test is about the consumer; the HTTP contract with order-service is covered by Pact.
 */
@SpringBootTest(classes = NotificationServiceApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles(NotificationServiceApplication.PROFILE)
class NotificationServiceIntegrationTest {

    /** Own topic so records left by other test classes in the shared broker can't interfere. */
    private static final String TOPIC = "it.notification.orders.created";
    private static final String DLT = TOPIC + ".DLT";

    private static final WireMockServer orderService = new WireMockServer(options().dynamicPort());
    private static final KafkaProducer<String, String> producer = producer();

    static {
        orderService.start();
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", Containers.NOTIFICATION_DB::getJdbcUrl);
        r.add("spring.datasource.username", Containers.NOTIFICATION_DB::getUsername);
        r.add("spring.datasource.password", Containers.NOTIFICATION_DB::getPassword);
        r.add("spring.kafka.bootstrap-servers", Containers.KAFKA::getBootstrapServers);
        r.add("app.order-service.url", orderService::baseUrl);
        r.add("app.topics.orders-created", () -> TOPIC);
        r.add("app.retry.backoff-ms", () -> 100);
    }

    @Autowired
    NotificationRepository repository;

    @BeforeEach
    void resetStubs() {
        orderService.resetAll();
    }

    @AfterAll
    static void tearDown() {
        producer.close();
        orderService.stop();
    }

    @Test
    void consumedEvent_isEnrichedFromOrderService_andStoredAsProcessed() {
        UUID orderId = UUID.randomUUID();
        stubOrder(orderId, "ada@example.com");

        send(orderId.toString(), event(UUID.randomUUID(), orderId));

        Notification n = awaitSingleProcessed(orderId);
        assertThat(n.getRecipient()).isEqualTo("ada@example.com");
        assertThat(n.getMessage()).contains("2 x Keyboard").contains("49.90");
        assertThat(n.getProcessedAt()).isAfterOrEqualTo(n.getReceivedAt());
        orderService.verify(1, getRequestedFor(urlEqualTo("/orders/" + orderId)));
    }

    @Test
    void duplicateEvent_isIgnored_noSecondRowAndNoSecondEnrichmentCall() {
        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        stubOrder(orderId, "grace@example.com");

        send(orderId.toString(), event(eventId, orderId));
        send(orderId.toString(), event(eventId, orderId));          // redelivery: same eventId
        UUID markerEvent = UUID.randomUUID();
        send(orderId.toString(), event(markerEvent, orderId));      // same key => same partition, after the duplicate

        // Once the marker is processed, the duplicate (earlier in the partition) has been consumed too.
        await().atMost(Duration.ofSeconds(20)).until(() -> processedEventIds(orderId).contains(markerEvent));

        List<UUID> eventIds = repository.findByOrderId(orderId).stream().map(Notification::getEventId).toList();
        assertThat(eventIds).containsExactlyInAnyOrder(eventId, markerEvent);
        orderService.verify(2, getRequestedFor(urlEqualTo("/orders/" + orderId)));
    }

    @Test
    void malformedJson_isRetriedThenPublishedToDlt_andDoesNotBlockThePartition() {
        String key = "poison-" + UUID.randomUUID();
        send(key, "this is {not json");

        ConsumerRecord<String, String> dead = awaitDeadLetter(key);
        assertThat(dead.value()).isEqualTo("this is {not json");
        assertThat(header(dead, "kafka_dlt-original-topic")).isEqualTo(TOPIC);
        assertThat(header(dead, "kafka_dlt-exception-message")).isNotBlank();

        // The consumer moved past the poison pill: a valid event after it is still processed.
        UUID orderId = UUID.randomUUID();
        stubOrder(orderId, "linus@example.com");
        send(orderId.toString(), event(UUID.randomUUID(), orderId));
        awaitSingleProcessed(orderId);
    }

    @Test
    void eventMissingRequiredFields_goesToDlt_andNothingIsStored() {
        UUID orderId = UUID.randomUUID();
        String key = orderId.toString();
        send(key, "{\"orderId\":\"" + orderId + "\"}");             // no eventId

        ConsumerRecord<String, String> dead = awaitDeadLetter(key);
        assertThat(header(dead, "kafka_dlt-exception-message")).contains("eventId and orderId are required");
        assertThat(repository.findByOrderId(orderId)).isEmpty();
    }

    @Test
    void enrichmentFailure_isRetried_thenDeadLettered_withoutLeavingAPartialRow() {
        UUID orderId = UUID.randomUUID();                            // not stubbed: WireMock returns 404
        send(orderId.toString(), event(UUID.randomUUID(), orderId));

        awaitDeadLetter(orderId.toString());
        // 1 attempt + 2 retries, each rolled back
        orderService.verify(3, getRequestedFor(urlEqualTo("/orders/" + orderId)));
        assertThat(repository.findByOrderId(orderId)).isEmpty();
    }

    // ---------------------------------------------------------------- helpers

    private Notification awaitSingleProcessed(UUID orderId) {
        return await().atMost(Duration.ofSeconds(20)).until(
                () -> repository.findByOrderId(orderId),
                list -> list.size() == 1 && list.get(0).getStatus() == Notification.Status.PROCESSED).get(0);
    }

    private List<UUID> processedEventIds(UUID orderId) {
        return repository.findByOrderId(orderId).stream()
                .filter(n -> n.getStatus() == Notification.Status.PROCESSED)
                .map(Notification::getEventId).toList();
    }

    private static ConsumerRecord<String, String> awaitDeadLetter(String key) {
        return await().atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500)).until(
                () -> TestKafkaConsumer.readAll(Containers.KAFKA.getBootstrapServers(), DLT).stream()
                        .filter(r -> key.equals(r.key())).toList(),
                list -> list.size() == 1).get(0);
    }

    private static String header(ConsumerRecord<String, String> r, String name) {
        return new String(r.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }

    private static void stubOrder(UUID orderId, String email) {
        orderService.stubFor(get(urlEqualTo("/orders/" + orderId)).willReturn(aResponse()
                .withHeader("Content-Type", "application/json")
                .withBody("""
                        {"id":"%s","customerEmail":"%s","product":"Keyboard","quantity":2,
                         "amount":49.90,"status":"CREATED","createdAt":"2026-01-01T10:00:00Z"}"""
                        .formatted(orderId, email))));
    }

    private static String event(UUID eventId, UUID orderId) {
        return """
                {"eventId":"%s","orderId":"%s","customerEmail":"x@example.com","product":"Keyboard",
                 "quantity":2,"amount":49.90,"createdAt":"2026-01-01T10:00:00Z"}""".formatted(eventId, orderId);
    }

    private static void send(String key, String value) {
        try {
            producer.send(new ProducerRecord<>(TOPIC, key, value)).get();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    private static KafkaProducer<String, String> producer() {
        return new KafkaProducer<>(Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Containers.KAFKA.getBootstrapServers(),
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class));
    }
}
