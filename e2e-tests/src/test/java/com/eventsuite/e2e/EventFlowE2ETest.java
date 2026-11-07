package com.eventsuite.e2e;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

import static com.eventsuite.e2e.E2eSupport.awaitHealthy;
import static com.eventsuite.e2e.E2eSupport.createOrders;
import static com.eventsuite.e2e.E2eSupport.notificationsFor;
import static com.eventsuite.e2e.E2eSupport.processedExactlyOnce;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Full path through the real stack: HTTP -> order-service -> Kafka -> notification-service -> HTTP enrich -> DB. */
class EventFlowE2ETest {

    private static ConsumerLag lag;

    @BeforeAll
    static void stackIsUp() {
        awaitHealthy(Env.ORDER_URL);
        awaitHealthy(Env.NOTIFICATION_URL);
        lag = new ConsumerLag(Env.KAFKA_BOOTSTRAP);
    }

    @AfterAll
    static void close() {
        lag.close();
    }

    @Test
    void everyCreatedOrderIsNotifiedExactlyOnce_andConsumerLagReturnsToZero() {
        String run = E2eSupport.runId();
        List<UUID> orderIds = createOrders(25, run);

        await("all notifications PROCESSED").atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> assertThat(orderIds).allMatch(E2eSupport::processedExactlyOnce));

        await("consumer lag back to 0").atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500))
                .until(() -> lag.total(Env.CONSUMER_GROUP, Env.TOPIC) == 0);

        // Enrichment really happened: data came from order-service, not from the event.
        assertThat(notificationsFor(orderIds.get(0)).get(0).get("recipient"))
                .isEqualTo("e2e+" + run + "-0@example.com");
    }
}
