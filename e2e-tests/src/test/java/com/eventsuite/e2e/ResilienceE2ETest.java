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
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Consumer outage: events published while notification-service is down must wait in Kafka
 * (lag grows) and be processed exactly once after it comes back (lag drains to 0).
 */
class ResilienceE2ETest {

    private static final String SERVICE = "notification-service";
    private static ConsumerLag lag;

    @BeforeAll
    static void stackIsUp() {
        awaitHealthy(Env.ORDER_URL);
        awaitHealthy(Env.NOTIFICATION_URL);
        lag = new ConsumerLag(Env.KAFKA_BOOTSTRAP);
    }

    @AfterAll
    static void close() {
        DockerCompose.start(SERVICE);   // never leave the stack broken for the next test
        lag.close();
    }

    @Test
    void backlogBuiltWhileConsumerIsDown_isFullyProcessedAfterRestart_withNoDuplicates() {
        await("clean starting point").atMost(Duration.ofSeconds(30))
                .until(() -> lag.total(Env.CONSUMER_GROUP, Env.TOPIC) == 0);

        DockerCompose.stop(SERVICE);
        int backlog = 30;
        List<UUID> orderIds = createOrders(backlog, E2eSupport.runId());

        // Nobody is consuming: every event is waiting in Kafka.
        assertThat(lag.total(Env.CONSUMER_GROUP, Env.TOPIC)).isEqualTo(backlog);

        DockerCompose.start(SERVICE);
        awaitHealthy(Env.NOTIFICATION_URL);

        await("backlog drained").atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofSeconds(1))
                .until(() -> lag.total(Env.CONSUMER_GROUP, Env.TOPIC) == 0);
        await("all processed").atMost(Duration.ofSeconds(30)).pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> assertThat(orderIds).allMatch(E2eSupport::processedExactlyOnce));

        // No duplicates: exactly one notification per order across the whole backlog.
        long total = orderIds.stream().mapToLong(id -> notificationsFor(id).size()).sum();
        assertThat(total).isEqualTo(backlog);
    }
}
