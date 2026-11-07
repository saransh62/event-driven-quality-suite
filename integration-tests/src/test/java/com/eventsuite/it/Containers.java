package com.eventsuite.it;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Singleton containers: started once per JVM and shared by every test class, so the suite pays
 * the start-up cost once. Ryuk removes them when the JVM exits.
 */
public final class Containers {

    public static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka:3.9.1"));

    /** Each service owns its database, exactly as in docker-compose. */
    public static final PostgreSQLContainer<?> ORDER_DB = postgres("orders");
    public static final PostgreSQLContainer<?> NOTIFICATION_DB = postgres("notifications");

    static {
        KAFKA.start();
        ORDER_DB.start();
        NOTIFICATION_DB.start();
    }

    private Containers() {
    }

    private static PostgreSQLContainer<?> postgres(String db) {
        return new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                .withDatabaseName(db).withUsername("app").withPassword("app");
    }
}
