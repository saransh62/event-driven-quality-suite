package com.eventsuite.e2e;

/** Where the docker-compose stack is reachable from the test JVM (override with -D...). */
final class Env {

    static final String ORDER_URL = System.getProperty("order.url", "http://localhost:8081");
    static final String NOTIFICATION_URL = System.getProperty("notification.url", "http://localhost:8082");
    static final String KAFKA_BOOTSTRAP = System.getProperty("kafka.bootstrap", "localhost:9094");
    static final String COMPOSE_FILE = System.getProperty("compose.file", "../docker-compose.yml");

    static final String TOPIC = "orders.created";
    static final String CONSUMER_GROUP = "notification-service";

    private Env() {
    }
}
