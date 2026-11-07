package com.eventsuite.e2e;

import io.restassured.http.ContentType;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.IntStream;

import static io.restassured.RestAssured.given;
import static org.awaitility.Awaitility.await;

final class E2eSupport {

    private E2eSupport() {
    }

    static List<UUID> createOrders(int count, String runId) {
        return IntStream.range(0, count).mapToObj(i -> UUID.fromString(given()
                        .baseUri(Env.ORDER_URL).contentType(ContentType.JSON)
                        .body(Map.of("customerEmail", "e2e+" + runId + "-" + i + "@example.com",
                                "product", "Widget-" + i, "quantity", 1 + i % 3, "amount", 10 + i))
                        .when().post("/orders")
                        .then().statusCode(201)
                        .extract().<String>path("id")))
                .toList();
    }

    /** Notifications stored for one order, as returned by notification-service. */
    static List<Map<String, Object>> notificationsFor(UUID orderId) {
        return given().baseUri(Env.NOTIFICATION_URL).queryParam("orderId", orderId)
                .when().get("/notifications")
                .then().statusCode(200)
                .extract().jsonPath().getList("$");
    }

    static boolean processedExactlyOnce(UUID orderId) {
        List<Map<String, Object>> list = notificationsFor(orderId);
        return list.size() == 1 && "PROCESSED".equals(list.get(0).get("status"));
    }

    static void awaitHealthy(String baseUrl) {
        await("health of " + baseUrl).atMost(Duration.ofSeconds(90)).pollInterval(Duration.ofSeconds(1))
                .ignoreExceptions()
                .until(() -> given().baseUri(baseUrl).get("/actuator/health").statusCode() == 200);
    }

    static String runId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }
}
