package com.eventsuite.notification;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.math.BigDecimal;
import java.util.UUID;

/** HTTP client for order-service. This is the "consumer" side of the Pact contract. */
@Component
public class OrderClient {

    /** Only the fields notification-service actually reads: the contract covers exactly these. */
    public record OrderView(UUID id, String customerEmail, String product, int quantity, BigDecimal amount,
                            String status) {
    }

    private final RestClient http;

    public OrderClient(RestClient.Builder builder, @Value("${app.order-service.url}") String baseUrl) {
        this.http = builder.baseUrl(baseUrl).build();
    }

    public OrderView getOrder(UUID id) {
        return http.get().uri("/orders/{id}", id).retrieve().body(OrderView.class);
    }
}
