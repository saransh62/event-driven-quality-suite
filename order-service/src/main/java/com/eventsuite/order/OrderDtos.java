package com.eventsuite.order;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public final class OrderDtos {

    private OrderDtos() {
    }

    public record CreateOrderRequest(
            @NotBlank @Email String customerEmail,
            @NotBlank String product,
            @NotNull @Min(1) Integer quantity,
            @NotNull @DecimalMin("0.01") BigDecimal amount) {
    }

    public record OrderResponse(UUID id, String customerEmail, String product, int quantity,
                                BigDecimal amount, String status, Instant createdAt) {

        static OrderResponse from(Order o) {
            return new OrderResponse(o.getId(), o.getCustomerEmail(), o.getProduct(), o.getQuantity(),
                    o.getAmount(), o.getStatus().name(), o.getCreatedAt());
        }
    }

    /** Payload published to orders.created. eventId lets consumers deduplicate redeliveries. */
    public record OrderCreatedEvent(UUID eventId, UUID orderId, String customerEmail, String product,
                                    int quantity, BigDecimal amount, Instant createdAt) {

        static OrderCreatedEvent from(Order o) {
            return new OrderCreatedEvent(UUID.randomUUID(), o.getId(), o.getCustomerEmail(), o.getProduct(),
                    o.getQuantity(), o.getAmount(), o.getCreatedAt());
        }
    }
}
