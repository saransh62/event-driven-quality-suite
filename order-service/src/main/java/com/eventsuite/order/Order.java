package com.eventsuite.order;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "orders")
public class Order {

    public enum Status { CREATED }

    @Id
    private UUID id;

    @Column(name = "customer_email", nullable = false)
    private String customerEmail;

    @Column(nullable = false)
    private String product;

    @Column(nullable = false)
    private int quantity;

    @Column(nullable = false)
    private BigDecimal amount;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Status status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected Order() {
    }

    public Order(UUID id, String customerEmail, String product, int quantity, BigDecimal amount) {
        this.id = id;
        this.customerEmail = customerEmail;
        this.product = product;
        this.quantity = quantity;
        this.amount = amount;
        this.status = Status.CREATED;
        this.createdAt = Instant.now();
    }

    public UUID getId() { return id; }
    public String getCustomerEmail() { return customerEmail; }
    public String getProduct() { return product; }
    public int getQuantity() { return quantity; }
    public BigDecimal getAmount() { return amount; }
    public Status getStatus() { return status; }
    public Instant getCreatedAt() { return createdAt; }
}
