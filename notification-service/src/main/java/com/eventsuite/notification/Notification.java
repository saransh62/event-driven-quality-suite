package com.eventsuite.notification;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "notifications")
public class Notification {

    public enum Status { RECEIVED, PROCESSED }

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** Unique: the idempotency key. A redelivered event has the same eventId. */
    @Column(name = "event_id", nullable = false, unique = true)
    private UUID eventId;

    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Column(name = "recipient")
    private String recipient;

    @Column(name = "message", length = 1000)
    private String message;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private Status status;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;

    @Column(name = "processed_at")
    private Instant processedAt;

    protected Notification() {
    }

    public Notification(UUID eventId, UUID orderId) {
        this.eventId = eventId;
        this.orderId = orderId;
        this.status = Status.RECEIVED;
        this.receivedAt = Instant.now();
    }

    public void markProcessed(String recipient, String message) {
        this.recipient = recipient;
        this.message = message;
        this.status = Status.PROCESSED;
        this.processedAt = Instant.now();
    }

    public Long getId() { return id; }
    public UUID getEventId() { return eventId; }
    public UUID getOrderId() { return orderId; }
    public String getRecipient() { return recipient; }
    public String getMessage() { return message; }
    public Status getStatus() { return status; }
    public Instant getReceivedAt() { return receivedAt; }
    public Instant getProcessedAt() { return processedAt; }
}
