package com.eventsuite.notification;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.UUID;

/** The consumer's own view of the event: it only needs ids (the rest is enriched over HTTP). */
@JsonIgnoreProperties(ignoreUnknown = true)
public record OrderCreatedEvent(UUID eventId, UUID orderId) {
}
