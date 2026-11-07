package com.eventsuite.notification;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

/** Read-only view used by the e2e tests to observe the outcome of async processing. */
@RestController
@RequestMapping("/notifications")
public class NotificationController {

    public record NotificationResponse(Long id, UUID eventId, UUID orderId, String recipient, String message,
                                       String status, Instant receivedAt, Instant processedAt) {
    }

    private final NotificationRepository repository;

    public NotificationController(NotificationRepository repository) {
        this.repository = repository;
    }

    @GetMapping
    public List<NotificationResponse> byOrder(@RequestParam UUID orderId) {
        return repository.findByOrderId(orderId).stream()
                .map(n -> new NotificationResponse(n.getId(), n.getEventId(), n.getOrderId(), n.getRecipient(),
                        n.getMessage(), n.getStatus().name(), n.getReceivedAt(), n.getProcessedAt()))
                .toList();
    }
}
