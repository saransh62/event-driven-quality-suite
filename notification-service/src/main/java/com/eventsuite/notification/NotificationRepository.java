package com.eventsuite.notification;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface NotificationRepository extends JpaRepository<Notification, Long> {

    boolean existsByEventId(UUID eventId);

    List<Notification> findByOrderId(UUID orderId);
}
