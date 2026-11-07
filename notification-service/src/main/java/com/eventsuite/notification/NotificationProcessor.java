package com.eventsuite.notification;

import com.eventsuite.notification.OrderClient.OrderView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class NotificationProcessor {

    private static final Logger log = LoggerFactory.getLogger(NotificationProcessor.class);

    private final NotificationRepository repository;
    private final OrderClient orders;

    public NotificationProcessor(NotificationRepository repository, OrderClient orders) {
        this.repository = repository;
        this.orders = orders;
    }

    /**
     * Idempotent: an eventId seen before is skipped. The unique constraint on event_id is the
     * backstop if two deliveries race. Everything runs in one transaction, so a failure while
     * enriching leaves no half-written row behind and the retry starts clean.
     *
     * @return true if a notification was created, false if the event was a duplicate
     */
    @Transactional
    public boolean process(OrderCreatedEvent event) {
        if (repository.existsByEventId(event.eventId())) {
            log.info("Duplicate event {} for order {} ignored", event.eventId(), event.orderId());
            return false;
        }
        Notification n = repository.save(new Notification(event.eventId(), event.orderId()));
        OrderView order = orders.getOrder(event.orderId());
        n.markProcessed(order.customerEmail(),
                "Hi! Your order of %d x %s (total %s) is confirmed."
                        .formatted(order.quantity(), order.product(), order.amount()));
        return true;
    }
}
