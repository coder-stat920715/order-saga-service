package com.example.saga.messaging;

import com.example.saga.event.OrderCompletedEvent;
import com.example.saga.idempotency.IdempotencyService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Downstream notification handler. Idempotent: the side effect runs once per eventId
 * even with redelivery, rebalances, or the publisher's at-least-once duplicates.
 *
 * Offsets commit per record (ack-mode: record) only after this method returns.
 * IdempotencyService.run() releases the key if the handler throws, so the error
 * handler's retry re-runs it instead of skipping it.
 */
@Component
public class OrderCompletedNotificationListener {

    private static final Logger log = LoggerFactory.getLogger(OrderCompletedNotificationListener.class);

    private final IdempotencyService idempotency;

    public OrderCompletedNotificationListener(IdempotencyService idempotency) {
        this.idempotency = idempotency;
    }

    @KafkaListener(topics = "${app.kafka.topics.order-completed}")
    public void onOrderCompleted(OrderCompletedEvent event) {
        // "notify:" namespaces this consumer; other consumers of the topic use their own prefix.
        idempotency.run("notify:" + event.eventId(), () -> sendNotification(event));
    }

    private void sendNotification(OrderCompletedEvent event) {
        // Replace with an email/SMS/push gateway call (pass event.eventId() as its idempotency key).
        log.info("Notifying customer {} about order {} ({} {})",
                event.customerId(), event.orderId(), event.amount(), event.currency());
    }
}
