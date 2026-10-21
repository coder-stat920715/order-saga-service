package com.example.saga.event;

import com.example.saga.domain.OrderRequest;
import java.math.BigDecimal;
import java.time.Instant;

/**
 * eventId is deterministic (derived from sagaId), so every retry of the publish
 * activity emits the same id and consumers can dedupe safely.
 */
public record OrderCompletedEvent(
        String eventId,
        String sagaId,
        String orderId,
        String customerId,
        String paymentId,
        BigDecimal amount,
        String currency,
        Instant occurredAt) {

    public static OrderCompletedEvent from(OrderRequest o, String paymentId) {
        return new OrderCompletedEvent(
                o.sagaId() + ":order-completed",
                o.sagaId(), o.orderId(), o.customerId(), paymentId,
                o.amount(), o.currency(), Instant.now());
    }
}
