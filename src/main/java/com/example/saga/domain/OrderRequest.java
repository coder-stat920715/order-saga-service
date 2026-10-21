package com.example.saga.domain;

import java.math.BigDecimal;
import java.util.List;

/**
 * Workflow input. sagaId doubles as the Temporal workflowId and as the base of
 * every idempotency key (sagaId + ":" + stepName).
 */
public record OrderRequest(
        String sagaId,
        String orderId,
        String customerId,
        List<OrderItem> items,
        BigDecimal amount,
        String currency) {
}
