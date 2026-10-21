package com.example.saga.fraud;

import java.math.BigDecimal;
import java.util.Objects;

/** Immutable request sent to the Fraud Detection Service. */
public record FraudCheckRequest(
        String sagaId,
        String customerId,
        BigDecimal amount,
        String currency) {

    public FraudCheckRequest {
        Objects.requireNonNull(sagaId, "sagaId");
        Objects.requireNonNull(customerId, "customerId");
        Objects.requireNonNull(amount, "amount");
        Objects.requireNonNull(currency, "currency");
        if (amount.signum() <= 0) {
            throw new IllegalArgumentException("amount must be positive");
        }
    }
}
