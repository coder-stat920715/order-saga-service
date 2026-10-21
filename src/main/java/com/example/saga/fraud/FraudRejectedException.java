package com.example.saga.fraud;

/**
 * Business rejection (NOT an infrastructure failure). Thrown by the Temporal activity
 * (Step 5) after a REJECTED decision. Ignored by the circuit breaker and retry config,
 * and mapped to a non-retryable Temporal ApplicationFailure.
 */
public class FraudRejectedException extends RuntimeException {
    public FraudRejectedException(String message) {
        super(message);
    }
}
