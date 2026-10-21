package com.example.saga.fraud;

/**
 * Infrastructure failure after Resilience4j gave up (circuit open, rate limited,
 * timeout, retries exhausted). Retryable at the Temporal activity level.
 */
public class FraudServiceUnavailableException extends RuntimeException {
    public FraudServiceUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
