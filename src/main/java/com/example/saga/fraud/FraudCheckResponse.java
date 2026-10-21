package com.example.saga.fraud;

/** Response from the Fraud Detection Service. */
public record FraudCheckResponse(
        String sagaId,
        Decision decision,
        int riskScore,
        String reason) {

    public enum Decision { APPROVED, REJECTED, MANUAL_REVIEW }

    public boolean isApproved() {
        return decision == Decision.APPROVED;
    }
}
