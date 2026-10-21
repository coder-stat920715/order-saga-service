package com.example.saga.domain;

public record OrderResult(String sagaId, OrderStatus status, String reason) {

    public static OrderResult completed(String sagaId) {
        return new OrderResult(sagaId, OrderStatus.COMPLETED, null);
    }

    public static OrderResult failed(String sagaId, String reason) {
        return new OrderResult(sagaId, OrderStatus.FAILED, reason);
    }

    public static OrderResult compensationFailed(String sagaId, String reason) {
        return new OrderResult(sagaId, OrderStatus.COMPENSATION_FAILED, reason);
    }
}
