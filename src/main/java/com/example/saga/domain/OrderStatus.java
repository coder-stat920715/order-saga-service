package com.example.saga.domain;

public enum OrderStatus {
    PENDING,
    IN_PROGRESS,
    COMPENSATING,
    COMPLETED,
    FAILED,
    COMPENSATION_FAILED
}
