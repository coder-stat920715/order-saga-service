package com.example.saga.domain;

public record OrderItem(String sku, int quantity) {
    public OrderItem {
        if (quantity <= 0) throw new IllegalArgumentException("quantity must be positive");
    }
}
