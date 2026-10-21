package com.example.saga.client;

import com.example.saga.domain.OrderItem;
import java.util.List;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

/** Plain client: Temporal's activity retry is the resilience layer here. */
@Component
public class InventoryClient {

    public record ReserveRequest(String sagaId, List<OrderItem> items) {}

    private final RestClient rest;

    public InventoryClient(@Qualifier("inventoryRestClient") RestClient rest) {
        this.rest = rest;
    }

    /** 409 Conflict => insufficient stock. */
    public void reserve(String sagaId, List<OrderItem> items, String idemKey) {
        rest.post().uri("/api/v1/inventory/reservations")
                .header("Idempotency-Key", idemKey)
                .body(new ReserveRequest(sagaId, items))
                .retrieve().toBodilessEntity();
    }

    /** Safe when nothing was reserved: 404 is treated as success. */
    public void release(String sagaId, String idemKey) {
        try {
            rest.delete().uri("/api/v1/inventory/reservations/{id}", sagaId)
                    .header("Idempotency-Key", idemKey)
                    .retrieve().toBodilessEntity();
        } catch (HttpClientErrorException.NotFound ignored) {
            // never reserved, nothing to release
        }
    }
}
