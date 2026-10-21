package com.example.saga.client;

import java.math.BigDecimal;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

@Component
public class PaymentClient {

    public record ChargeRequest(String sagaId, String customerId, BigDecimal amount, String currency) {}
    public record ChargeResponse(String paymentId) {}

    private final RestClient rest;

    public PaymentClient(@Qualifier("paymentRestClient") RestClient rest) {
        this.rest = rest;
    }

    /** 402 Payment Required => card declined. */
    public ChargeResponse charge(ChargeRequest request, String idemKey) {
        return rest.post().uri("/api/v1/payments")
                .header("Idempotency-Key", idemKey)
                .body(request)
                .retrieve().body(ChargeResponse.class);
    }

    /** Safe when nothing was charged: 404 is treated as success. */
    public void refund(String sagaId, String idemKey) {
        try {
            rest.post().uri("/api/v1/payments/{id}/refund", sagaId)
                    .header("Idempotency-Key", idemKey)
                    .retrieve().toBodilessEntity();
        } catch (HttpClientErrorException.NotFound ignored) {
            // never charged, nothing to refund
        }
    }
}
