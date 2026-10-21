package com.example.saga.api;

import com.example.saga.domain.OrderItem;
import com.example.saga.domain.OrderRequest;
import com.example.saga.domain.OrderStatus;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/orders")
public class OrderController {

    public record CreateOrderRequest(
            @NotBlank String customerId,
            @NotEmpty List<OrderItem> items,
            @NotNull @Positive BigDecimal amount,
            @NotBlank @Size(min = 3, max = 3) String currency) {}

    public record Accepted(String sagaId, String orderId) {}

    private final OrderService service;

    public OrderController(OrderService service) {
        this.service = service;
    }

    @PostMapping
    public ResponseEntity<Accepted> create(
            @RequestHeader(value = "Idempotency-Key", required = false) String idemKey,
            @Valid @RequestBody CreateOrderRequest req) {

        String sagaId = (idemKey != null && !idemKey.isBlank()) ? idemKey : UUID.randomUUID().toString();
        // Deterministic orderId so client retries with the same key hit the same row.
        String orderId = UUID.nameUUIDFromBytes(sagaId.getBytes(StandardCharsets.UTF_8)).toString();

        service.submit(new OrderRequest(
                sagaId, orderId, req.customerId(), req.items(), req.amount(), req.currency()));
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(new Accepted(sagaId, orderId));
    }

    @GetMapping("/{sagaId}/status")
    public OrderStatus status(@PathVariable String sagaId) {
        return service.status(sagaId);
    }
}
