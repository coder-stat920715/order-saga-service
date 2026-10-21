package com.example.saga.activity;

import com.example.saga.domain.OrderRequest;
import io.temporal.activity.ActivityInterface;

/**
 * Every method derives its idempotency key from order.sagaId() + step name
 * (implemented in Step 5). Compensations must be safe to run when the forward
 * step never happened or ran only partially.
 */
@ActivityInterface
public interface OrderSagaActivities {

    // Non-retryable business error types (thrown as ApplicationFailure by the impl)
    String FRAUD_REJECTED = "FraudRejected";
    String INSUFFICIENT_STOCK = "InsufficientStock";
    String PAYMENT_DECLINED = "PaymentDeclined";

    // ---- forward steps ----
    void checkFraud(OrderRequest order);

    void reserveInventory(OrderRequest order);

    /** @return the payment id issued by the payment service */
    String chargePayment(OrderRequest order);

    // ---- compensations ----
    void refundPayment(OrderRequest order);

    void releaseInventory(OrderRequest order);

    void markOrderFailed(String sagaId, String orderId, String reason);

    // ---- post-pivot steps (never compensated) ----
    void markOrderCompleted(String sagaId, String orderId);

    void publishOrderCompleted(OrderRequest order, String paymentId);
}
