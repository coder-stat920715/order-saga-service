package com.example.saga.activity;

import com.example.saga.client.InventoryClient;
import com.example.saga.client.PaymentClient;
import com.example.saga.domain.OrderRequest;
import com.example.saga.event.OrderCompletedEvent;
import com.example.saga.event.OrderEventPublisher; // Step 6
import com.example.saga.fraud.FraudCheckClient;
import com.example.saga.fraud.FraudCheckRequest;
import com.example.saga.fraud.FraudCheckResponse;
import com.example.saga.fraud.FraudRejectedException;
import com.example.saga.idempotency.IdempotencyService;
import com.example.saga.persistence.OrderRepository;
import io.temporal.failure.ApplicationFailure;
import io.temporal.spring.boot.ActivityImpl;
import java.util.concurrent.CompletionException;
import org.springframework.web.client.HttpClientErrorException;
import com.example.saga.workflow.OrderSagaWorkflow;

/**
 * Registered as a Spring bean by @ActivityImpl, so constructor injection works.
 * Every method wraps its work in IdempotencyService with key = sagaId:step, and
 * passes that same key downstream as an Idempotency-Key header.
 */
@ActivityImpl(taskQueues = OrderSagaWorkflow.TASK_QUEUE)
public class OrderSagaActivitiesImpl implements OrderSagaActivities {

    private final IdempotencyService idempotency;
    private final FraudCheckClient fraudClient;
    private final InventoryClient inventory;
    private final PaymentClient payment;
    private final OrderRepository orders;
    private final OrderEventPublisher publisher;

    public OrderSagaActivitiesImpl(IdempotencyService idempotency, FraudCheckClient fraudClient,
                                   InventoryClient inventory, PaymentClient payment,
                                   OrderRepository orders, OrderEventPublisher publisher) {
        this.idempotency = idempotency;
        this.fraudClient = fraudClient;
        this.inventory = inventory;
        this.payment = payment;
        this.orders = orders;
        this.publisher = publisher;
    }

    // ---------------- forward steps ----------------

    @Override
    public void checkFraud(OrderRequest o) {
        try {
            idempotency.run(key(o, "fraud-check"), () -> {
                FraudCheckResponse r = callFraudService(o);
                if (!r.isApproved()) {
                    throw new FraudRejectedException(
                            "Fraud check " + r.decision() + ": " + r.reason());
                }
            });
        } catch (FraudRejectedException e) {
            throw ApplicationFailure.newNonRetryableFailure(e.getMessage(), FRAUD_REJECTED);
        }
    }

    @Override
    public void reserveInventory(OrderRequest o) {
        String k = key(o, "reserve-inventory");
        try {
            idempotency.run(k, () -> inventory.reserve(o.sagaId(), o.items(), k));
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode().value() == 409) {
                throw ApplicationFailure.newNonRetryableFailure("Insufficient stock", INSUFFICIENT_STOCK);
            }
            throw badRequest(e);
        }
    }

    @Override
    public String chargePayment(OrderRequest o) {
        String k = key(o, "charge-payment");
        try {
            return idempotency.execute(k, String.class, () -> payment.charge(
                    new PaymentClient.ChargeRequest(o.sagaId(), o.customerId(), o.amount(), o.currency()),
                    k).paymentId());
        } catch (HttpClientErrorException e) {
            if (e.getStatusCode().value() == 402) {
                throw ApplicationFailure.newNonRetryableFailure("Payment declined", PAYMENT_DECLINED);
            }
            throw badRequest(e);
        }
    }

    // ---------------- compensations ----------------

    @Override
    public void refundPayment(OrderRequest o) {
        String k = key(o, "refund-payment");
        idempotency.run(k, () -> payment.refund(o.sagaId(), k));
    }

    @Override
    public void releaseInventory(OrderRequest o) {
        String k = key(o, "release-inventory");
        idempotency.run(k, () -> inventory.release(o.sagaId(), k));
    }

    @Override
    public void markOrderFailed(String sagaId, String orderId, String reason) {
        idempotency.run(sagaId + ":mark-failed", () -> orders.markFailed(orderId, reason));
    }

    // ---------------- post-pivot ----------------

    @Override
    public void markOrderCompleted(String sagaId, String orderId) {
        idempotency.run(sagaId + ":mark-completed", () -> orders.markCompleted(orderId));
    }

    @Override
    public void publishOrderCompleted(OrderRequest o, String paymentId) {
        // Crash between send and ledger update => duplicate send; the consumer dedupes by eventId.
        idempotency.run(key(o, "publish-completed"),
                () -> publisher.publish(OrderCompletedEvent.from(o, paymentId)));
    }

    // ---------------- helpers ----------------

    private FraudCheckResponse callFraudService(OrderRequest o) {
        try {
            return fraudClient.checkAsync(new FraudCheckRequest(
                    o.sagaId(), o.customerId(), o.amount(), o.currency())).join();
        } catch (CompletionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof HttpClientErrorException h) {
                throw badRequest(h);
            }
            if (cause instanceof RuntimeException re) {
                throw re; // FraudServiceUnavailableException => retryable by Temporal
            }
            throw e;
        }
    }

    private static RuntimeException badRequest(HttpClientErrorException e) {
        return ApplicationFailure.newNonRetryableFailure(
                "Downstream rejected request: " + e.getStatusCode(), "BadRequest");
    }

    private static String key(OrderRequest o, String step) {
        return o.sagaId() + ":" + step;
    }
}
