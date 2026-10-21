package com.example.saga.workflow;

import com.example.saga.activity.OrderSagaActivities;
import com.example.saga.domain.OrderRequest;
import com.example.saga.domain.OrderResult;
import com.example.saga.domain.OrderStatus;
import io.temporal.activity.ActivityOptions;
import io.temporal.common.RetryOptions;
import io.temporal.failure.CanceledFailure;
import io.temporal.failure.TemporalFailure;
import io.temporal.spring.boot.WorkflowImpl;
import io.temporal.workflow.Saga;
import io.temporal.workflow.Workflow;
import java.time.Duration;
import org.slf4j.Logger;

/**
 * Orchestrated saga. Must stay deterministic: no I/O, no random, no system clock,
 * no Spring beans. All side effects go through activities.
 *
 * Ordering (compensations run in REVERSE registration order):
 *   register markOrderFailed -> checkFraud
 *   register releaseInventory -> reserveInventory
 *   register refundPayment   -> chargePayment   (pivot)
 * Failure after the pivot's registration therefore runs: refund, release, markFailed.
 */
@WorkflowImpl(taskQueues = OrderSagaWorkflow.TASK_QUEUE)
public class OrderSagaWorkflowImpl implements OrderSagaWorkflow {

    private static final Logger log = Workflow.getLogger(OrderSagaWorkflowImpl.class);

    // Fraud activity wraps Resilience4j (up to 3 x 3s + backoff), so give it headroom.
    private final OrderSagaActivities fraudActs = Workflow.newActivityStub(
            OrderSagaActivities.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(20))
                    .setRetryOptions(RetryOptions.newBuilder()
                            .setInitialInterval(Duration.ofSeconds(1))
                            .setBackoffCoefficient(2.0)
                            .setMaximumInterval(Duration.ofSeconds(10))
                            .setMaximumAttempts(4)
                            .setDoNotRetry(OrderSagaActivities.FRAUD_REJECTED)
                            .build())
                    .build());

    // Forward steps: bounded retries, business errors are non-retryable.
    private final OrderSagaActivities stepActs = Workflow.newActivityStub(
            OrderSagaActivities.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(30))
                    .setRetryOptions(RetryOptions.newBuilder()
                            .setInitialInterval(Duration.ofSeconds(1))
                            .setBackoffCoefficient(2.0)
                            .setMaximumInterval(Duration.ofSeconds(15))
                            .setMaximumAttempts(5)
                            .setDoNotRetry(
                                    OrderSagaActivities.INSUFFICIENT_STOCK,
                                    OrderSagaActivities.PAYMENT_DECLINED)
                            .build())
                    .build());

    // Compensations and post-pivot steps: retry (effectively) forever, they must not give up.
    private final OrderSagaActivities durableActs = Workflow.newActivityStub(
            OrderSagaActivities.class,
            ActivityOptions.newBuilder()
                    .setStartToCloseTimeout(Duration.ofSeconds(30))
                    .setRetryOptions(RetryOptions.newBuilder()
                            .setInitialInterval(Duration.ofSeconds(1))
                            .setBackoffCoefficient(2.0)
                            .setMaximumInterval(Duration.ofMinutes(1))
                            .setMaximumAttempts(0) // 0 = unlimited
                            .build())
                    .build());

    private OrderStatus status = OrderStatus.PENDING;
    private String failureReason = "unknown";

    @Override
    public OrderResult processOrder(OrderRequest order) {
        status = OrderStatus.IN_PROGRESS;

        Saga saga = new Saga(new Saga.Options.Builder()
                .setParallelCompensation(false)
                .setContinueWithError(true) // keep unwinding even if one compensation throws
                .build());

        String paymentId;
        try {
            // Registered first => runs last. Reason is read lazily when it executes.
            saga.addCompensation(() ->
                    durableActs.markOrderFailed(order.sagaId(), order.orderId(), failureReason));

            fraudActs.checkFraud(order);

            // Register BEFORE running: if the activity succeeded remotely but the
            // response was lost, the compensation still cleans up (it is idempotent).
            saga.addCompensation(() -> durableActs.releaseInventory(order));
            stepActs.reserveInventory(order);

            saga.addCompensation(() -> durableActs.refundPayment(order));
            paymentId = stepActs.chargePayment(order);

        } catch (TemporalFailure e) {
            failureReason = rootMessage(e);
            status = OrderStatus.COMPENSATING;
            log.warn("Saga {} failed, compensating: {}", order.sagaId(), failureReason);

            try {
                // Detached scope: compensation still runs if the workflow was cancelled.
                Workflow.newDetachedCancellationScope(saga::compensate).run();
            } catch (Saga.CompensationException ce) {
                status = OrderStatus.COMPENSATION_FAILED;
                log.error("Saga {} compensation incomplete", order.sagaId(), ce);
                return OrderResult.compensationFailed(order.sagaId(), failureReason);
            }

            if (e instanceof CanceledFailure) {
                throw e; // preserve cancellation semantics after cleanup
            }
            status = OrderStatus.FAILED;
            return OrderResult.failed(order.sagaId(), failureReason);
        }

        // ---- Past the pivot: payment succeeded. Never refund because of what follows. ----
        durableActs.markOrderCompleted(order.sagaId(), order.orderId());
        durableActs.publishOrderCompleted(order, paymentId);
        status = OrderStatus.COMPLETED;
        return OrderResult.completed(order.sagaId());
    }

    @Override
    public OrderStatus getStatus() {
        return status;
    }

    private static String rootMessage(Throwable t) {
        Throwable root = t;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return root.getMessage() != null ? root.getMessage() : root.getClass().getSimpleName();
    }
}
