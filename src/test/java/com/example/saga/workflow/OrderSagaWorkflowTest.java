package com.example.saga.workflow;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.example.saga.activity.OrderSagaActivities;
import com.example.saga.domain.OrderItem;
import com.example.saga.domain.OrderRequest;
import com.example.saga.domain.OrderResult;
import com.example.saga.domain.OrderStatus;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowOptions;
import io.temporal.failure.ApplicationFailure;
import io.temporal.testing.TestWorkflowEnvironment;
import io.temporal.worker.Worker;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

/**
 * Runs the real workflow against Temporal's in-memory test server (time-skipping,
 * so retry backoffs don't slow the test) with mocked activities.
 */
class OrderSagaWorkflowTest {

    private static final String SAGA_ID = "saga-1";
    private static final String ORDER_ID = "order-1";

    private TestWorkflowEnvironment env;
    private WorkflowClient client;
    private OrderSagaActivities activities;
    private OrderRequest order;

    @BeforeEach
    void setUp() {
        env = TestWorkflowEnvironment.newInstance();
        Worker worker = env.newWorker(OrderSagaWorkflow.TASK_QUEUE);
        worker.registerWorkflowImplementationTypes(OrderSagaWorkflowImpl.class);

        activities = mock(OrderSagaActivities.class);
        worker.registerActivitiesImplementations(activities);

        env.start();
        client = env.getWorkflowClient();
        order = new OrderRequest(SAGA_ID, ORDER_ID, "cust-1",
                List.of(new OrderItem("ABC", 1)), new BigDecimal("49.90"), "USD");
    }

    @AfterEach
    void tearDown() {
        env.close();
    }

    private OrderResult run() {
        OrderSagaWorkflow workflow = client.newWorkflowStub(
                OrderSagaWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setTaskQueue(OrderSagaWorkflow.TASK_QUEUE)
                        .setWorkflowId(SAGA_ID)
                        .build());
        return workflow.processOrder(order);
    }

    @Test
    void happyPath_completesAndNeverCompensates() {
        when(activities.chargePayment(any())).thenReturn("pay-1");

        OrderResult result = run();

        assertThat(result.status()).isEqualTo(OrderStatus.COMPLETED);
        verify(activities).markOrderCompleted(SAGA_ID, ORDER_ID);
        verify(activities).publishOrderCompleted(any(), eq("pay-1"));
        verify(activities, never()).refundPayment(any());
        verify(activities, never()).releaseInventory(any());
        verify(activities, never()).markOrderFailed(any(), any(), any());
    }

    @Test
    void paymentDeclined_compensatesInReverseOrder() {
        when(activities.chargePayment(any())).thenThrow(
                ApplicationFailure.newNonRetryableFailure("declined", OrderSagaActivities.PAYMENT_DECLINED));

        OrderResult result = run();

        assertThat(result.status()).isEqualTo(OrderStatus.FAILED);
        assertThat(result.reason()).contains("declined");

        InOrder order = inOrder(activities);
        order.verify(activities).refundPayment(any());
        order.verify(activities).releaseInventory(any());
        order.verify(activities).markOrderFailed(eq(SAGA_ID), eq(ORDER_ID), contains("declined"));

        // Post-pivot steps must not run for a failed saga
        verify(activities, never()).markOrderCompleted(any(), any());
        verify(activities, never()).publishOrderCompleted(any(), any());
    }

    @Test
    void outOfStock_skipsPaymentAndRefund() {
        org.mockito.Mockito.doThrow(ApplicationFailure.newNonRetryableFailure(
                        "Insufficient stock", OrderSagaActivities.INSUFFICIENT_STOCK))
                .when(activities).reserveInventory(any());

        OrderResult result = run();

        assertThat(result.status()).isEqualTo(OrderStatus.FAILED);
        verify(activities, never()).chargePayment(any());
        // The release compensation is registered before reserve runs, so it is idempotent-safe to call
        verify(activities).releaseInventory(any());
        verify(activities).markOrderFailed(eq(SAGA_ID), eq(ORDER_ID), contains("stock"));
    }

    @Test
    void fraudRejected_onlyMarksFailed() {
        org.mockito.Mockito.doThrow(ApplicationFailure.newNonRetryableFailure(
                        "Fraud check REJECTED", OrderSagaActivities.FRAUD_REJECTED))
                .when(activities).checkFraud(any());

        OrderResult result = run();

        assertThat(result.status()).isEqualTo(OrderStatus.FAILED);
        verify(activities, never()).reserveInventory(any());
        verify(activities, never()).releaseInventory(any());
        verify(activities, never()).refundPayment(any());
        verify(activities).markOrderFailed(eq(SAGA_ID), eq(ORDER_ID), contains("REJECTED"));
    }
}
