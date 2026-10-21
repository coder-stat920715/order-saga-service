package com.example.saga.api;

import com.example.saga.domain.OrderRequest;
import com.example.saga.persistence.OrderRepository;
import com.example.saga.workflow.OrderSagaWorkflow;
import io.temporal.api.enums.v1.WorkflowIdReusePolicy;
import io.temporal.client.WorkflowClient;
import io.temporal.client.WorkflowExecutionAlreadyStarted;
import io.temporal.client.WorkflowOptions;
import java.time.Duration;
import org.springframework.stereotype.Service;

@Service
public class OrderService {

    private final WorkflowClient workflowClient;
    private final OrderRepository orders;

    public OrderService(WorkflowClient workflowClient, OrderRepository orders) {
        this.workflowClient = workflowClient;
        this.orders = orders;
    }

    /**
     * Idempotent: the same sagaId always maps to the same workflow. If the process dies
     * between the insert and the start, the client retries with the same Idempotency-Key
     * and both steps converge (insert is ON CONFLICT DO NOTHING, start tolerates duplicates).
     */
    public void submit(OrderRequest order) {
        orders.insertPending(order);

        OrderSagaWorkflow workflow = workflowClient.newWorkflowStub(
                OrderSagaWorkflow.class,
                WorkflowOptions.newBuilder()
                        .setTaskQueue(OrderSagaWorkflow.TASK_QUEUE)
                        .setWorkflowId(order.sagaId())
                        .setWorkflowIdReusePolicy(
                                WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE)
                        .setWorkflowExecutionTimeout(Duration.ofHours(1))
                        .build());
        try {
            WorkflowClient.start(workflow::processOrder, order);
        } catch (WorkflowExecutionAlreadyStarted alreadyStarted) {
            // duplicate request: the saga already exists, nothing to do
        }
    }

    public com.example.saga.domain.OrderStatus status(String sagaId) {
        return workflowClient.newWorkflowStub(OrderSagaWorkflow.class, sagaId).getStatus();
    }
}
