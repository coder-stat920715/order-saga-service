package com.example.saga.workflow;

import com.example.saga.domain.OrderRequest;
import com.example.saga.domain.OrderResult;
import com.example.saga.domain.OrderStatus;
import io.temporal.workflow.QueryMethod;
import io.temporal.workflow.WorkflowInterface;
import io.temporal.workflow.WorkflowMethod;

@WorkflowInterface
public interface OrderSagaWorkflow {

    String TASK_QUEUE = "ORDER_SAGA_TASK_QUEUE";

    @WorkflowMethod
    OrderResult processOrder(OrderRequest order);

    @QueryMethod
    OrderStatus getStatus();
}
