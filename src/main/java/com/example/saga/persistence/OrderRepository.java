package com.example.saga.persistence;

import com.example.saga.domain.OrderRequest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class OrderRepository {

    private final JdbcClient jdbc;

    public OrderRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public void insertPending(OrderRequest o) {
        jdbc.sql("""
                INSERT INTO orders (order_id, saga_id, customer_id, amount, currency, status)
                VALUES (:id, :saga, :cust, :amt, :cur, 'PENDING')
                ON CONFLICT (order_id) DO NOTHING
                """)
                .param("id", o.orderId()).param("saga", o.sagaId())
                .param("cust", o.customerId()).param("amt", o.amount())
                .param("cur", o.currency())
                .update();
    }

    /** Terminal states never overwrite each other, so replays are harmless. */
    public void markCompleted(String orderId) {
        jdbc.sql("""
                UPDATE orders SET status = 'COMPLETED', updated_at = now()
                 WHERE order_id = :id AND status <> 'FAILED'
                """)
                .param("id", orderId).update();
    }

    public void markFailed(String orderId, String reason) {
        jdbc.sql("""
                UPDATE orders SET status = 'FAILED', failure_reason = :r, updated_at = now()
                 WHERE order_id = :id AND status <> 'COMPLETED'
                """)
                .param("id", orderId).param("r", reason).update();
    }
}
