package com.example.saga.idempotency;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.temporal.failure.ApplicationFailure;
import java.util.Optional;
import java.util.function.Supplier;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Service;

/**
 * Deterministic idempotency for activities and message handlers, backed by PostgreSQL.
 *
 *  1. INSERT ... ON CONFLICT DO NOTHING acquires the key (atomic, race-free).
 *  2. Key already COMPLETED       -> return the stored result, skip the action.
 *  3. Key IN_PROGRESS, lease live -> throw a retryable failure (another attempt is running).
 *  4. Key IN_PROGRESS, lease dead -> take over (previous worker crashed) and re-run.
 *  5. Action throws               -> release the key so the next attempt can run.
 *
 * No DB transaction spans the action: remote calls must not hold connections.
 * Re-execution after a crash is made safe by passing the same key downstream
 * as an Idempotency-Key header. The lease MUST exceed the activity startToClose timeout.
 */
@Service
public class IdempotencyService {

    private static final long LEASE_SECONDS = 90;

    private final JdbcClient jdbc;
    private final ObjectMapper mapper;

    public IdempotencyService(JdbcClient jdbc, ObjectMapper mapper) {
        this.jdbc = jdbc;
        this.mapper = mapper;
    }

    public <T> T execute(String key, Class<T> type, Supplier<T> action) {
        if (!tryAcquire(key)) {
            Optional<Row> existing = find(key);
            if (existing.isPresent() && "COMPLETED".equals(existing.get().status())) {
                return deserialize(existing.get().result(), type);
            }
            if (!takeOverExpiredLease(key)) {
                throw ApplicationFailure.newFailure(
                        "Idempotency key in progress: " + key, "IdempotencyInProgress");
            }
        }

        T result;
        try {
            result = action.get();
        } catch (RuntimeException e) {
            release(key);
            throw e;
        }
        complete(key, serialize(result));
        return result;
    }

    public void run(String key, Runnable action) {
        execute(key, Void.class, () -> {
            action.run();
            return null;
        });
    }

    /** For Kafka handlers: true if this call won the key and should process the message. */
    public boolean markProcessedOnce(String key) {
        if (tryAcquire(key)) {
            complete(key, "null");
            return true;
        }
        return false;
    }

    // ---- SQL ----

    private boolean tryAcquire(String key) {
        return jdbc.sql("""
                INSERT INTO idempotency_record (idem_key, status, locked_until)
                VALUES (:k, 'IN_PROGRESS', now() + (:secs * interval '1 second'))
                ON CONFLICT (idem_key) DO NOTHING
                """)
                .param("k", key).param("secs", LEASE_SECONDS)
                .update() == 1;
    }

    private boolean takeOverExpiredLease(String key) {
        return jdbc.sql("""
                UPDATE idempotency_record
                   SET locked_until = now() + (:secs * interval '1 second'), updated_at = now()
                 WHERE idem_key = :k AND status = 'IN_PROGRESS' AND locked_until < now()
                """)
                .param("k", key).param("secs", LEASE_SECONDS)
                .update() == 1;
    }

    private Optional<Row> find(String key) {
        return jdbc.sql("SELECT status, result FROM idempotency_record WHERE idem_key = :k")
                .param("k", key)
                .query((rs, i) -> new Row(rs.getString("status"), rs.getString("result")))
                .optional();
    }

    private void complete(String key, String json) {
        jdbc.sql("""
                UPDATE idempotency_record
                   SET status = 'COMPLETED', result = :r, updated_at = now()
                 WHERE idem_key = :k
                """)
                .param("k", key).param("r", json)
                .update();
    }

    private void release(String key) {
        jdbc.sql("DELETE FROM idempotency_record WHERE idem_key = :k AND status = 'IN_PROGRESS'")
                .param("k", key)
                .update();
    }

    // ---- JSON ----

    private String serialize(Object value) {
        try {
            return mapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize idempotent result", e);
        }
    }

    private <T> T deserialize(String json, Class<T> type) {
        try {
            return mapper.readValue(json, type);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot deserialize idempotent result", e);
        }
    }

    private record Row(String status, String result) {}
}
