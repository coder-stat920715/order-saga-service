package com.example.saga.event;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Publishes directly from the Temporal activity. No outbox: the activity blocks until
 * the broker acks (acks=all + idempotent producer). If that fails, the activity throws
 * and Temporal retries it, which is the durability mechanism.
 *
 * Delivery is at-least-once: a crash after the ack but before the idempotency ledger
 * update re-sends the event. Consumers dedupe on eventId.
 */
@Component
public class OrderEventPublisher {

    private final KafkaTemplate<String, OrderCompletedEvent> kafka;
    private final String topic;

    public OrderEventPublisher(KafkaTemplate<String, OrderCompletedEvent> kafka,
                               @Value("${app.kafka.topics.order-completed}") String topic) {
        this.kafka = kafka;
        this.topic = topic;
    }

    public void publish(OrderCompletedEvent event) {
        // Key = sagaId keeps all events of one saga ordered on one partition.
        ProducerRecord<String, OrderCompletedEvent> record =
                new ProducerRecord<>(topic, event.sagaId(), event);
        record.headers().add("eventId", event.eventId().getBytes(StandardCharsets.UTF_8));

        try {
            kafka.send(record).get(20, TimeUnit.SECONDS); // < activity startToClose (30s)
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing " + event.eventId(), e);
        } catch (ExecutionException | TimeoutException e) {
            throw new IllegalStateException("Kafka publish failed for " + event.eventId(), e);
        }
    }
}
