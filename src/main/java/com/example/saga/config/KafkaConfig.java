package com.example.saga.config;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;

@Configuration
public class KafkaConfig {

    /** Local defaults. In production: replicas=3 and min.insync.replicas=2. */
    @Bean
    NewTopic orderCompletedTopic(@Value("${app.kafka.topics.order-completed}") String name) {
        return TopicBuilder.name(name).partitions(3).replicas(1).build();
    }

    /**
     * Retry with backoff, then park the record on "<topic>.DLT" so one poison message
     * never blocks the partition. Spring Boot wires this CommonErrorHandler bean into
     * the listener container factory automatically.
     */
    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaTemplate<String, Object> jsonTemplate,
                                          KafkaProperties props) {
        // Deserialization failures carry raw bytes, so the DLT needs a byte[] template too.
        Map<String, Object> cfg = props.buildProducerProperties(null);
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        KafkaTemplate<String, byte[]> rawTemplate =
                new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(cfg));

        Map<Class<?>, KafkaOperations<?, ?>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, rawTemplate);
        templates.put(Object.class, jsonTemplate);

        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(5);
        backOff.setInitialInterval(1_000);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(15_000);

        return new DefaultErrorHandler(new DeadLetterPublishingRecoverer(templates), backOff);
    }
}
