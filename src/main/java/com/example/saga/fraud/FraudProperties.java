package com.example.saga.fraud;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.fraud")
public record FraudProperties(
        String baseUrl,
        Duration connectTimeout,
        Duration readTimeout) {
}
