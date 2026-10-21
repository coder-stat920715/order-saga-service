package com.example.saga.client;

import java.net.http.HttpClient;
import java.time.Duration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;

@Configuration
public class DownstreamClientConfig {

    @Bean
    RestClient inventoryRestClient(@Value("${app.inventory.base-url}") String baseUrl) {
        return build(baseUrl);
    }

    @Bean
    RestClient paymentRestClient(@Value("${app.payment.base-url}") String baseUrl) {
        return build(baseUrl);
    }

    private static RestClient build(String baseUrl) {
        HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
        JdkClientHttpRequestFactory factory = new JdkClientHttpRequestFactory(http);
        factory.setReadTimeout(Duration.ofSeconds(10)); // < activity startToClose (30s)
        return RestClient.builder().baseUrl(baseUrl).requestFactory(factory).build();
    }
}
