package com.example.saga.fraud;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.ratelimiter.annotation.RateLimiter;
import io.github.resilience4j.retry.annotation.Retry;
import io.github.resilience4j.timelimiter.annotation.TimeLimiter;
import jakarta.annotation.PreDestroy;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Qualifier;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

/**
 * Guarded synchronous integration with the Fraud Detection Service.
 *
 * Only call this from a Temporal Activity. @TimeLimiter requires a CompletableFuture
 * return type, so callers join the future (see Step 5 activity).
 *
 * The fallback is declared on @Retry only (the outermost aspect), so it fires once,
 * after retries are exhausted or the circuit is open.
 */
@Component
public class FraudCheckClient {

    private static final Logger log = LoggerFactory.getLogger(FraudCheckClient.class);
    private static final String INSTANCE = "fraudCheck";

    private final RestClient restClient;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public FraudCheckClient(@Qualifier("fraudRestClient") RestClient fraudRestClient) {
        this.restClient = fraudRestClient;
    }

    @Retry(name = INSTANCE, fallbackMethod = "fallback")
    @CircuitBreaker(name = INSTANCE)
    @RateLimiter(name = INSTANCE)
    @TimeLimiter(name = INSTANCE)
    public CompletableFuture<FraudCheckResponse> checkAsync(FraudCheckRequest request) {
        return CompletableFuture.supplyAsync(() -> doCall(request), executor);
    }

    private FraudCheckResponse doCall(FraudCheckRequest request) {
        log.debug("Fraud check sagaId={}", request.sagaId());
        return restClient.post()
                .uri("/api/v1/fraud/check")
                // Lets the fraud service dedupe if a retry lands after a slow success
                .header("Idempotency-Key", request.sagaId())
                .body(request)
                .retrieve()
                .body(FraudCheckResponse.class);
    }

    /**
     * Fail closed: never auto-approve when the fraud service is unreachable.
     * 4xx client errors are not infrastructure failures, so they pass through unchanged.
     */
    @SuppressWarnings("unused")
    private CompletableFuture<FraudCheckResponse> fallback(FraudCheckRequest request, Throwable t) {
        Throwable cause = (t instanceof CompletionException && t.getCause() != null) ? t.getCause() : t;

        if (cause instanceof HttpClientErrorException) {
            return CompletableFuture.failedFuture(cause);
        }
        log.warn("Fraud check unavailable sagaId={} cause={}", request.sagaId(), cause.toString());
        return CompletableFuture.failedFuture(new FraudServiceUnavailableException(
                "Fraud service unavailable for saga " + request.sagaId(), cause));
    }

    @PreDestroy
    void shutdown() {
        executor.close();
    }
}
