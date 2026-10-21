# order-saga-service

Orchestrated Saga (Temporal) + Kafka + Resilience4j. Java 21, Spring Boot 3.3.

## Run
```
cd infra && docker compose up -d      # Temporal, Kafka (KRaft), Postgres, WireMock mocks
cd .. && mvn spring-boot:run
```
Temporal UI: http://localhost:8233

## Try it
```
curl -X POST localhost:8080/orders -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: demo-1' \
  -d '{"customerId":"decline-user","items":[{"sku":"ABC","quantity":1}],"amount":49.90,"currency":"USD"}'
curl localhost:8080/orders/demo-1/status
```
Mock triggers: customerId `fraud-user` (rejected), `decline-user` (payment 402 -> compensation),
`slow-user` (timeout), `down-user` (503, opens circuit); sku `OUT-OF-STOCK` (409).

## Test
`mvn test`  (workflow tests use Temporal's in-memory test server)

## Notes
- No outbox, no CDC: Temporal history is the durable state.
- Not compiled by the author: run `mvn -q compile` first and fix any version drift.
- `temporalio/auto-setup` is for local development only.
