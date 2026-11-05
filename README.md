# Notification Platform

[![CI](https://github.com/ltlkay/NotificationPlatform/actions/workflows/ci.yml/badge.svg)](https://github.com/ltlkay/NotificationPlatform/actions/workflows/ci.yml)

An event-driven notification service. Clients register subscriptions (an event type plus a delivery channel and target), publish events through a REST API, and a Kafka consumer fans each event out to every matching subscription. Every delivery attempt is recorded in PostgreSQL, and failed webhook deliveries are sent to a dead-letter topic.

This is a solo portfolio project. Email delivery is simulated (a configurable random failure rate); the focus is the Kafka pipeline, retry and dead-letter handling, and unit testing around them.

## How it works

```
Client --POST /events--> EventController --> EventProducer --> Kafka: notification-events
                                                                      |
                                                            NotificationConsumer
                                                                      |
                                       look up subscriptions for the event type (PostgreSQL)
                                                                      |
                                          for each subscription, by channel:
                                          EMAIL   -> EmailChannel   (simulated, 1 attempt)
                                          WEBHOOK -> WebhookChannel (HTTP POST, retries with backoff)
                                                                      |
                                           save a DeliveryLog row (SUCCESS / FAILED)
                                                                      |
                                    failed WEBHOOK delivery -> Kafka: notification-events-dlq
```

Behavior worth knowing:

- **Webhook retries:** up to 5 attempts with exponential backoff (1s initial delay, x2 multiplier). Connection errors and 5xx responses are retried; 4xx responses fail immediately.
- **Dead-letter topic:** only failed webhook deliveries are dead-lettered. Failed email deliveries are logged but not dead-lettered.
- **Isolation:** an exception from one channel is recorded as a failed delivery and does not stop the remaining subscriptions for that event.
- **Persistence failures:** if saving a delivery log fails, the exception propagates so Kafka redelivers the record. This can cause duplicate deliveries; idempotent delivery is a tracked follow-up.

## API

| Method | Path | Purpose |
|---|---|---|
| POST | `/events` | Publish an event (returns `202` with the event id) |
| POST | `/subscriptions` | Create a subscription (returns `201`) |
| GET | `/subscriptions` | List subscriptions, optionally filtered with `?eventType=` |

`POST /events` body:

```json
{ "eventType": "ORDER_SHIPPED", "payload": { "orderId": "123" } }
```

`POST /subscriptions` body (`channel` is `EMAIL` or `WEBHOOK`):

```json
{ "eventType": "ORDER_SHIPPED", "channel": "WEBHOOK", "target": "https://example.com/hook" }
```

`eventType` and `target` must not be blank; `channel` is required.

## Run it

Requires Docker and Docker Compose.

```bash
cp dev.env.example .env      # then set DB_PASSWORD in .env
docker compose up --build
```

This starts PostgreSQL, Kafka (KRaft mode) and the app. The API is then available at http://localhost:8081.

```bash
curl -X POST http://localhost:8081/subscriptions \
  -H "Content-Type: application/json" \
  -d '{"eventType": "ORDER_SHIPPED", "channel": "EMAIL", "target": "user@example.com"}'

curl -X POST http://localhost:8081/events \
  -H "Content-Type: application/json" \
  -d '{"eventType": "ORDER_SHIPPED", "payload": {"orderId": "123"}}'
```

More request examples are in `scripts/http/`.

## Configuration

Set in `src/main/resources/application.yml`:

| Property | Default | Purpose |
|---|---|---|
| `notification.email.failure-rate` | `0.2` | Probability that a simulated email send fails |
| `notification.webhook.max-attempts` | `5` | Webhook attempts before giving up |
| `notification.webhook.initial-delay-ms` | `1000` | Delay before the first retry |
| `notification.webhook.multiplier` | `2` | Backoff multiplier between retries |
| `spring.kafka.template.default-topic` | `notification-events` | Event topic |
| `spring.kafka.template.dlq-topic` | `notification-events-dlq` | Dead-letter topic |

The database password comes from `DB_PASSWORD`, read from the environment or from a local `.env` file (which is gitignored).

## Tests

```bash
./gradlew test
```

The unit tests use Mockito and AssertJ with no Kafka broker. They cover `NotificationConsumer` (routing, channel failures and exceptions, dead-letter callbacks, save failures), `EventProducer` and `DeliveryResult`.

The `contextLoads` test starts the full Spring context and needs a reachable PostgreSQL database named `np_db_local`, with the password in `DB_PASSWORD`. For example, start the compose database with `docker compose up db` and set `DB_PASSWORD` in `.env`.

Planned: integration tests with Testcontainers against a real Kafka broker.

## CI

GitHub Actions runs on every push and pull request to `master`:

- **Test:** `./gradlew test` on JDK 26 against a PostgreSQL 18 service container. The test report is uploaded as an artifact if the run fails.
- **Docker build:** validates the compose file and builds the image.

## Stack

Java 26, Spring Boot 4, Spring for Apache Kafka, Spring Data JPA, PostgreSQL, Lombok, JUnit 5, Mockito, AssertJ, Gradle, Docker Compose.
