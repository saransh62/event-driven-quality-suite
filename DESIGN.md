# Design notes

Why the test suite is built the way it is. For how to run it, see [README.md](README.md).

## 1. Testcontainers instead of mocks

The bugs that hurt event-driven services sit at the boundaries: serializer settings, the
listener's acknowledgement mode, which listener Kafka advertises, Flyway scripts that only fail
on real PostgreSQL, transactions that don't roll back the way you thought. An embedded broker,
an H2 database or a mocked `KafkaTemplate` replaces exactly those parts with something else.

So the integration tests run the real Spring context against **a real Kafka broker
(apache/kafka, KRaft) and real PostgreSQL 16** in throwaway containers:

- Same images as `docker-compose.yml`, so "works in tests" means "works in compose".
- The containers are **singletons** (`integration-tests/.../Containers.java`): started once per JVM and
  shared by every test class, so the whole module starts them only once.
- Test isolation comes from data rather than from restarting containers: every test uses fresh
  UUIDs, the notification test uses its own topic, and Kafka assertions filter by record key.

Mocks are still used where they make the test more focused:

| Where | What is faked | Why |
|---|---|---|
| `NotificationServiceIntegrationTest` | order-service (WireMock) | The test is about the consumer. The HTTP contract is covered by Pact, so a second real service would add start-up time and failure modes without adding coverage. |
| `OrderServiceProviderPactIT` | `KafkaTemplate` (`@MockitoBean`) | The contract covers HTTP only. Kafka publishing is covered by the integration test. |

## 2. Consumer-driven contracts (Pact)

notification-service calls `GET /orders/{id}`. Without a contract there are two bad options:

- Test against a stub the consumer team wrote. The stub drifts away from the real API, and the
  tests stay green while production breaks.
- Test against a deployed order-service. This is slow and shared, and it fails for reasons that have nothing to do with the code under test.

With Pact, **the consumer states exactly what it uses** (`OrderClientPactTest`): the request
path and the response fields it reads, with type matchers instead of exact values. That
produces `contract-tests/pacts/notification-service-order-service.json`. The **provider replays that
file against its real code** (`OrderServiceProviderPactIT`) in its own build.

- **Provider states** (`@State("order 3f1c… exists")`) let each interaction set up the data it
  needs. The state name is the agreement between the two sides.
- Only the fields the consumer reads are in the contract. order-service can add fields (`createdAt`
  is not in the contract) but it cannot rename or remove one the consumer reads.
- Spec choice: the pact is committed instead of being published to a broker. CI regenerates it and fails
  if it differs from the committed file (`git diff --exit-code contract-tests/pacts`), so the
  checked-in contract can't go stale.

## 3. Idempotency and the dead-letter topic

### Why idempotency is required
Kafka consumption here is **at-least-once**. The listener commits the offset *after*
processing (`ack-mode: record`). If the process dies between "row written" and "offset committed",
the record is delivered again after restart. Producers can also send duplicates on retry. So the
consumer has to treat "seen this event before" as a normal case.

### How it's implemented
- Every `OrderCreated` carries a unique `eventId`.
- `NotificationProcessor.process` runs in **one DB transaction**: it checks `existsByEventId`,
  inserts `RECEIVED`, enriches over HTTP, then marks the row `PROCESSED`.
- `UNIQUE(event_id)` is the backstop when two deliveries race past the check.
- The transaction matters. If enrichment fails, the row is rolled back. Without the rollback the retry would
  find a `RECEIVED` row, treat the record as a duplicate, and the notification would never be
  processed.

### How it's tested
- **Duplicate:** the test sends event E, then E again, then a marker event M with the **same key**.
  The same key means the same partition and strict ordering, so when M is processed the duplicate has
  already been consumed. The test then asserts exactly two rows (E, M) and **exactly two** HTTP
  calls to order-service. The duplicate never reached enrichment. The test doesn't sleep.
- **Resilience (e2e):** 30 events are built up as backlog while the consumer is stopped. After the
  restart, the test asserts exactly 30 notifications in total.

### Dead-letter topic
`DefaultErrorHandler` with `FixedBackOff(500 ms, 2 retries)` and a `DeadLetterPublishingRecoverer`
that publishes to `orders.created.DLT` on the same partition. Spring adds headers with the
original topic, partition, offset and exception. After that the offset is committed and the
partition moves on.

Tested cases:
- Malformed JSON goes to the DLT. A valid event sent *after* it on the same partition is still processed,
  so the poison pill doesn't block the partition.
- Missing `eventId` goes to the DLT with a readable exception message, and no row is stored.
- order-service returning 404: the test checks there are exactly 3 HTTP attempts (1 + 2 retries), then the record goes to the DLT, and no partial row
  remains.

Trade-off: malformed JSON can never succeed, so retrying it only wastes time. In
production you'd mark deserialization errors as not retryable
(`errorHandler.addNotRetryableExceptions(...)`). The spec asked for "DLT after retries", so every
failure is retried here.

## 4. Measuring consumer lag

`lag(partition) = log-end offset − committed offset of the consumer group`, summed over
the partitions of `orders.created`.

- In Java (`e2e-tests/.../ConsumerLag.java`): `Admin.listConsumerGroupOffsets(group)` for
  committed offsets and `Admin.listOffsets(OffsetSpec.latest())` for end offsets. A partition with
  no committed offset counts as fully lagging.
- In the load-test script (`load-tests/measure-drain.sh`): `kafka-consumer-groups.sh --describe`
  run inside the broker container. The script sums the `LAG` column and polls every second until it reaches 0. It records
  the starting lag and the drain time in `results/drain.json`.

Lag gives the tests something exact to assert when work is asynchronous:
- After the event-flow test, lag must return to **0**, which means nothing is stuck or skipped.
- In the resilience test, lag must be **exactly 30** while the consumer is down. That proves the
  events wait in Kafka rather than being dropped. After the restart it must return to 0.

Lag only counts records that are *committed*. A record that went to the DLT is committed too, so lag 0 means
"handled", not "succeeded". That's why the tests assert the business outcome (each order is PROCESSED once)
*and* lag.

## 5. Load thresholds

`load-tests/orders-load.js` uses an **open model** (`ramping-arrival-rate`): k6 starts a fixed
number of requests per second however fast the server answers. In a closed model (fixed
VUs), a slow server would quietly lower the load and make the results look better than they are.

| Threshold | Meaning | Why this number |
|---|---|---|
| `http_req_duration p(95) < 300 ms` | 95% of `POST /orders` calls (DB insert + synchronous Kafka publish with `acks=all`) finish within 300 ms | A reasonable SLO for a write that waits on two systems. p95 rather than the average, because the average hides the slow tail that users notice. |
| `http_req_failed rate < 1%` | Fewer than 1 in 100 requests fail (non-2xx/3xx or network error) | Normal error budget. Under it, a few failures from noise are tolerated. |
| `checks rate > 99%` | The response is 201 and contains an id | Catches "fast but wrong" responses that latency metrics miss. |

If any threshold fails, k6 exits non-zero, so CI goes red. The smoke profile (CI) peaks at 20
req/s for about 40 s. The load profile (local) holds 100 req/s. After a run, `measure-drain.sh` reports how far the
consumer fell behind and how long it took to catch up. Producer latency can look fine while the
consumer drowns, and only lag shows that.

## 6. Known limitations (deliberate, to keep the services minimal)

- **Dual write:** order-service writes to the DB and then publishes inside one `@Transactional` method, and it
  waits for the broker ack. If Kafka fails, the insert rolls back. But if the DB commit fails *after* a
  successful send, an event exists for an order that doesn't. The standard fix is a **transactional
  outbox** (write the event into an outbox table in the same transaction, then relay it with
  Debezium or a poller).
- Retries are blocking (in-memory). Spring Kafka's non-blocking `@RetryableTopic` would keep the
  partition moving while one record waits for its retry.
- One broker and replication factor 1. Fine for tests, not for production.
