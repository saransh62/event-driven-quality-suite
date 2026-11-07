# Learning guide

A walkthrough of the code in reading order, followed by interview questions with short answers.

## Part 1: code walkthrough (read in this order)

### Step 1: the system in one picture
1. `docker-compose.yml`: Kafka (KRaft), PostgreSQL with two databases (`docker/postgres-init.sql`),
   both services, and an optional Kafka UI (`--profile ui`). Note the two Kafka listeners:
   `kafka:9092` for containers and `localhost:9094` for the host.
2. `pom.xml` (root): the parent POM, the module list, and the Pact version. Spring Boot's parent manages
   every other version.

### Step 2: the producer (order-service)
3. `OrderServiceApplication`: turns on the `order` profile, so config comes from
   `application-order.yml`. That lets both services share a test classpath without their configs colliding.
4. `OrderDtos`: the request (with Bean Validation annotations), the response, and `OrderCreatedEvent`
   (with `eventId`, which the consumer uses for idempotency).
5. `OrderController`, then `OrderService.create`: save the row, then `kafkaTemplate.send(topic, key=orderId, json).get()`
   inside `@Transactional`.
6. `ApiErrorHandler`: the 400 body format `{status, error, message, fieldErrors[]}`.
7. `KafkaTopicsConfig` and `V1__create_orders.sql`: the topic (3 partitions) and the schema.

### Step 3: the consumer (notification-service)
8. `OrderCreatedListener`: `@KafkaListener` parses the JSON and rejects events without ids.
9. `NotificationProcessor.process`: the core logic. Duplicate check, insert, HTTP enrichment and
   PROCESSED status, all in one transaction.
10. `OrderClient`: the `RestClient` call to order-service. This is the Pact consumer.
11. `KafkaConsumerConfig`: `DefaultErrorHandler` with 2 retries, then `orders.created.DLT`.
12. `V1__create_notifications.sql`: `UNIQUE(event_id)` is the idempotency backstop.
13. `NotificationController`: a read-only endpoint the e2e tests use to see the result.

### Step 4: integration tests (`integration-tests`)
14. `Containers`: singleton Kafka and PostgreSQL containers shared by every test class.
15. `TestKafkaConsumer.readAll`: reads a topic up to its current end offsets. This gives an exact
    record count with no sleeping.
16. `OrderServiceIntegrationTest`: the row is persisted, exactly one event has the right key and payload, and invalid input
    returns 400 with nothing persisted or published.
17. `NotificationServiceIntegrationTest`: happy path, the duplicate+marker trick, three ways to
    end up on the DLT, and proof that a poison pill doesn't block the partition.

### Step 5: contract tests (`contract-tests`)
18. `PactFixtures`: ids and provider-state names shared by both sides.
19. `OrderClientPactTest` (consumer): runs the real `OrderClient` against Pact's mock server and writes
    `pacts/notification-service-order-service.json`.
20. `OrderServiceProviderPactIT` (provider): runs the real order-service, sets up provider states, and replays
    the pact. It runs under failsafe so it executes after the consumer test has regenerated the file.

### Step 6: end-to-end (`e2e-tests`, run with `-Pe2e` against compose)
21. `ConsumerLag`: computes lag with AdminClient (end offsets minus committed offsets).
22. `EventFlowE2ETest`: 25 orders, each notified exactly once, then lag returns to 0.
23. `ResilienceE2ETest`: stop the consumer, create 30 orders, check lag == 30, restart, check lag
    returns to 0 with no duplicates.
24. `DockerCompose`: uses `docker compose` or `docker-compose`, whichever is installed.

### Step 7: load and CI
25. `load-tests/orders-load.js`: an open-model ramp with thresholds and a JSON summary from `handleSummary`.
26. `load-tests/measure-drain.sh`: lag right after the load run and the time it takes to drain.
27. `.github/workflows/ci.yml`: job 1 runs `mvn verify` plus the stale-pact check. Job 2 runs compose,
    the e2e tests, the k6 smoke run and the drain check, then uploads the artifacts.

## Part 2: 15 likely interview questions

**1. What is a Kafka consumer group?**
A set of consumers that share the work of reading a topic, identified by `group.id`. Each
partition is assigned to exactly one consumer in the group, so the partition count caps
parallelism. Different groups read the same topic independently, and each group keeps its own
offsets.

**2. What is an offset, and where is it stored?**
An offset is the position of a record within a partition. The group's *committed* offset is the
next record it will read. It's stored in Kafka's internal `__consumer_offsets` topic, so it
survives consumer restarts. The resilience test depends on this.

**3. What is consumer lag and how do you measure it?**
Log-end offset minus committed offset, summed over partitions. It is the number of records written but not
yet processed. You can measure it with `Admin.listConsumerGroupOffsets` plus `Admin.listOffsets(latest)`, or with
`kafka-consumer-groups.sh --describe`. Growing lag means the consumer is slower than the producer.

**4. At-most-once, at-least-once, exactly-once: which one does this project use?**
At-least-once. The offset is committed *after* processing (`ack-mode: record`), so a crash between
processing and commit causes a redelivery. At-most-once commits first and can lose
records. Exactly-once in Kafka covers Kafka-to-Kafka transactions only. With a database side effect, you get the same
result through at-least-once delivery plus an idempotent consumer.

**5. How did you make the consumer idempotent?**
Each event has a unique `eventId`. The consumer checks it before processing, and `UNIQUE(event_id)`
in the database catches races. Insert, enrichment and status update run in one transaction, so a failed
attempt leaves nothing behind for the retry to mistake for a duplicate.

**6. Why is the message key the orderId?**
The key decides the partition, and ordering is only guaranteed within a partition. Keying by
orderId keeps all events for one order in order. The duplicate test relies on this: a marker event
with the same key is guaranteed to be consumed after the duplicate.

**7. What is a dead-letter topic and why do you need one?**
It's where records go after processing keeps failing. Without one, a single bad record (a poison pill)
is retried forever and blocks its partition. Here, `DefaultErrorHandler` retries twice, then
`DeadLetterPublishingRecoverer` publishes the record to `orders.created.DLT` with the exception in its headers. Then
the consumer moves on.

**8. How do you test something asynchronous without flaky sleeps?**
Poll for the expected outcome with a timeout. `Thread.sleep(5000)` is too slow when the system is fast
and too short when CI is slow. Awaitility's `await().atMost(30s).until(condition)` returns as soon as
the condition holds and fails with a clear message when it times out. Also assert on deterministic
facts: lag == 0, "exactly one row", and reading a topic up to fixed end offsets.

**9. How do you prove a message was *not* processed twice, or *not* sent?**
You can't wait forever for something not to happen, so you create a point in time after which it
would already have happened. For duplicates, send a marker on the same partition and wait for the marker.
For "not published", read the topic up to its end offsets and compare the count before and after.

**10. Why Testcontainers rather than mocks or embedded Kafka?**
Bugs live in the real integrations: serialization, listener configuration, SQL dialect, transactions.
Testcontainers runs the same images as production or compose, in throwaway containers on random ports.
Mocks are still useful for isolating a dependency that's tested elsewhere (WireMock for order-service,
since Pact covers that contract).

**11. What is consumer-driven contract testing?**
The consumer records the requests it makes and the response fields it relies on, as a pact. The provider
verifies that pact against its real implementation in its own build. A breaking change, such as renaming
`customerEmail`, fails the *provider's* build before deploy, with no shared test environment.

**12. What is a provider state?**
A named precondition attached to an interaction, for example "order 3f1c… exists". Before replaying that
interaction, the provider test runs the matching `@State` method to create the data. The name is the
agreement between the consumer and provider teams.

**13. Contract tests vs. integration tests vs. e2e tests: why have all three?**
Integration tests cover one service against its real infrastructure, and they're fast and precise. Contract
tests check that two services agree on an API without running both. E2e tests show the whole system works
together, including lag and recovery, but they're slower and harder to debug. That's why there are few
of them and they cover only the critical flows.

**14. What do your k6 thresholds mean, and why use an open model?**
p95 < 300 ms means 95% of requests are faster than 300 ms. Errors < 1% and checks > 99% complete the
picture. The thresholds fail the run, and CI goes red. `ramping-arrival-rate` sends N requests per second
whatever the response time, so a slow server shows up as high latency instead of quietly lowering the load
(the "coordinated omission" problem with closed models).

**15. What would you change for production?**
Use a transactional outbox to close the DB-to-Kafka dual-write gap. Make deserialization errors
non-retryable. Use non-blocking retries (`@RetryableTopic`). Run 3 brokers with `min.insync.replicas=2`.
Publish pacts to a Pact Broker and gate deploys with `can-i-deploy`. Alert on consumer lag, and
add DLT monitoring with a replay tool.
