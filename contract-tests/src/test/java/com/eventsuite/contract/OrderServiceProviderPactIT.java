package com.eventsuite.contract;

import au.com.dius.pact.provider.junit5.HttpTestTarget;
import au.com.dius.pact.provider.junit5.PactVerificationContext;
import au.com.dius.pact.provider.junitsupport.Provider;
import au.com.dius.pact.provider.junitsupport.State;
import au.com.dius.pact.provider.junitsupport.loader.PactFolder;
import au.com.dius.pact.provider.spring.junit5.PactVerificationSpringProvider;
import com.eventsuite.order.Order;
import com.eventsuite.order.OrderRepository;
import com.eventsuite.order.OrderServiceApplication;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.math.BigDecimal;

import static com.eventsuite.contract.PactFixtures.EXISTING_ORDER_ID;
import static com.eventsuite.contract.PactFixtures.PROVIDER;
import static com.eventsuite.contract.PactFixtures.STATE_ORDER_EXISTS;
import static com.eventsuite.contract.PactFixtures.STATE_ORDER_MISSING;

/**
 * Provider side: boots the real order-service (real PostgreSQL, Kafka publishing mocked out because
 * the contract is HTTP-only) and replays every interaction in pacts/ against it.
 * {@code @State} methods put the provider into the state each interaction was recorded under.
 */
@Provider(PROVIDER)
@PactFolder("pacts")
@Testcontainers
@SpringBootTest(classes = OrderServiceApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "spring.kafka.admin.auto-create=false")
@ActiveProfiles(OrderServiceApplication.PROFILE)
class OrderServiceProviderPactIT {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> db = new PostgreSQLContainer<>("postgres:16-alpine");

    @MockitoBean
    KafkaTemplate<String, String> kafkaTemplate;

    @LocalServerPort
    int port;

    @Autowired
    OrderRepository repository;

    @BeforeEach
    void target(PactVerificationContext context) {
        context.setTarget(new HttpTestTarget("localhost", port));
    }

    @TestTemplate
    @ExtendWith(PactVerificationSpringProvider.class)
    void verifyPact(PactVerificationContext context) {
        context.verifyInteraction();
    }

    @State(STATE_ORDER_EXISTS)
    void orderExists() {
        repository.save(new Order(EXISTING_ORDER_ID, "provider-state@example.com", "Monitor", 1,
                new BigDecimal("199.00")));
    }

    @State(STATE_ORDER_MISSING)
    void orderMissing() {
        repository.deleteAll();
    }
}
