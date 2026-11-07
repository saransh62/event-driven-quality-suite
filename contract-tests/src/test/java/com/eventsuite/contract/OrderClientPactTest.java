package com.eventsuite.contract;

import au.com.dius.pact.consumer.MockServer;
import au.com.dius.pact.consumer.dsl.PactDslJsonBody;
import au.com.dius.pact.consumer.dsl.PactDslWithProvider;
import au.com.dius.pact.consumer.junit5.PactConsumerTestExt;
import au.com.dius.pact.consumer.junit5.PactTestFor;
import au.com.dius.pact.core.model.V4Pact;
import au.com.dius.pact.core.model.annotations.Pact;
import com.eventsuite.notification.OrderClient;
import com.eventsuite.notification.OrderClient.OrderView;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestClient;

import static com.eventsuite.contract.PactFixtures.CONSUMER;
import static com.eventsuite.contract.PactFixtures.EXISTING_ORDER_ID;
import static com.eventsuite.contract.PactFixtures.MISSING_ORDER_ID;
import static com.eventsuite.contract.PactFixtures.PROVIDER;
import static com.eventsuite.contract.PactFixtures.STATE_ORDER_EXISTS;
import static com.eventsuite.contract.PactFixtures.STATE_ORDER_MISSING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Consumer side: runs the real {@link OrderClient} against Pact's mock server. The expectations
 * declared here become the contract (pacts/notification-service-order-service.json). Matchers
 * pin types, not example values, so the provider is free to return any valid data.
 */
@ExtendWith(PactConsumerTestExt.class)
@PactTestFor(providerName = PROVIDER)
class OrderClientPactTest {

    @Pact(consumer = CONSUMER)
    V4Pact existingOrder(PactDslWithProvider builder) {
        PactDslJsonBody body = new PactDslJsonBody()
                .uuid("id", EXISTING_ORDER_ID)
                .stringType("customerEmail", "ada@example.com")
                .stringType("product", "Keyboard")
                .integerType("quantity", 2)
                .numberType("amount", 49.90)
                .stringMatcher("status", "CREATED|CANCELLED", "CREATED");
        return builder
                .given(STATE_ORDER_EXISTS)
                .uponReceiving("a request for an existing order")
                .path("/orders/" + EXISTING_ORDER_ID)
                .method("GET")
                .willRespondWith()
                .status(200)
                .matchHeader("Content-Type", "application/json.*", "application/json")
                .body(body)
                .toPact(V4Pact.class);
    }

    @Pact(consumer = CONSUMER)
    V4Pact missingOrder(PactDslWithProvider builder) {
        return builder
                .given(STATE_ORDER_MISSING)
                .uponReceiving("a request for an order that does not exist")
                .path("/orders/" + MISSING_ORDER_ID)
                .method("GET")
                .willRespondWith()
                .status(404)
                .toPact(V4Pact.class);
    }

    @Test
    @PactTestFor(pactMethod = "existingOrder")
    void getOrder_mapsTheFieldsNotificationServiceNeeds(MockServer server) {
        OrderView order = client(server).getOrder(EXISTING_ORDER_ID);

        assertThat(order.id()).isEqualTo(EXISTING_ORDER_ID);
        assertThat(order.customerEmail()).isEqualTo("ada@example.com");
        assertThat(order.product()).isEqualTo("Keyboard");
        assertThat(order.quantity()).isEqualTo(2);
        assertThat(order.amount()).isEqualByComparingTo("49.90");
    }

    @Test
    @PactTestFor(pactMethod = "missingOrder")
    void getOrder_unknownId_surfacesA404(MockServer server) {
        assertThatThrownBy(() -> client(server).getOrder(MISSING_ORDER_ID))
                .isInstanceOf(HttpClientErrorException.NotFound.class);
    }

    private static OrderClient client(MockServer server) {
        return new OrderClient(RestClient.builder(), server.getUrl());
    }
}
