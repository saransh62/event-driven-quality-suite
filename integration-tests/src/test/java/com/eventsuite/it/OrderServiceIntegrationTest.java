package com.eventsuite.it;

import com.eventsuite.order.Order;
import com.eventsuite.order.OrderRepository;
import com.eventsuite.order.OrderServiceApplication;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.util.List;
import java.util.UUID;

import static io.restassured.RestAssured.given;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

@SpringBootTest(classes = OrderServiceApplication.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles(OrderServiceApplication.PROFILE)
class OrderServiceIntegrationTest {

    private static final String TOPIC = "orders.created";

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry r) {
        r.add("spring.datasource.url", Containers.ORDER_DB::getJdbcUrl);
        r.add("spring.datasource.username", Containers.ORDER_DB::getUsername);
        r.add("spring.datasource.password", Containers.ORDER_DB::getPassword);
        r.add("spring.kafka.bootstrap-servers", Containers.KAFKA::getBootstrapServers);
    }

    @LocalServerPort
    int port;

    @Autowired
    OrderRepository repository;

    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        RestAssured.port = port;
    }

    @Test
    void createOrder_persistsRow_andPublishesExactlyOneEventKeyedByOrderId() throws Exception {
        String id = given().contentType(ContentType.JSON)
                .body("""
                        {"customerEmail":"ada@example.com","product":"Keyboard","quantity":2,"amount":49.90}""")
                .when().post("/orders")
                .then().statusCode(201)
                .header("Location", containsString("/orders/"))
                .body("status", equalTo("CREATED"))
                .extract().path("id");
        UUID orderId = UUID.fromString(id);

        Order saved = repository.findById(orderId).orElseThrow();
        assertThat(saved.getCustomerEmail()).isEqualTo("ada@example.com");
        assertThat(saved.getQuantity()).isEqualTo(2);
        assertThat(saved.getAmount()).isEqualByComparingTo("49.90");

        // The send is awaited before the 201 is returned, so the record is already on the topic.
        List<ConsumerRecord<String, String>> mine = TestKafkaConsumer
                .readAll(Containers.KAFKA.getBootstrapServers(), TOPIC).stream()
                .filter(r -> id.equals(r.key()))
                .toList();
        assertThat(mine).as("records keyed by the new order id").hasSize(1);

        JsonNode event = mapper.readTree(mine.get(0).value());
        assertThat(event.get("orderId").asText()).isEqualTo(id);
        assertThat(UUID.fromString(event.get("eventId").asText())).isNotNull();
        assertThat(event.get("customerEmail").asText()).isEqualTo("ada@example.com");
        assertThat(event.get("product").asText()).isEqualTo("Keyboard");
        assertThat(event.get("quantity").asInt()).isEqualTo(2);
        assertThat(event.get("amount").decimalValue()).isEqualByComparingTo("49.90");
    }

    @Test
    void getById_returnsOrder_and404WhenUnknown() {
        String id = createOrder("grace@example.com");

        given().when().get("/orders/{id}", id)
                .then().statusCode(200)
                .body("id", equalTo(id))
                .body("customerEmail", equalTo("grace@example.com"));

        given().when().get("/orders/{id}", UUID.randomUUID()).then().statusCode(404);
    }

    @Test
    void listByStatus_includesCreatedOrders() {
        String id = createOrder("linus@example.com");

        given().queryParam("status", "CREATED")
                .when().get("/orders")
                .then().statusCode(200)
                .body("id", hasItem(id));
    }

    @Test
    void invalidPayload_returns400WithFieldErrors_andNothingIsPersistedOrPublished() {
        long rowsBefore = repository.count();
        int eventsBefore = TestKafkaConsumer.readAll(Containers.KAFKA.getBootstrapServers(), TOPIC).size();

        given().contentType(ContentType.JSON)
                .body("""
                        {"customerEmail":"not-an-email","product":"","quantity":0}""")
                .when().post("/orders")
                .then().statusCode(400)
                .body("status", equalTo(400))
                .body("error", equalTo("Bad Request"))
                .body("message", equalTo("Validation failed"))
                .body("fieldErrors", hasSize(4))
                .body("fieldErrors.field", equalTo(List.of("amount", "customerEmail", "product", "quantity")));

        assertThat(repository.count()).isEqualTo(rowsBefore);
        assertThat(TestKafkaConsumer.readAll(Containers.KAFKA.getBootstrapServers(), TOPIC)).hasSize(eventsBefore);
    }

    @Test
    void malformedJson_returns400WithClearMessage() {
        given().contentType(ContentType.JSON)
                .body("{\"customerEmail\": ")
                .when().post("/orders")
                .then().statusCode(400)
                .body("message", equalTo("Malformed JSON request body"))
                .body("fieldErrors", hasSize(0));
    }

    @Test
    void unknownStatusFilter_returns400() {
        given().queryParam("status", "SHIPPED_TO_MARS")
                .when().get("/orders")
                .then().statusCode(400)
                .body("message", equalTo("Invalid value for parameter 'status'"));
    }

    private String createOrder(String email) {
        return given().contentType(ContentType.JSON)
                .body("{\"customerEmail\":\"" + email + "\",\"product\":\"Mouse\",\"quantity\":1,\"amount\":19.99}")
                .when().post("/orders")
                .then().statusCode(201)
                .extract().path("id");
    }
}
