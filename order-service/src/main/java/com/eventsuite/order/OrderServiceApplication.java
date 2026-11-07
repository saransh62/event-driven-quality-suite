package com.eventsuite.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class OrderServiceApplication {

    /** Config lives in application-order.yml so both services can share one test classpath. */
    public static final String PROFILE = "order";

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(OrderServiceApplication.class);
        app.setAdditionalProfiles(PROFILE);
        app.run(args);
    }
}
