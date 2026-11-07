package com.eventsuite.notification;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class NotificationServiceApplication {

    /** Config lives in application-notification.yml so both services can share one test classpath. */
    public static final String PROFILE = "notification";

    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(NotificationServiceApplication.class);
        app.setAdditionalProfiles(PROFILE);
        app.run(args);
    }
}
