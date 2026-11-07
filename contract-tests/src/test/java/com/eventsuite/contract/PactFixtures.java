package com.eventsuite.contract;

import java.util.UUID;

/** Shared between consumer and provider so the provider state and the pact agree on ids. */
final class PactFixtures {

    static final String CONSUMER = "notification-service";
    static final String PROVIDER = "order-service";

    private static final String EXISTING_ID = "3f1c2b7e-8a4d-4e5f-9b6a-1c2d3e4f5a6b";
    private static final String MISSING_ID = "00000000-0000-4000-8000-000000000404";

    static final UUID EXISTING_ORDER_ID = UUID.fromString(EXISTING_ID);
    static final UUID MISSING_ORDER_ID = UUID.fromString(MISSING_ID);

    // compile-time constants: used in @State annotations
    static final String STATE_ORDER_EXISTS = "order " + EXISTING_ID + " exists";
    static final String STATE_ORDER_MISSING = "no order " + MISSING_ID + " exists";

    private PactFixtures() {
    }
}
