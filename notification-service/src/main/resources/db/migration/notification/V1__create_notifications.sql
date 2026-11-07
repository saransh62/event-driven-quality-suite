CREATE TABLE notifications (
    id           BIGSERIAL PRIMARY KEY,
    event_id     UUID         NOT NULL,
    order_id     UUID         NOT NULL,
    recipient    VARCHAR(255),
    message      VARCHAR(1000),
    status       VARCHAR(32)  NOT NULL,
    received_at  TIMESTAMPTZ  NOT NULL,
    processed_at TIMESTAMPTZ,
    CONSTRAINT uq_notifications_event_id UNIQUE (event_id)
);

CREATE INDEX idx_notifications_order_id ON notifications (order_id);
