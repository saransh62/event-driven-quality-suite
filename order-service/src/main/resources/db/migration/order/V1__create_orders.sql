CREATE TABLE orders (
    id             UUID PRIMARY KEY,
    customer_email VARCHAR(255)   NOT NULL,
    product        VARCHAR(255)   NOT NULL,
    quantity       INT            NOT NULL CHECK (quantity > 0),
    amount         NUMERIC(12, 2) NOT NULL CHECK (amount > 0),
    status         VARCHAR(32)    NOT NULL,
    created_at     TIMESTAMPTZ    NOT NULL
);

CREATE INDEX idx_orders_status ON orders (status);
