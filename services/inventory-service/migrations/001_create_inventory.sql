
CREATE TABLE IF NOT EXISTS inventory_stock (
    product_id VARCHAR(100) PRIMARY KEY,
    quantity INTEGER NOT NULL CHECK (quantity >= 0),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
