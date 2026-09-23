CREATE TABLE IF NOT EXISTS silver_orders (
    order_id INTEGER,
    customer_id INTEGER,
    order_date DATE,
    order_status VARCHAR(50),
    total_amount DECIMAL(14,2),
    updated_at TIMESTAMP,
    ingestion_timestamp TIMESTAMP,
    source_file VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS silver_order_items (
    order_item_id INTEGER,
    order_id INTEGER,
    product_id INTEGER,
    quantity INTEGER,
    unit_price DECIMAL(12,2),
    ingestion_timestamp TIMESTAMP,
    source_file VARCHAR(200)
);
