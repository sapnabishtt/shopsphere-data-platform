CREATE TABLE IF NOT EXISTS fact_sales (
    sales_key BIGINT PRIMARY KEY,
    order_id INTEGER,
    customer_key INTEGER,
    product_key INTEGER,
    date_key INTEGER,
    quantity INTEGER,
    unit_price DECIMAL(12,2),
    discount DECIMAL(12,2),
    sales_amount DECIMAL(14,2)
);
