CREATE TABLE IF NOT EXISTS dim_product (
    product_key INTEGER,
    product_id INTEGER,
    product_name VARCHAR(200),
    category VARCHAR(100),
    supplier_id VARCHAR(50),
    price DECIMAL(12,2),
    effective_from TIMESTAMP,
    effective_to TIMESTAMP,
    is_current CHAR(1)
);

CREATE TABLE IF NOT EXISTS silver_products (
    product_id INTEGER,
    product_name VARCHAR(200),
    category VARCHAR(100),
    price DECIMAL(12,2),
    supplier_id VARCHAR(50),
    updated_at TIMESTAMP,
    ingestion_timestamp TIMESTAMP,
    source_file VARCHAR(200)
);
