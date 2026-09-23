CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key INTEGER,
    customer_id INTEGER,
    customer_name VARCHAR(200),
    email VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(100),
    effective_from TIMESTAMP,
    effective_to TIMESTAMP,
    is_current CHAR(1)
);

-- INSERT INTO dim_customer (
--     customer_key,
--     customer_id,
--     customer_name,
--     email,
--     city,
--     state,
--     effective_from,
--     effective_to,
--     is_current
-- )
-- SELECT
--     ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_key,
--     customer_id,
--     customer_name,
--     email,
--     city,
--     state,
--     CURRENT_TIMESTAMP AS effective_from,
--     NULL AS effective_to,
--     'Y' AS is_current
-- FROM silver_customers;

CREATE TABLE IF NOT EXISTS silver_customers (
    customer_id INTEGER,
    customer_name VARCHAR(200),
    email VARCHAR(200),
    city VARCHAR(100),
    state VARCHAR(100),
    signup_date DATE,
    updated_at TIMESTAMP,
    ingestion_timestamp TIMESTAMP,
    source_file VARCHAR(200)
);

INSERT INTO dim_customer (
    customer_key,
    customer_id,
    customer_name,
    email,
    city,
    state,
    effective_from,
    effective_to,
    is_current
)
SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id) AS customer_key,
    customer_id,
    customer_name,
    email,
    city,
    state,
    CURRENT_TIMESTAMP AS effective_from,
    NULL AS effective_to,
    'Y' AS is_current
FROM silver_customers;