-- ============================================================
-- ShopSphere Data Quality Checks
-- ============================================================

-- 1. Fact row count
SELECT
    'fact_sales_row_count' AS check_name,
    COUNT(*) AS actual_value,
    CASE
        WHEN COUNT(*) > 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM fact_sales;


-- 2. NULL customer keys
SELECT
    'fact_null_customer_key' AS check_name,
    COUNT(*) AS actual_value,
    CASE
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM fact_sales
WHERE customer_key IS NULL;


-- 3. NULL product keys
SELECT
    'fact_null_product_key' AS check_name,
    COUNT(*) AS actual_value,
    CASE
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM fact_sales
WHERE product_key IS NULL;


-- 4. NULL date keys
SELECT
    'fact_null_date_key' AS check_name,
    COUNT(*) AS actual_value,
    CASE
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM fact_sales
WHERE date_key IS NULL;


-- 5. Negative sales amounts
SELECT
    'negative_sales_amount' AS check_name,
    COUNT(*) AS actual_value,
    CASE
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM fact_sales
WHERE sales_amount < 0;


-- 6. Invalid quantities
SELECT
    'invalid_quantity' AS check_name,
    COUNT(*) AS actual_value,
    CASE
        WHEN COUNT(*) = 0 THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM fact_sales
WHERE quantity <= 0;


-- 7. Unknown customer records
SELECT
    'unknown_customer_records' AS check_name,
    COUNT(*) AS actual_value,
    'INFO' AS status
FROM fact_sales
WHERE customer_key = 0;


-- 8. Duplicate sales keys
SELECT
    'duplicate_sales_keys' AS check_name,
    COUNT(*) - COUNT(DISTINCT sales_key) AS actual_value,
    CASE
        WHEN COUNT(*) = COUNT(DISTINCT sales_key) THEN 'PASS'
        ELSE 'FAIL'
    END AS status
FROM fact_sales;