INSERT INTO fact_sales (
    sales_key,
    order_item_id,
    order_id,
    customer_key,
    product_key,
    date_key,
    quantity,
    unit_price,
    discount,
    sales_amount
)
SELECT
    COALESCE(
        (SELECT MAX(sales_key) FROM fact_sales),
        0
    ) + ROW_NUMBER() OVER (ORDER BY oi.order_item_id) AS sales_key,

    oi.order_item_id,
    oi.order_id,
    COALESCE(dc.customer_key, 0) AS customer_key,
    dp.product_key,
    dd.date_key,
    oi.quantity,
    oi.unit_price,
    0.00 AS discount,
    oi.quantity * oi.unit_price AS sales_amount

FROM silver_order_items oi

JOIN silver_orders o
    ON oi.order_id = o.order_id

LEFT JOIN dim_customer dc
    ON o.customer_id = dc.customer_id
   AND dc.is_current = 'Y'

JOIN dim_product dp
    ON oi.product_id = dp.product_id
   AND dp.is_current = 'Y'

JOIN dim_date dd
    ON o.order_date = dd.full_date

WHERE NOT EXISTS (
    SELECT 1
    FROM fact_sales f
    WHERE f.order_item_id = oi.order_item_id
);
