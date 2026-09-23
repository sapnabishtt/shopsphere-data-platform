import psycopg2
import os
from dotenv import load_dotenv


def main():
    connection = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", "5432")),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

    cursor = connection.cursor()

    print("Starting product SCD Type 2 process...")

    # 1. Expire current records where product attributes changed
    cursor.execute("""
        UPDATE dim_product d
        SET
            effective_to = CURRENT_TIMESTAMP,
            is_current = 'N'
        FROM silver_products s
        WHERE d.product_id = s.product_id
          AND d.is_current = 'Y'
          AND (
              d.product_name IS DISTINCT FROM s.product_name
              OR d.category IS DISTINCT FROM s.category
              OR d.supplier_id IS DISTINCT FROM s.supplier_id
              OR d.price IS DISTINCT FROM s.price
          );
    """)

    expired_count = cursor.rowcount
    print(f"Expired records: {expired_count}")

    # 2. Insert new products and new versions of changed products
    cursor.execute("""
        INSERT INTO dim_product (
            product_key,
            product_id,
            product_name,
            category,
            supplier_id,
            price,
            effective_from,
            effective_to,
            is_current
        )
        SELECT
            (SELECT COALESCE(MAX(product_key), 0)
             FROM dim_product)
            + ROW_NUMBER() OVER (ORDER BY s.product_id),
            s.product_id,
            s.product_name,
            s.category,
            s.supplier_id,
            s.price,
            CURRENT_TIMESTAMP,
            NULL,
            'Y'
        FROM silver_products s
        LEFT JOIN dim_product d
            ON s.product_id = d.product_id
           AND d.is_current = 'Y'
        WHERE d.product_id IS NULL;
    """)

    inserted_count = cursor.rowcount
    print(f"Inserted records: {inserted_count}")

    connection.commit()

    cursor.close()
    connection.close()

    print("Product SCD Type 2 process completed successfully.")


if __name__ == "__main__":
    main()
