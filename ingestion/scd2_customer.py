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

    print("Starting customer SCD Type 2 process...")

    # 1. Expire current records where customer attributes changed
    cursor.execute("""
        UPDATE dim_customer d
        SET
            effective_to = CURRENT_TIMESTAMP,
            is_current = 'N'
        FROM silver_customers s
        WHERE d.customer_id = s.customer_id
          AND d.is_current = 'Y'
          AND (
              d.customer_name IS DISTINCT FROM s.customer_name
              OR d.email IS DISTINCT FROM s.email
              OR d.city IS DISTINCT FROM s.city
              OR d.state IS DISTINCT FROM s.state
          );
    """)

    expired_count = cursor.rowcount
    print(f"Expired records: {expired_count}")

    # 2. Insert new customers and new versions of changed customers
    cursor.execute("""
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
            (SELECT COALESCE(MAX(customer_key), 0)
             FROM dim_customer)
            + ROW_NUMBER() OVER (ORDER BY s.customer_id),

            s.customer_id,
            s.customer_name,
            s.email,
            s.city,
            s.state,
            CURRENT_TIMESTAMP,
            NULL,
            'Y'
        FROM silver_customers s
        LEFT JOIN dim_customer d
            ON s.customer_id = d.customer_id
           AND d.is_current = 'Y'
        WHERE d.customer_id IS NULL;
    """)

    inserted_count = cursor.rowcount
    print(f"Inserted records: {inserted_count}")

    connection.commit()

    cursor.close()
    connection.close()

    print("Customer SCD Type 2 process completed successfully.")


if __name__ == "__main__":
    main()
