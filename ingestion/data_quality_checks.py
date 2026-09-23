import psycopg2
import os
from dotenv import load_dotenv


DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": int(os.getenv("DB_PORT", "5432"))
}


def run_check(cursor, check_name, query):
    cursor.execute(query)
    result = cursor.fetchone()[0]

    status = "PASS" if result == 0 else "FAIL"

    print(f"{check_name}: {result} -> {status}")

    return status


def main():

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    checks = [
        (
            "NULL customer keys",
            """
            SELECT COUNT(*)
            FROM fact_sales
            WHERE customer_key IS NULL;
            """
        ),
        (
            "NULL product keys",
            """
            SELECT COUNT(*)
            FROM fact_sales
            WHERE product_key IS NULL;
            """
        ),
        (
            "NULL date keys",
            """
            SELECT COUNT(*)
            FROM fact_sales
            WHERE date_key IS NULL;
            """
        ),
        (
            "Negative sales amounts",
            """
            SELECT COUNT(*)
            FROM fact_sales
            WHERE sales_amount < 0;
            """
        ),
        (
            "Invalid quantities",
            """
            SELECT COUNT(*)
            FROM fact_sales
            WHERE quantity <= 0;
            """
        ),
        (
            "Duplicate sales keys",
            """
            SELECT COUNT(*) - COUNT(DISTINCT sales_key)
            FROM fact_sales;
            """
        )
    ]

    failed_checks = 0

    for check_name, query in checks:

        status = run_check(
            cursor,
            check_name,
            query
        )

        if status == "FAIL":
            failed_checks += 1

    cursor.execute("SELECT COUNT(*) FROM fact_sales;")
    fact_count = cursor.fetchone()[0]

    print(f"\nFact sales row count: {fact_count}")

    cursor.execute("""
        SELECT COUNT(*)
        FROM fact_sales
        WHERE customer_key = 0;
    """)

    unknown_customers = cursor.fetchone()[0]

    print(
        f"Unknown customer records: "
        f"{unknown_customers} -> INFO"
    )

    cursor.close()
    conn.close()

    if failed_checks > 0:
        print(f"\nDATA QUALITY FAILED: {failed_checks} check(s) failed")
        raise Exception("Data quality checks failed")

    print("\nALL DATA QUALITY CHECKS PASSED")


if __name__ == "__main__":
    main()
