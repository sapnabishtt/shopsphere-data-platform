import os
import psycopg2
import os
from dotenv import load_dotenv


PROJECT_PATH = "/mnt/c/Users/bisht/OneDrive/Documents/shopsphere-data-platform"


def load_fact_sales():
    sql_path = os.path.join(
        PROJECT_PATH,
        "sql",
        "facts",
        "load_fact_sales.sql",
    )

    with open(sql_path, "r") as file:
        sql = file.read()

    connection = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", "5432")),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )

    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)

        connection.commit()
        print("fact_sales loaded successfully.")

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


if __name__ == "__main__":
    load_fact_sales()
