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

    print("Connected to ShopSphere PostgreSQL database")

    connection.close()


if __name__ == "__main__":
    main()
