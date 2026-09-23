import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
from pathlib import Path
import sys


PROJECT_PATH = Path(
    "/mnt/c/Users/bisht/OneDrive/Documents/shopsphere-data-platform"
)

load_dotenv(PROJECT_PATH / ".env")


def main(batch_id=None):

    if batch_id is None:
        batch_id = "20260905_165442"

    silver_path = (
        PROJECT_PATH
        / "data"
        / "silver"
        / f"batch_{batch_id}"
        / "order_items"
    )

    print(f"Batch ID: {batch_id}")
    print("Reading Silver order item data...")
    print(f"Path: {silver_path}")

    df = pd.read_parquet(silver_path)

    print(f"Rows read from Silver: {len(df)}")

    connection = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=int(os.getenv("DB_PORT", "5432")),
        database=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )

    cursor = connection.cursor()
    cursor.execute("TRUNCATE TABLE silver_order_items;")

    insert_query = """
        INSERT INTO silver_order_items (
            order_item_id,
            order_id,
            product_id,
            quantity,
            unit_price,
            ingestion_timestamp,
            source_file
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """

    for row in df.itertuples(index=False, name=None):
        cursor.execute(insert_query, row)

    connection.commit()

    print(
        f"Loaded {len(df)} order items "
        f"into silver_order_items"
    )

    cursor.close()
    connection.close()


if __name__ == "__main__":
    batch_id = sys.argv[1] if len(sys.argv) > 1 else None
    main(batch_id)
