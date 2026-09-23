from pathlib import Path
from datetime import datetime

import pandas as pd


RAW_DATA_DIR = Path("data/raw")
BRONZE_DATA_DIR = Path("data/bronze")


def ingest_file(file_name, batch_dir, batch_id):
    source_file = RAW_DATA_DIR / file_name

    table_name = Path(file_name).stem
    destination_dir = batch_dir / table_name
    destination_file = destination_dir / file_name

    if not source_file.exists():
        raise FileNotFoundError(
            f"Source file not found: {source_file}"
        )

    df = pd.read_csv(source_file)

    ingestion_timestamp = datetime.now()

    df["ingestion_timestamp"] = ingestion_timestamp
    df["source_file"] = file_name

    destination_dir.mkdir(parents=True, exist_ok=True)

    df.to_csv(destination_file, index=False)

    print(
        f"Ingested {file_name}: "
        f"{len(df)} rows → {destination_file}"
    )

    return {
        "batch_id": batch_id,
        "file_name": file_name,
        "row_count": len(df),
        "ingestion_timestamp": ingestion_timestamp,
        "status": "SUCCESS",
    }


def main(batch_id=None):

    # If batch_id is supplied by Airflow, use it.
    # Otherwise generate one for manual execution.
    if batch_id is None:
        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    batch_dir = BRONZE_DATA_DIR / f"batch_{batch_id}"
    batch_dir.mkdir(parents=True, exist_ok=True)

    files = [
        "customers.csv",
        "products.csv",
        "orders.csv",
        "order_items.csv",
        "payments.csv",
    ]

    manifest_records = []

    for file_name in files:
        record = ingest_file(
            file_name,
            batch_dir,
            batch_id
        )
        manifest_records.append(record)

    manifest_df = pd.DataFrame(manifest_records)

    manifest_file = batch_dir / "ingestion_manifest.csv"
    manifest_df.to_csv(manifest_file, index=False)

    print("\nIngestion completed successfully!")
    print(f"Batch ID: {batch_id}")
    print(f"Manifest: {manifest_file}")


if __name__ == "__main__":
    main()
