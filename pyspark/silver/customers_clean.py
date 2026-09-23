from pathlib import Path
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("ShopSphere Customer Silver")
    .master("local[*]")
    .config(
        "spark.hadoop.fs.file.impl",
        "org.apache.hadoop.fs.RawLocalFileSystem"
    )
    .getOrCreate()
)


spark.sparkContext.setLogLevel("WARN")


# --------------------------------------------------
# 2. Define paths
# --------------------------------------------------

BATCH_ID = sys.argv[1] if len(sys.argv) > 1 else "20260905_165442"

BRONZE_PATH = (
    Path("data/bronze")
    / f"batch_{BATCH_ID}"
    / "customers"
    / "customers.csv"
)

SILVER_PATH = (
    Path("data/silver")
    / f"batch_{BATCH_ID}"
    / "customers"
)

REJECTED_PATH = (
    Path("data/rejected")
    / f"batch_{BATCH_ID}"
    / "customers"
)


# --------------------------------------------------
# 3. Read Bronze customer data
# --------------------------------------------------

df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(str(BRONZE_PATH))
)


# --------------------------------------------------
# 4. Profile Bronze data
# --------------------------------------------------

print("\n===== CUSTOMER DATA PROFILE =====")

total_records = df.count()

print(f"Total records: {total_records}")


# Find duplicate customer IDs

duplicate_customers = (
    df.groupBy("customer_id")
    .count()
    .filter(col("count") > 1)
)

duplicate_count = duplicate_customers.count()

print(f"Duplicate customer IDs: {duplicate_count}")


# Find NULL emails

null_email_count = (
    df.filter(col("email").isNull())
    .count()
)

print(f"NULL emails: {null_email_count}")


# Find NULL customer IDs

null_customer_id_count = (
    df.filter(col("customer_id").isNull())
    .count()
)

print(f"NULL customer IDs: {null_customer_id_count}")


# --------------------------------------------------
# 5. Display data-quality issues
# --------------------------------------------------

print("\n===== DUPLICATE CUSTOMER IDs =====")

duplicate_customers.show(10)


print("\n===== NULL EMAIL RECORDS =====")

(
    df
    .filter(col("email").isNull())
    .show(10, truncate=False)
)


# --------------------------------------------------
# 6. Remove duplicate customers
# --------------------------------------------------

print("\n===== REMOVING DUPLICATES =====")


# For each customer, keep the latest record
# based on updated_at.

window_spec = (
    Window
    .partitionBy("customer_id")
    .orderBy(
        col("updated_at").desc(),
        col("ingestion_timestamp").desc()
    )
)


df_deduplicated = (
    df
    .withColumn(
        "row_number",
        row_number().over(window_spec)
    )
    .filter(col("row_number") == 1)
    .drop("row_number")
)


print(
    f"Records before deduplication: "
    f"{df.count()}"
)

print(
    f"Records after deduplication: "
    f"{df_deduplicated.count()}"
)


# --------------------------------------------------
# 7. Separate valid and rejected customers
# --------------------------------------------------

print("\n===== HANDLING NULL EMAILS =====")


# Valid customers
# Email is required for our Silver customer dataset.

valid_customers = (
    df_deduplicated
    .filter(col("email").isNotNull())
)


# Rejected customers
# These records have missing email addresses.

rejected_customers = (
    df_deduplicated
    .filter(col("email").isNull())
)


print(
    f"Valid customer records: "
    f"{valid_customers.count()}"
)

print(
    f"Rejected customer records: "
    f"{rejected_customers.count()}"
)


# --------------------------------------------------
# 8. Display final counts
# --------------------------------------------------

print("\n===== FINAL CUSTOMER COUNTS =====")

print(f"Bronze records: {df.count()}")
print(f"Deduplicated records: {df_deduplicated.count()}")
print(f"Valid Silver records: {valid_customers.count()}")
print(f"Rejected records: {rejected_customers.count()}")

# --------------------------------------------------
# 9. Write Silver and Rejected data
# --------------------------------------------------

print("\n===== WRITING SILVER DATA =====")

valid_customers.write \
    .mode("overwrite") \
    .parquet(str(SILVER_PATH))

print(f"Silver data written to: {SILVER_PATH}")


print("\n===== WRITING REJECTED DATA =====")

rejected_customers.write \
    .mode("overwrite") \
    .parquet(str(REJECTED_PATH))

print(f"Rejected data written to: {REJECTED_PATH}")

# --------------------------------------------------
# 10. Stop Spark
# --------------------------------------------------

spark.stop()
