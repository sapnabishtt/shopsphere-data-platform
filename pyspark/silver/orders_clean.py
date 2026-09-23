from pathlib import Path
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


spark = (
    SparkSession.builder
    .appName("ShopSphere Orders Silver")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


BATCH_ID = sys.argv[1] if len(sys.argv) > 1 else "20260905_165442"

BRONZE_PATH = (
    Path("data/bronze")
    / f"batch_{BATCH_ID}"
    / "orders"
    / "orders.csv"
)


df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(str(BRONZE_PATH))
)


print("\n========== ORDERS BRONZE PROFILE ==========")

print("\nSchema:")
df.printSchema()

total_records = df.count()

duplicate_orders = (
    df.groupBy("order_id")
    .count()
    .filter(col("count") > 1)
)

duplicate_order_count = duplicate_orders.count()

null_customer_count = (
    df.filter(col("customer_id").isNull())
    .count()
)

negative_amount_count = (
    df.filter(col("total_amount") < 0)
    .count()
)

orphan_customer_count = (
    df.filter(col("customer_id") == 99999999)
    .count()
)


print(f"\nTotal records: {total_records}")
print(f"Duplicate order IDs: {duplicate_order_count}")
print(f"NULL customer IDs: {null_customer_count}")
print(f"Negative order amounts: {negative_amount_count}")
print(f"Orphan customer IDs: {orphan_customer_count}")


print("\nSample problematic records:")

print("\nDuplicate orders:")
duplicate_orders.show(5)

print("\nNULL customer IDs:")
df.filter(col("customer_id").isNull()).show(5)

print("\nNegative amounts:")
df.filter(col("total_amount") < 0).show(5)

print("\nOrphan customers:")
df.filter(col("customer_id") == 99999999).show(5)

# ============================================================
# STEP 2: DEDUPLICATE ORDERS
# ============================================================

window_spec = (
    Window
    .partitionBy("order_id")
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

print("\n========== AFTER DEDUPLICATION ==========")

print(f"Records before deduplication: {df.count()}")
print(f"Records after deduplication: {df_deduplicated.count()}")

# ============================================================
# STEP 3: VALIDATE ORDERS
# ============================================================

valid_orders = df_deduplicated.filter(
    col("customer_id").isNotNull()
    & (col("total_amount") >= 0)
    & (col("customer_id") != 99999999)
)

rejected_orders = df_deduplicated.filter(
    col("customer_id").isNull()
    | (col("total_amount") < 0)
    | (col("customer_id") == 99999999)
)

print("\n========== AFTER VALIDATION ==========")

print(f"Unique orders: {df_deduplicated.count()}")
print(f"Valid orders: {valid_orders.count()}")
print(f"Rejected orders: {rejected_orders.count()}")

# ============================================================
# STEP 4: WRITE SILVER AND REJECTED DATA
# ============================================================

SILVER_PATH = (
    Path("data/silver")
    / f"batch_{BATCH_ID}"
    / "orders"
)

REJECTED_PATH = (
    Path("data/rejected")
    / f"batch_{BATCH_ID}"
    / "orders"
)


# Convert customer_id from double to integer
valid_orders = valid_orders.withColumn(
    "customer_id",
    col("customer_id").cast("integer")
)


# Write valid orders to Silver
valid_orders.write \
    .mode("overwrite") \
    .parquet(str(SILVER_PATH))


# Write rejected orders to quarantine
rejected_orders.write \
    .mode("overwrite") \
    .parquet(str(REJECTED_PATH))


print("\n========== ORDERS SILVER WRITE ==========")
print(f"Silver path: {SILVER_PATH}")
print(f"Rejected path: {REJECTED_PATH}")
print("Orders Silver processing completed successfully!")
spark.stop()
