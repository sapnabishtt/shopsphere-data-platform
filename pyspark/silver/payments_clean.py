from pathlib import Path
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("ShopSphere Payments Silver")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


BATCH_ID = sys.argv[1] if len(sys.argv) > 1 else "20260905_165442"

BRONZE_PATH = (
    Path("data/bronze")
    / f"batch_{BATCH_ID}"
    / "payments"
    / "payments.csv"
)


df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(str(BRONZE_PATH))
)


print("\n========== PAYMENTS BRONZE PROFILE ==========")

print("\nSchema:")
df.printSchema()

total_records = df.count()

duplicate_payment_count = (
    df.groupBy("payment_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

null_payment_id_count = (
    df.filter(col("payment_id").isNull())
    .count()
)

null_order_id_count = (
    df.filter(col("order_id").isNull())
    .count()
)

negative_payment_count = (
    df.filter(col("payment_amount") < 0)
    .count()
)


print(f"\nTotal records: {total_records}")
print(f"Duplicate payment IDs: {duplicate_payment_count}")
print(f"NULL payment IDs: {null_payment_id_count}")
print(f"NULL order IDs: {null_order_id_count}")
print(f"Negative payment amounts: {negative_payment_count}")


print("\nSample payments:")
df.show(5, truncate=False)

# ============================================================
# STEP 2: CHECK PAYMENT → ORDER RELATIONSHIP
# ============================================================

ORDERS_SILVER_PATH = (
    Path("data/silver")
    / f"batch_{BATCH_ID}"
    / "orders"
)

orders_df = (
    spark.read
    .parquet(str(ORDERS_SILVER_PATH))
    .select("order_id")
    .dropDuplicates()
)


orphan_payments = (
    df
    .join(
        orders_df,
        df.order_id == orders_df.order_id,
        "left_anti"
    )
)


print("\n========== FOREIGN KEY VALIDATION ==========")

print(
    f"Payments with missing orders: "
    f"{orphan_payments.count()}"
)
# ============================================================
# STEP 3: VALIDATE PAYMENTS
# ============================================================

# First remove duplicate payment records.
window_spec = (
    Window
    .partitionBy("payment_id")
    .orderBy(
        col("ingestion_timestamp").desc()
    )
)

deduplicated_payments = (
    df
    .withColumn(
        "row_number",
        row_number().over(window_spec)
    )
    .filter(col("row_number") == 1)
    .drop("row_number")
)


# Keep only payments that:
# 1. Have a valid order
# 2. Have a non-negative payment amount

valid_payments = (
    deduplicated_payments
    .join(
        orders_df,
        deduplicated_payments.order_id == orders_df.order_id,
        "inner"
    )
    .filter(
        col("payment_amount") >= 0
    )
    .select(deduplicated_payments["*"])
)


rejected_payments = (
    deduplicated_payments
    .join(
        orders_df,
        deduplicated_payments.order_id == orders_df.order_id,
        "left_anti"
    )
    .unionByName(
        deduplicated_payments.filter(
            col("payment_amount") < 0
        )
    )
)


print("\n========== PAYMENTS VALIDATION ==========")

print(f"Bronze records: {df.count()}")
print(f"After deduplication: {deduplicated_payments.count()}")
print(f"Valid payments: {valid_payments.count()}")
print(f"Rejected payments: {rejected_payments.count()}")

# ============================================================
# STEP 4: CLEAN PAYMENTS
# ============================================================

clean_payments = (
    valid_payments
    .withColumn(
        "payment_amount",
        col("payment_amount").cast("decimal(12, 2)")
    )
)


# ============================================================
# STEP 5: WRITE SILVER AND REJECTED DATA
# ============================================================

SILVER_PATH = (
    Path("data/silver")
    / f"batch_{BATCH_ID}"
    / "payments"
)

REJECTED_PATH = (
    Path("data/rejected")
    / f"batch_{BATCH_ID}"
    / "payments"
)


clean_payments.write \
    .mode("overwrite") \
    .parquet(str(SILVER_PATH))


rejected_payments.write \
    .mode("overwrite") \
    .parquet(str(REJECTED_PATH))


print("\n========== PAYMENTS SILVER WRITE ==========")

print(f"Silver records: {clean_payments.count()}")
print(f"Rejected records: {rejected_payments.count()}")

print(f"Silver path: {SILVER_PATH}")
print(f"Rejected path: {REJECTED_PATH}")

print("Payments Silver processing completed successfully!")


spark.stop()
