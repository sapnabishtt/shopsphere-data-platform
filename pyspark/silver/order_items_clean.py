from pathlib import Path
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = (
    SparkSession.builder
    .appName("ShopSphere Order Items Silver")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


BATCH_ID = sys.argv[1] if len(sys.argv) > 1 else "20260905_165442"

BRONZE_PATH = (
    Path("data/bronze")
    / f"batch_{BATCH_ID}"
    / "order_items"
    / "order_items.csv"
)


df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(str(BRONZE_PATH))
)


print("\n========== ORDER ITEMS BRONZE PROFILE ==========")

print("\nSchema:")
df.printSchema()

total_records = df.count()

duplicate_item_count = (
    df.groupBy("order_item_id")
    .count()
    .filter(col("count") > 1)
    .count()
)

null_order_id_count = (
    df.filter(col("order_id").isNull())
    .count()
)

null_product_id_count = (
    df.filter(col("product_id").isNull())
    .count()
)

invalid_quantity_count = (
    df.filter(col("quantity") <= 0)
    .count()
)

invalid_price_count = (
    df.filter(col("unit_price") < 0)
    .count()
)


print(f"\nTotal records: {total_records}")
print(f"Duplicate order item IDs: {duplicate_item_count}")
print(f"NULL order IDs: {null_order_id_count}")
print(f"NULL product IDs: {null_product_id_count}")
print(f"Invalid quantities: {invalid_quantity_count}")
print(f"Negative unit prices: {invalid_price_count}")


print("\nSample order items:")
df.show(5, truncate=False)

# ============================================================
# STEP 2: CHECK FOREIGN KEY RELATIONSHIPS
# ============================================================

ORDERS_SILVER_PATH = (
    Path("data/silver")
    / f"batch_{BATCH_ID}"
    / "orders"
)

PRODUCTS_SILVER_PATH = (
    Path("data/silver")
    / f"batch_{BATCH_ID}"
    / "products"
)


orders_df = (
    spark.read
    .parquet(str(ORDERS_SILVER_PATH))
    .select("order_id")
    .dropDuplicates()
)

products_df = (
    spark.read
    .parquet(str(PRODUCTS_SILVER_PATH))
    .select("product_id")
    .dropDuplicates()
)


# Find order items whose order_id doesn't exist
orphan_order_items = (
    df
    .join(
        orders_df,
        df.order_id == orders_df.order_id,
        "left_anti"
    )
)


# Find order items whose product_id doesn't exist
orphan_product_items = (
    df
    .join(
        products_df,
        df.product_id == products_df.product_id,
        "left_anti"
    )
)


print("\n========== FOREIGN KEY VALIDATION ==========")

print(
    f"Order items with missing orders: "
    f"{orphan_order_items.count()}"
)

print(
    f"Order items with missing products: "
    f"{orphan_product_items.count()}"
)
# ============================================================
# STEP 3: VALIDATE ORDER ITEMS
# ============================================================

valid_order_items = (
    df
    .join(
        orders_df,
        df.order_id == orders_df.order_id,
        "inner"
    )
    .join(
        products_df,
        df.product_id == products_df.product_id,
        "inner"
    )
    .select(df["*"])
)


rejected_order_items = (
    df
    .join(
        orders_df,
        df.order_id == orders_df.order_id,
        "left_anti"
    )
)


print("\n========== ORDER ITEMS VALIDATION ==========")

print(f"Bronze records: {df.count()}")
print(f"Valid order items: {valid_order_items.count()}")
print(f"Rejected order items: {rejected_order_items.count()}")
# ============================================================
# STEP 4: CLEAN ORDER ITEMS
# ============================================================

clean_order_items = (
    valid_order_items
    .withColumn(
        "unit_price",
        col("unit_price").cast("decimal(12, 2)")
    )
)


# ============================================================
# STEP 5: WRITE SILVER AND REJECTED DATA
# ============================================================

SILVER_PATH = (
    Path("data/silver")
    / f"batch_{BATCH_ID}"
    / "order_items"
)

REJECTED_PATH = (
    Path("data/rejected")
    / f"batch_{BATCH_ID}"
    / "order_items"
)


clean_order_items.write \
    .mode("overwrite") \
    .parquet(str(SILVER_PATH))


rejected_order_items.write \
    .mode("overwrite") \
    .parquet(str(REJECTED_PATH))


print("\n========== ORDER ITEMS SILVER WRITE ==========")

print(f"Silver records: {clean_order_items.count()}")
print(f"Rejected records: {rejected_order_items.count()}")

print(f"Silver path: {SILVER_PATH}")
print(f"Rejected path: {REJECTED_PATH}")

print("Order Items Silver processing completed successfully!")


spark.stop()
