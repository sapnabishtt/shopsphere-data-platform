from pathlib import Path
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = (
    SparkSession.builder
    .appName("ShopSphere Products Silver")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


BATCH_ID = sys.argv[1] if len(sys.argv) > 1 else "20260905_165442"

BRONZE_PATH = (
    Path("data/bronze")
    / f"batch_{BATCH_ID}"
    / "products"
    / "products.csv"
)


df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(str(BRONZE_PATH))
)


print("\n========== PRODUCTS BRONZE PROFILE ==========")

print("\nSchema:")
df.printSchema()

total_records = df.count()

duplicate_products = (
    df.groupBy("product_id")
    .count()
    .filter(col("count") > 1)
)

duplicate_product_count = duplicate_products.count()

null_product_id_count = (
    df.filter(col("product_id").isNull())
    .count()
)

null_product_name_count = (
    df.filter(col("product_name").isNull())
    .count()
)

null_category_count = (
    df.filter(col("category").isNull())
    .count()
)

negative_price_count = (
    df.filter(col("price") < 0)
    .count()
)


print(f"\nTotal records: {total_records}")
print(f"Duplicate product IDs: {duplicate_product_count}")
print(f"NULL product IDs: {null_product_id_count}")
print(f"NULL product names: {null_product_name_count}")
print(f"NULL categories: {null_category_count}")
print(f"Negative prices: {negative_price_count}")


print("\nSample products:")
df.show(5, truncate=False)


# ============================================================
# STEP 2: CLEAN PRODUCTS
# ============================================================

clean_products = (
    df
    .dropDuplicates(["product_id"])
    .withColumn(
        "product_id",
        col("product_id").cast("integer")
    )
    .withColumn(
        "price",
        col("price").cast("decimal(12, 2)")
    )
)

print("\n========== PRODUCTS SILVER ==========")

print(f"Bronze records: {df.count()}")
print(f"Silver records: {clean_products.count()}")

clean_products.printSchema()


# ============================================================
# STEP 3: WRITE PRODUCTS SILVER
# ============================================================

SILVER_PATH = (
    Path("data/silver")
    / f"batch_{BATCH_ID}"
    / "products"
)

clean_products.write \
    .mode("overwrite") \
    .parquet(str(SILVER_PATH))


print("\nProducts Silver processing completed successfully!")
print(f"Silver path: {SILVER_PATH}")


spark.stop()
