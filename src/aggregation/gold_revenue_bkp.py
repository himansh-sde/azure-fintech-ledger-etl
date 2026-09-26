# src/aggregation/gold_revenue.py
from pyspark.sql.functions import col, count, when, lit, current_date, round
import importlib
import src.config.spark_config
importlib.reload(src.config.spark_config)
from src.config.spark_config import configure_spark_abfs

# 1. FORCE AUTHENTICATION FIRST (Don't hide it inside a function)
storage_account = configure_spark_abfs(spark)

# 2. DEFINE PATHS
silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/user_profiles/"
gold_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/mrr_reporting/"

print("1. Reading active users from Silver layer...")
# We only care about CURRENT subscriptions for MRR
df_silver = spark.read.format("delta").load(silver_path).filter(col("is_active") == True)

print("2. Calculating MRR aggregations...")
df_aggregated = df_silver.groupBy("subscription_tier").agg(
    count("user_id").alias("total_active_users")
)

# Assign prices and calculate total revenue (with rounding!)
df_gold = df_aggregated.withColumn(
    "tier_price",
    when(col("subscription_tier") == "Basic", lit(9.99))
    .when(col("subscription_tier") == "Premium", lit(19.99))
    .when(col("subscription_tier") == "Ultra", lit(29.99))
    .otherwise(lit(0.00))
).withColumn(
    "total_mrr", 
    round(col("total_active_users") * col("tier_price"), 2)
).withColumn(
    "report_date", 
    current_date()
)

print("3. Writing to Gold layer...")
(df_gold.write
    .format("delta")
    .mode("overwrite") 
    .save(gold_path)
)

print("4. Optimizing Gold table for BI consumption...")
# Z-Ordering physically sorts the data on disk to make BI queries lightning fast
spark.sql(f"OPTIMIZE delta.`{gold_path}` ZORDER BY (subscription_tier)")
    
print("5. Gold aggregation successfully complete!")
