# src/aggregation/gold_revenue.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, lit, current_date

def aggregate_mrr_reporting(spark: SparkSession, storage_account_name: str):
    """
    Aggregates active subscriptions from the Silver layer to calculate 
    Monthly Recurring Revenue (MRR) for the Gold reporting layer.
    """
    silver_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/user_profiles/"
    gold_path = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/mrr_reporting/"

    print("Generating Gold MRR aggregations...")

    # 1. Read ONLY the currently active users from the Silver layer
    df_silver = spark.read.format("delta").load(silver_path).filter(col("is_active") == True)

    # 2. Aggregate counts by subscription tier
    df_aggregated = df_silver.groupBy("subscription_tier").agg(
        count("user_id").alias("total_active_users")
    )

    # 3. Apply business logic (Assigning dollar values to tiers)
    df_gold = df_aggregated.withColumn(
        "tier_price",
        when(col("subscription_tier") == "Basic", lit(9.99))
        .when(col("subscription_tier") == "Premium", lit(19.99))
        .when(col("subscription_tier") == "Ultra", lit(29.99))
        .otherwise(lit(0.00))
    ).withColumn(
        "total_mrr", 
        col("total_active_users") * col("tier_price")
    ).withColumn(
        "report_date", 
        current_date()
    )

    # 4. Write to Gold Layer and Optimize
    (df_gold.write
        .format("delta")
        .mode("overwrite") # Overwrite is fine here; Gold is a daily snapshot for BI tools
        .save(gold_path)
    )

    # 5. Optimize the Delta Table for fast BI tool querying
    print("Optimizing Gold table for BI consumption...")
    spark.sql(f"OPTIMIZE delta.`{gold_path}` ZORDER BY (subscription_tier)")
    
    print("Gold layer aggregation complete.")