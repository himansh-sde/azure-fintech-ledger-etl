# 1. Authenticate first!
from src.config.spark_config import configure_spark_abfs
storage_account = configure_spark_abfs(spark)

# src/processing/silver_scd2.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from delta.tables import DeltaTable

def process_silver_scd2(spark: SparkSession, storage_account_name: str):
    bronze_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/delta_table/"
    silver_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/user_profiles/"

    # 1. Read the new data from Bronze
    df_updates = spark.read.format("delta").load(bronze_path)

    # 2. The "EAFP" Pattern: Try to read Silver. If it fails, it's our first run!
    try:
        # If this succeeds, the table exists and is ready
        df_silver_active = spark.read.format("delta").load(silver_path).filter(col("is_active") == True)
        silver_table = DeltaTable.forPath(spark, silver_path)
        is_first_run = False
    except Exception as e:
        # If it throws an error (like PATH_NOT_FOUND), the table isn't there yet
        is_first_run = True

    # 3. Initial Load Logic
    if is_first_run:
        print("Initializing Silver table for the first time...")
        df_updates.withColumn("is_active", lit(True)) \
                  .withColumn("end_date", lit(None).cast("date")) \
                  .write.format("delta").mode("overwrite").save(silver_path)
        print("Silver initialization complete!")
        return

    # 4. SCD Type 2 Merge Logic (Only runs for Batch 2 and beyond)
    staged_updates = df_updates.join(df_silver_active, "user_id") \
        .filter(df_updates.subscription_tier != df_silver_active.subscription_tier) \
        .select(df_updates["*"]) \
        .withColumn("mergeKey", lit(None)) 

    df_staged_updates = df_updates.withColumn("mergeKey", col("user_id")).unionByName(staged_updates, allowMissingColumns=True)

    print("Merging updates into Silver layer...")
    silver_table.alias("target").merge(
        df_staged_updates.alias("updates"),
        "target.user_id = updates.mergeKey"
    ).whenMatchedUpdate(
        condition="target.is_active = true AND target.subscription_tier <> updates.subscription_tier",
        set={
            "is_active": lit(False),
            "end_date": "updates.transaction_date"
        }
    ).whenNotMatchedInsert(
        values={
            "user_id": "updates.user_id",
            "subscription_tier": "updates.subscription_tier",
            "transaction_date": "updates.transaction_date",
            "end_date": lit(None).cast("date"),
            "is_active": lit(True)
        }
    ).execute()
    print("Silver merge complete.")


    # --- EXECUTION BLOCK ---
if __name__ == "__main__":
    from src.config.spark_config import configure_spark_abfs
    # 1. Authenticate and get storage account name
    storage_account = configure_spark_abfs(spark)
    # 2. Run the processing
    process_silver_scd2(spark, storage_account)