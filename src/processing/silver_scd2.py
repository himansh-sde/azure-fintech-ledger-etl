# src/processing/silver_scd2.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, row_number, desc
from pyspark.sql.window import Window
from delta.tables import DeltaTable

def process_silver_scd2(spark: SparkSession, storage_account_name: str):
    bronze_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/delta_table/"
    silver_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/user_profiles/"

    print("1. Reading updates from Bronze...")
    df_raw_updates = spark.read.format("delta").load(bronze_path)

    # -------------------------------------------------------------------------
    # INTERVIEW GOLD: Source Deduplication
    # Delta MERGE fails if the source has multiple rows for the same target row.
    # We use a Window function to isolate the latest transaction per user.
    # -------------------------------------------------------------------------
    print("1.5 Deduplicating source data to prevent merge conflicts...")
    window_spec = Window.partitionBy("user_id").orderBy(desc("transaction_date"))
    
    df_updates = df_raw_updates.withColumn("rn", row_number().over(window_spec)) \
                               .filter(col("rn") == 1) \
                               .drop("rn")

    # --- Proceed with the EAGER EVALUATION try/except block below exactly as before ---
    try:
        spark.read.format("delta").load(silver_path).limit(1).count()
        table_exists = True
    except Exception:
        table_exists = False

    # --- INITIAL LOAD ---
    if not table_exists:
        print("2. First run detected. Initializing Silver table...")
        df_updates.withColumn("is_active", lit(True)) \
                  .withColumn("end_date", lit(None).cast("date")) \
                  .write.format("delta").mode("overwrite").save(silver_path)
        print("3. Silver initialization complete! Pipeline ready for next batch.")
        return

    # --- SCD TYPE 2 MERGE ---
    print("2. Silver table exists. Staging updates for SCD Type 2 Merge...")
    silver_table = DeltaTable.forPath(spark, silver_path)
    df_silver_active = spark.read.format("delta").load(silver_path).filter(col("is_active") == True)
    
    # Identify which records actually changed (e.g., Basic -> Premium)
    staged_updates = df_updates.join(df_silver_active, "user_id") \
        .filter(df_updates.subscription_tier != df_silver_active.subscription_tier) \
        .select(df_updates["*"]) \
        .withColumn("mergeKey", lit(None)) 

    # Union the updates to process both INSERTS (new tiers) and UPDATES (retiring old tiers)
    df_staged_updates = df_updates.withColumn("mergeKey", col("user_id")) \
                                  .unionByName(staged_updates, allowMissingColumns=True)

    print("3. Executing Delta Merge...")
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
    
    print("4. Silver merge successfully complete!")


# --- EXECUTION BLOCK ---
if __name__ == "__main__":
    import importlib
    import src.config.spark_config
    importlib.reload(src.config.spark_config) # Prevents caching issues
    from src.config.spark_config import configure_spark_abfs
    
    # 1. Authenticate via Key Vault
    storage_account = configure_spark_abfs(spark)
    
    # 2. Run Pipeline
    process_silver_scd2(spark, storage_account)