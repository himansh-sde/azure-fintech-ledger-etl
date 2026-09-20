# src/processing/silver_scd2.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from delta.tables import DeltaTable

def process_silver_scd2(spark: SparkSession, storage_account_name: str):
    """
    Merges new transactions into the Silver Delta table, maintaining historical 
    records using SCD Type 2 logic.
    """
    bronze_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/delta_table/"
    silver_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/user_profiles/"

    # 1. Read the new data from Bronze
    df_updates = spark.read.format("delta").load(bronze_path)

    # If Silver table doesn't exist yet, do an initial load and exit
    if not DeltaTable.isDeltaTable(spark, silver_path):
        print("Initializing Silver table for the first time...")
        df_updates.withColumn("is_active", lit(True)) \
                  .withColumn("end_date", lit(None).cast("date")) \
                  .write.format("delta").save(silver_path)
        return

    # 2. Identify records that exist in both Silver and updates WHERE the profile changed.
    # These are the records we need to EXPIRE.
    silver_table = DeltaTable.forPath(spark, silver_path)
    df_silver_active = spark.read.format("delta").load(silver_path).filter(col("is_active") == True)

    staged_updates = df_updates.join(df_silver_active, "user_id") \
        .filter(df_updates.subscription_tier != df_silver_active.subscription_tier) \
        .select(df_updates["*"]) \
        .withColumn("mergeKey", lit(None)) # Null key forces an INSERT of the new row later

    # 3. Union the "to-be-expired" records with the actual updates
    df_staged_updates = df_updates.withColumn("mergeKey", col("user_id")).unionByName(staged_updates, allowMissingColumns=True)

    # 4. Execute the Delta Merge (SCD Type 2)
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