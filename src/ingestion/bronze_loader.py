# src/ingestion/bronze_loader.py
from pyspark.sql import SparkSession

def ingest_raw_transactions(spark: SparkSession, storage_account_name: str):

    """Ingests raw JSON transaction files incrementally into the Bronze layer using Databricks Auto Loader. """
    # Define our ADLS Gen2 paths
    landing_zone_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/raw_json/"
    bronze_table_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/delta_table/"
    
    # Auto Loader requires dedicated paths to store schema and read state checkpoints
    schema_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/_metadata/schema/"
    checkpoint_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/_metadata/checkpoint/"

    print(f"Starting Auto Loader stream from {landing_zone_path}...")

    # 1. Read Stream using Auto Loader (cloudFiles)
    df_raw = (spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.inferColumnTypes", "true") # Automatically infers if a column is Int, String, etc.
        .load(landing_zone_path)
    )

    # 2. Write Stream to Delta Lake (Bronze Table)
    query = (df_raw.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .trigger(availableNow=True) # Processes all pending files, then shuts down to save costs
        .outputMode("append")
        .start(bronze_table_path)
    )
    
    query.awaitTermination()
    print("Bronze ingestion complete. Pipeline idempotent and ready for next batch.")

# If running directly in a Databricks notebook context for testing:
#storage_account = "stfintechdatalake"
#ingest_raw_transactions(spark, storage_account)


# --- EXECUTION BLOCK ---
if __name__ == "__main__":
    from src.config.spark_config import configure_spark_abfs
    # 1. Authenticate and get storage account name
    storage_account = configure_spark_abfs(spark)
    # 2. Run the ingestion
    ingest_raw_transactions(spark, storage_account)