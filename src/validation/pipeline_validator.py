from pyspark.sql.functions import col
from src.config.spark_config import configure_spark_abfs

# 1. FORCE AUTHENTICATION FIRST
storage_account = configure_spark_abfs(spark)

# 2. DEFINE PATHS
bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/delta_table/"
silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/user_profiles/"
gold_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/mrr_reporting/"

# 3. RUN VALIDATION REPORT
print("="*60)
print("🚀 FINTECH DATA PIPELINE: VALIDATION REPORT 🚀")
print("="*60)

# --- VALIDATE BRONZE ---
print("\n🟢 BRONZE LAYER (Raw JSON Ingestion)")
try:
    df_bronze = spark.read.format("delta").load(bronze_path)
    print(f"   -> Status: SUCCESS")
    print(f"   -> Total Raw Records Ingested: {df_bronze.count()}")
except Exception as e:
    print(f"   -> Status: FAILED (Table not found)")

# --- VALIDATE SILVER ---
print("\n⚪ SILVER LAYER (SCD Type 2 Profiles)")
try:
    df_silver = spark.read.format("delta").load(silver_path)
    total_silver = df_silver.count()
    active_silver = df_silver.filter(col("is_active") == True).count()
    history_silver = df_silver.filter(col("is_active") == False).count()

    print(f"   -> Status: SUCCESS")
    print(f"   -> Total Profiles (Including History): {total_silver}")
    print(f"   -> Currently Active Subscriptions:     {active_silver}")
    print(f"   -> Historical (Expired) Records:       {history_silver}")
except Exception as e:
    print(f"   -> Status: FAILED (Table not found)")

# --- VALIDATE GOLD ---
print("\n🟡 GOLD LAYER (Business MRR Aggregation)")
try:
    df_gold = spark.read.format("delta").load(gold_path)
    print(f"   -> Status: SUCCESS")
    print(f"   -> MRR Tiers Generated: {df_gold.count()}")
    print("\n📊 FINAL MRR REPORT PREVIEW:")
    
    # Display the actual data clearly
    df_gold.show(truncate=False)
except Exception as e:
    print(f"   -> Status: FAILED (Table not found)")

print("="*60)
print("✅ DATA PIPELINE VALIDATION COMPLETE ✅")
print("="*60)