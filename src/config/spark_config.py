#src/config/spark_config.py
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils

def configure_spark_abfs(spark: SparkSession):
	"""
	Configures Spark to authenticate with ADLS Gen2 using Azure Key Vault secrets.
	
	"""

	# In a standard python scripts, DBUtils must be instantiated
	dbutils = DBUtils(spark)

	storage_account_name = "stfintechdatalake"
	scope_name = "akv-fintech-scope"

	client_id = dbutils.secrets.get(scope=scope_name, key="fintech-client-id")
	tenant_id = dbutils.secrets.get(scope=scope_name, key="fintech-tenant-id")
	client_secret= dbutils.secrets.get(scope=scope_name, key="fintech-client-secret")


	spark.conf.set(f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net", "OAuth")
	spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
	spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account_name}.dfs.core.windows.net", client_id)
	spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account_name}.dfs.core.windows.net", client_secret)
    spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")



return storage_account_name
