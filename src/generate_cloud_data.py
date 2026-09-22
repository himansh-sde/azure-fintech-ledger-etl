# src/utils/generate_cloud_data.py
import json
from datetime import datetime
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

def upload_to_adls_gen2(storage_account_name, container_name, directory_name, data):
    """
    Connects to Azure directly and streams a list of dictionaries as a JSON-Lines 
    file straight into the ADLS Gen2 Bronze layer.
    """
    # 1. Authenticate (Uses your 'az login' locally, or Managed Identity in the cloud)
    credential = DefaultAzureCredential()
    service_client = DataLakeServiceClient(
        account_url=f"https://{storage_account_name}.dfs.core.windows.net", 
        credential=credential
    )

    # 2. Format the data as JSON-Lines (one JSON object per line)
    json_lines_data = "\n".join([json.dumps(record) for record in data])
    
    # 3. Generate a dynamic filename based on the current time
    timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
    filename = f"transactions_{timestamp}.json"

    # 4. Connect to the Bronze container and raw_json directory
    file_system_client = service_client.get_file_system_client(file_system=container_name)
    directory_client = file_system_client.get_directory_client(directory_name)
    
    # Create the raw_json directory if it doesn't exist yet
    if not directory_client.exists():
        directory_client.create_directory()

    # 5. Create the file and upload the data directly to the cloud
    file_client = directory_client.get_file_client(filename)
    file_client.upload_data(json_lines_data, overwrite=True)
    
    print(f"Success! Data uploaded to Azure: abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/{directory_name}/{filename}")


# --- BATCH 1: Initial Signups ---
batch_1 = [
    {"user_id": "101", "subscription_tier": "Basic", "transaction_date": "2026-01-01"},
    {"user_id": "102", "subscription_tier": "Basic", "transaction_date": "2026-01-01"},
    {"user_id": "103", "subscription_tier": "Premium", "transaction_date": "2026-01-02"},
]

# --- BATCH 2: Upgrades (Run this later to test the SCD Type 2 Merge) ---
batch_2 = [
    {"user_id": "101", "subscription_tier": "Premium", "transaction_date": "2026-02-15"},
    {"user_id": "104", "subscription_tier": "Ultra", "transaction_date": "2026-02-16"},
]

if __name__ == "__main__":
    # Ensure you are logged into Azure CLI ('az login') before running this
    STORAGE_ACCOUNT = "stfintechdatalake" # Replace if yours is different
    
    print("Generating Batch 1 and streaming directly to ADLS Gen2...")
    upload_to_adls_gen2(
        storage_account_name=STORAGE_ACCOUNT,
        container_name="bronze",
        directory_name="raw_json",
        data=batch_1
    )
    
    # When you are ready to test the Silver Merge logic, comment out Batch 1 above and uncomment below:
    # print("Generating Batch 2 and streaming directly to ADLS Gen2...")
    # upload_to_adls_gen2(STORAGE_ACCOUNT, "bronze", "raw_json", batch_2)