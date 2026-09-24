# src/utils/generate_mock_data.py
import json
import os
from datetime import datetime

def generate_batch(batch_number, data, folder_path="mock_data"):
    """Writes a list of dictionaries to a JSON file."""
    if not os.path.exists(folder_path):
        os.makedirs(folder_path)
        
    filename = f"{folder_path}/transactions_batch_{batch_number}_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    
    with open(filename, 'w') as f:
        for record in data:
            f.write(json.dumps(record) + '\n')
            
    print(f"Generated {filename} with {len(data)} records.")

# --- BATCH 1: Initial Signups (January) ---
batch_1 = [
    {"user_id": "101", "subscription_tier": "Basic", "transaction_date": "2026-01-01"},
    {"user_id": "102", "subscription_tier": "Basic", "transaction_date": "2026-01-01"},
    {"user_id": "103", "subscription_tier": "Premium", "transaction_date": "2026-01-02"},
]

# --- BATCH 2: Upgrades and New Users (February) ---
batch_2 = [
    {"user_id": "101", "subscription_tier": "Premium", "transaction_date": "2026-02-15"}, # UPGRADED! (Triggers SCD2)
    {"user_id": "104", "subscription_tier": "Ultra", "transaction_date": "2026-02-16"},   # NEW USER
]

if __name__ == "__main__":
    print("Generating mock data batches...")
    generate_batch(1, batch_1)
    generate_batch(2, batch_2) # We will run this later to test the Silver Merge!