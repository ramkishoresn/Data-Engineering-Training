import pandas as pd
import numpy as np
import requests
import json
from datetime import datetime, timedelta


# --- 1. Data Collection (using requests to simulate an API call) [cite: 19] ---

# NOTE: Since the sample API "https://api.sampledata.com/orders" does not exist,
# we will simulate the data fetching step by creating local dummy data.

def fetch_sample_data():
    """Simulates fetching JSON order data from an API."""
    print("--- 1. Fetching Sample Order Data (Simulated API Call) ---")

    # Dummy data simulating raw API response
    raw_data = [
        {"order_id": 101, "supplier_id": "S001", "item_name": "Widget A", "quantity": 150,
         "delivery_date": "2025-10-15", "price": 10.00, "status": "Delivered"},
        {"order_id": 102, "supplier_id": "S002", "item_name": "Raw Material B", "quantity": 500,
         "delivery_date": "2025-10-28", "price": 5.50, "status": "Pending"},
        {"order_id": 103, "supplier_id": "S001", "item_name": "Widget A", "quantity": 250,
         "delivery_date": "2025-10-23", "price": 10.00, "status": "Delayed"},
        {"order_id": 104, "supplier_id": "S003", "item_name": "Chemical C", "quantity": None,
         "delivery_date": "2025-11-05", "price": 45.00, "status": "Pending"},  # Missing quantity
        {"order_id": 105, "supplier_id": "S002", "item_name": "Raw Material B", "quantity": 100,
         "delivery_date": "2025-10-01", "price": 5.50, "status": "Delivered"},  # Past and Delayed
        {"order_id": 106, "supplier_id": "S001", "item_name": "Component Z", "quantity": 100, "delivery_date": None,
         "price": 2.00, "status": "Pending"}  # Missing date
    ]

    # In a real scenario, you'd use:
    # response = requests.get("https://api.sampledata.com/orders")
    # return response.json()

    return raw_data


# Load data into a Pandas DataFrame
data = fetch_sample_data()
df = pd.DataFrame(data)

# --- 2. Data Cleaning and Transformation (using pandas) [cite: 20] ---

print("\n--- 2. Data Cleaning and Transformation ---")

# 2.1 Drop nulls for critical columns (e.g., records missing quantity are useless for calculations) [cite: 20]
print(f"Initial shape: {df.shape}")
df.dropna(subset=['quantity', 'delivery_date'], inplace=True)
print(f"Shape after dropping nulls: {df.shape}")

# 2.2 Format Dates (convert string date to datetime objects) [cite: 20]
df['delivery_date'] = pd.to_datetime(df['delivery_date'])  # This mirrors line 31 of the Capstone snippet

# 2.3 Correct data types
df['quantity'] = df['quantity'].astype(int)
df['price'] = df['price'].astype(float)

# --- 3. Feature Engineering and Calculations (using numpy & pandas) [cite: 21] ---

print("\n--- 3. Performing Calculations (Delays) ---")

# Define today's date for delay calculation
today = pd.Timestamp(datetime.now().strftime('%Y-%m-%d'))

# 3.1 Calculate Delay Days (Difference between today and expected delivery date) [cite: 21, 32]
# We only care about delays for items that are not yet delivered
df['delay_days'] = np.where(
    df['status'] != 'Delivered',
    (today - df['delivery_date']).dt.days,
    0  # Set delay to 0 for delivered items
)

# 3.2 Use numpy for conditional check: Is the order delayed? [cite: 21, 33]
# A non-delivered item is delayed if 'delay_days' is greater than 0 (i.e., expected date is in the past)
df['is_delayed'] = np.where(df['delay_days'] > 0, 1, 0)  # Mirrors line 33 of the Capstone snippet

# 3.3 Basic calculation: Total order value
df['total_value'] = df['quantity'] * df['price']

# --- 4. Display Results (The Deliverable Output) [cite: 22] ---

print("\n--- 4. Cleaned and Processed Data Output (Head) ---")

# Display the final DataFrame with key processed outputs [cite: 22, 34]
output_df = df[['order_id', 'supplier_id', 'delivery_date', 'status', 'delay_days', 'is_delayed', 'total_value']]
print(output_df.head(10))

