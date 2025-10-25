from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when, count

# 1. Initialize Spark Session
# This is the entry point for all PySpark functionality
spark = SparkSession.builder \
    .appName("SupplyChainPySparkProcessor") \
    .getOrCreate()

# --- 2. Load Data from CSV ---

# Define the path to your CSV file. 
# NOTE: Replace 'path/to/' with your actual file path.
# We infer the schema to automatically detect data types.
file_path = "path/to/processed_orders.csv"

# In a real environment, you'd load the cleaned data from Week 2.
# Since we don't have that file here, we'll create a dummy DataFrame 
# and save it to simulate the load.

# --- SIMULATION START: Creating dummy CSV to ensure script runs ---
# This part simulates the output of Week 2 to make Week 3 runnable.
data = [
    (101, "S001", "2025-10-15", "Delivered", 0, 0, 1500.00),
    (102, "S002", "2025-10-28", "Pending", -3, 0, 2750.00), # Not delayed yet
    (103, "S001", "2025-10-23", "Delayed", 2, 1, 2500.00), # Delayed (Expected 2 days ago)
    (105, "S002", "2025-10-01", "Delivered", 0, 0, 550.00)
]
columns = ["order_id", "supplier_id", "delivery_date", "status", "delay_days", "is_delayed", "total_value"]
dummy_df = spark.createDataFrame(data, columns)
dummy_df.write.mode("overwrite").csv("data/raw_orders_pyspark.csv", header=True)
# --- SIMULATION END ---

# Now, load the data from the simulated CSV
orders_df = spark.read.csv("data/raw_orders_pyspark.csv", header=True, inferSchema=True)
print("Orders DataFrame Schema:")
orders_df.printSchema()


# --- 3. Filter Delayed Shipments ---
# We filter for records where 'is_delayed' is 1 (True)
delayed_shipments_df = orders_df.filter(col("is_delayed") == 1)

print("\nDelayed Shipments Preview:")
delayed_shipments_df.show(5)


# --- 4. Group by Supplier and Count Delayed Orders ---
# Aggregation to find how many delayed orders each supplier has
delayed_counts_by_supplier = delayed_shipments_df.groupBy("supplier_id") \
    .agg(
        count(col("order_id")).alias("Delayed_Order_Count"),
        sum(col("total_value")).alias("Total_Delayed_Value")
    ) \
    .orderBy(col("Delayed_Order_Count").desc())

print("\nGrouped Results: Supplier Performance (Delayed Orders)")
delayed_counts_by_supplier.show()


# --- 5. Save Processed Data to CSV (or Parquet) ---
output_path = "data/processed/supplier_delay_metrics"

# Save the grouped results as Parquet (optimized for subsequent Spark reads)
delayed_counts_by_supplier.write \
    .mode("overwrite") \
    .parquet(output_path + "_parquet")

# Save the grouped results as CSV (for easy human review - the Deliverable "Output file")
delayed_counts_by_supplier.write \
    .mode("overwrite") \
    .csv(output_path + "_csv", header=True)

print(f"\nProcessing Complete. Output saved to: {output_path}_*")

# Stop the Spark session
spark.stop()
