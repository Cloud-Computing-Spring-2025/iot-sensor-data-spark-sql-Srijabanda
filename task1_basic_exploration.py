from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder.appName("IoT Sensor Task 1").getOrCreate()

print("📥 Reading sensor_data.csv...")

# Load CSV
sensor_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("sensor_data.csv")
)

print("✅ Data loaded!")

# Show first 5 rows
print("🔎 Preview of data:")
sensor_df.show(5)

# Total records
total = sensor_df.count()
print(f"📊 Total records: {total}")

# Distinct locations
print("🌍 Distinct locations:")
sensor_df.select("location").distinct().show()

# Save first 5 rows
sensor_df.limit(5).coalesce(1).write.mode("overwrite").option("header", True).csv("task1_output.csv")

print("💾 First 5 rows saved to folder: task1_output.csv/")

