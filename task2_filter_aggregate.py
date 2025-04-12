from pyspark.sql import SparkSession

# Start Spark session
spark = SparkSession.builder.appName("IoT Sensor Task 2").getOrCreate()

# Load sensor data
sensor_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("sensor_data.csv")
)

# Filter in-range and out-of-range temperature readings
temp_in_range = sensor_df.filter((sensor_df.temperature >= 18) & (sensor_df.temperature <= 30))
temp_out_range = sensor_df.filter((sensor_df.temperature < 18) | (sensor_df.temperature > 30))

# Count results
print("🌡️ In-range count:", temp_in_range.count())
print("🔥 Out-of-range count:", temp_out_range.count())

# Group by location and compute averages
agg_df = (
    sensor_df.groupBy("location")
    .avg("temperature", "humidity")
    .withColumnRenamed("avg(temperature)", "avg_temperature")
    .withColumnRenamed("avg(humidity)", "avg_humidity")
    .orderBy("avg_temperature", ascending=False)
)

# Save aggregated result
agg_df.coalesce(1).write.mode("overwrite").option("header", True).csv("task2_output.csv")

print("✅ Aggregated output saved to 'task2_output.csv/'")
