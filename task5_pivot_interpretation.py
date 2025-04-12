from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, avg, to_timestamp, round as spark_round

# Initialize Spark session
spark = SparkSession.builder.appName("IoT Sensor Task 5").getOrCreate()

# Load sensor data
sensor_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("sensor_data.csv")
)

# Convert timestamp and extract hour
sensor_df = sensor_df.withColumn("timestamp", to_timestamp("timestamp"))
sensor_df = sensor_df.withColumn("hour_of_day", hour("timestamp"))

# Pivot: average temperature by location and hour
pivot_df = (
    sensor_df.groupBy("location")
    .pivot("hour_of_day")
    .agg(avg("temperature"))
)

# Round temperature values to 2 decimal places
for hour_col in pivot_df.columns[1:]:  # Skip "location" column
    pivot_df = pivot_df.withColumn(hour_col, spark_round(hour_col, 2))

# Save pivoted table to single CSV file
pivot_df.coalesce(1).write.mode("overwrite").option("header", True).csv("task5_output.csv")

print("📊 Hourly pivot table by location saved to 'task5_output.csv/'")
