from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, avg, to_timestamp

# Initialize Spark session
spark = SparkSession.builder.appName("IoT Sensor Task 3").getOrCreate()

# Load sensor data
sensor_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("sensor_data.csv")
)

# Convert string to timestamp
sensor_df = sensor_df.withColumn("timestamp", to_timestamp("timestamp"))

# Optional: Register as a SQL view
sensor_df.createOrReplaceTempView("sensor_readings")

# Extract hour from timestamp
sensor_df = sensor_df.withColumn("hour_of_day", hour("timestamp"))

# Calculate average temperature per hour
hourly_avg_temp = (
    sensor_df.groupBy("hour_of_day")
    .agg(avg("temperature").alias("avg_temp"))
    .orderBy("hour_of_day")
)

# Save result to a single CSV file
hourly_avg_temp.coalesce(1).write.mode("overwrite").option("header", True).csv("task3_output.csv")

print("✅ Hourly temperature averages saved to 'task3_output.csv/'")
