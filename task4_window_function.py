from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, dense_rank
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("IoT Sensor Task 4").getOrCreate()

# Load the dataset
sensor_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv("sensor_data.csv")
)

# Compute average temperature per sensor
avg_temp_df = (
    sensor_df.groupBy("sensor_id")
    .agg(avg("temperature").alias("avg_temp"))
)

# Rank sensors by average temperature (highest first)
rank_spec = Window.orderBy(avg_temp_df["avg_temp"].desc())
ranked_df = avg_temp_df.withColumn("rank_temp", dense_rank().over(rank_spec))

# Save top 5 sensors by temperature to CSV
ranked_df.limit(5).coalesce(1).write.mode("overwrite").option("header", True).csv("task4_output.csv")

print("🏅 Top 5 sensors by average temperature saved to 'task4_output.csv/'")
