from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, to_timestamp, concat, lit, hour, avg, date_format, to_date
from pyspark.sql.types import TimestampType, DoubleType, DateType

# SparkSession
spark = (SparkSession.builder 
    .appName("DroneDataAnalysis") 
    .getOrCreate())

# CSV file
df = spark.read.csv("/opt/bitnami/spark/data/flights.csv", header=True, inferSchema=True)
df.show(5, truncate=False)

df.select("date").distinct().show(10)

df.select("time_day").distinct().show(10)

df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd")) \
       .withColumn("time_day", date_format(col("time_day"), "HH:mm:ss")) \
       .withColumn("timestamp", to_timestamp(concat(col("date").cast("string"), lit(" "), col("time_day")), "yyyy-MM-dd HH:mm:ss")) \
       .withColumn("altitude", col("altitude").cast(DoubleType())) \
       .withColumn("battery_voltage", col("battery_voltage").cast(DoubleType())) \
       .withColumn("battery_current", col("battery_current").cast(DoubleType())) \
       .withColumn("speed",col("speed").cast(DoubleType()))

df.printSchema()

date_counts = df.groupBy("date").count().orderBy(col("count").desc())
date_counts.show(10)

top_dates = date_counts.limit(3)
top_dates.show()

top_dates_list = [row['date'] for row in top_dates.collect()]

time_series_data = df.filter(col("date").isin(top_dates_list)) \
    .select("date", "timestamp", "battery_voltage", "battery_current", "altitude", "speed") \
    .orderBy("timestamp")

time_series_data.show(20)

# hourly_data = time_series_data.groupBy(
#     time_series_data.date,
#     hour(time_series_data.timestamp).alias("hour")
# ).agg(
#     avg("battery_voltage").alias("avg_voltage"),
#     avg("battery_current").alias("avg_current"),
#     avg("altitude").alias("avg_altitude"),
#     avg("speed").alias("avg_speed")
# ).orderBy("date", "hour")

# hourly_data.show()

spark.stop()