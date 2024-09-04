from pyspark.sql.functions import col, to_timestamp, concat, lit
from pyspark.sql.types import TimestampType, DoubleType, DateType

def read_input(spark, input_path):
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    return df

def init_df(df):
    return df.withColumn("date", col("date").cast(DateType())) \
        .withColumn("timestamp", to_timestamp(concat(col("date").cast("string"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("altitude", col("altitude").cast(DoubleType())) \
        .withColumn("battery_voltage", col("battery_voltage").cast(DoubleType())) \
        .withColumn("battery_current", col("battery_current").cast(DoubleType())) \
        .withColumn("speed", col("speed").cast(DoubleType()))

class BaseFilter:
    def __init__(self, spark):
        self.spark = spark

    def filter(self, df):
        pass