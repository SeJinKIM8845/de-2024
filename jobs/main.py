from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, to_timestamp, concat, lit

from base import read_input, init_df
from filter import TopDatesFilter, TimeSeriesFilter, DetailedTimeSeriesFilter
from es import Es
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", default="/opt/bitnami/spark/data/flights.csv", help="Input CSV file path")
    args = parser.parse_args()

    spark = (SparkSession
             .builder
             .master("local")
             .appName("DroneDataAnalysis")
             .config("spark.driver.extraClassPath", "/opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar")
             .config("spark.jars", "/opt/bitnami/spark/resources/elasticsearch-spark-30_2.12-8.4.3.jar")
             .getOrCreate())
    
    df = read_input(spark, args.input_path)
    df = init_df(df)

    # Print schema for debugging
    print("Initial DataFrame Schema:")
    df.printSchema()

    top_dates_filter = TopDatesFilter(spark)
    top_dates = top_dates_filter.filter(df)

    time_series_filter = TimeSeriesFilter(spark)
    time_series_data = time_series_filter.filter(df, top_dates)

    detailed_time_series_filter = DetailedTimeSeriesFilter(spark)
    detailed_data = detailed_time_series_filter.filter(time_series_data)

    # Print schema before string formatting for debugging
    print("Detailed Data Schema before string formatting:")
    detailed_data.printSchema()

    # Recreate timestamp column if it's missing
    if "timestamp" not in detailed_data.columns:
        detailed_data = detailed_data.withColumn("timestamp", 
            to_timestamp(concat(col("date").cast("string"), lit(" "), col("time")), "yyyy-MM-dd HH:mm:ss"))

    # Format date and timestamp as strings
    detailed_data = detailed_data.withColumn("date_string", date_format("date", "yyyy-MM-dd"))
    detailed_data = detailed_data.withColumn("timestamp_string", date_format("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))

    # Print final schema for debugging
    print("Final Detailed Data Schema:")
    detailed_data.printSchema()

    top_dates.show()
    time_series_data.show()
    detailed_data.show()

    es = Es("http://es:9200")

    for date in top_dates.select("date").rdd.flatMap(lambda x: x).collect():
        date_data = detailed_data.filter(col("date") == date)
        
        date_data = date_data.orderBy("date", "time")
        
        es.write_df(date_data, f"drone-data-{date}")

    print("Data processing and storage completed.")
    spark.stop()