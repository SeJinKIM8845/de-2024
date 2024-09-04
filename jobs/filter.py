from base import BaseFilter
from pyspark.sql.functions import col, desc

class TopDatesFilter(BaseFilter):
    def filter(self, df):
        return df.groupBy("date").count().orderBy(desc("count")).limit(3)

class TimeSeriesFilter(BaseFilter):
    def filter(self, df, top_dates):
        top_dates_list = [row['date'] for row in top_dates.collect()]
        return df.filter(col("date").isin(top_dates_list))

class DetailedTimeSeriesFilter(BaseFilter):
    def filter(self, df):
        return df.select(
            "date",
            "time",
            "timestamp",  # 이 줄을 추가
            "battery_voltage",
            "battery_current",
            "altitude",
            "speed"
        ).orderBy("date", "time")