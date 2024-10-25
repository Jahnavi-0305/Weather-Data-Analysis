from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Step 1: Setup Spark Session
spark = SparkSession.builder \
    .appName("TemperatureAnalysisLocationTimePeriod") \
    .getOrCreate()

# Step 2: Read Cleaned Data
csv_path = "2022clean.csv"
df = spark.read.option("header", "true").csv(csv_path)

# Step 3: Define Function for Temperature Analysis
def temperature_analysis(location, start_date, end_date):
    filtered_df = df.filter((df["station_id"] == location) & (df["date"] >= start_date) & (df["date"] <= end_date))
    
    aggregated_df = filtered_df.groupBy("date").agg(
        F.avg(F.when(df["element"] == "TMIN", df["filled_value"])).alias("avg_min_temp"),
        F.avg(F.when(df["element"] == "TMAX", df["filled_value"])).alias("avg_max_temp"),
        F.avg(F.when(df["element"] == "TAVG", df["filled_value"])).alias("avg_avg_temp")
    ).orderBy("date")
    
    return aggregated_df

# Step 4: Perform Temperature Analysis
location = "AQW00061705"  # Example location
start_date = "20120101"   # Example start date
end_date = "20240131"     # Example end date

result_df = temperature_analysis(location, start_date, end_date)

# Step 5: Save Results to File
result_df.write.csv("/home/spark/Final/temperature_analysis_location_time_period1", mode="overwrite", header=True)

# Step 6: Stop Spark Session
spark.stop()
