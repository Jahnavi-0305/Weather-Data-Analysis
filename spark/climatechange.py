from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, avg, min, max, sum, substring, col

# Create a Spark session
spark = SparkSession.builder \
    .appName("Climate Analysis") \
    .getOrCreate()

# Load the CSV data into a DataFrame
file_path = "/home/spark/datafinal.csv"
df = spark.read.format("csv") \
    .option("header", "false") \
    .option("inferSchema", "true") \
    .load(file_path) \
    .toDF("station_id", "date", "element", "filled_value")

# Extract country and state from station_id
df = df.withColumn("country", substring(df["station_id"], 1, 2))
df = df.withColumn("state", substring(df["station_id"], 3, 2))

# Filter data for US, Canada, and Mexico
filtered_df = df.filter(df["country"].isin("AQ", "CA", "MX"))

# Extract year and month from the date column
parsed_df = filtered_df.withColumn("year", substring("date", 1, 4).cast("int"))
parsed_df = parsed_df.withColumn("month", substring("date", 5, 2).cast("int"))

# Temperature analysis
temp_agg_df = parsed_df.filter(parsed_df["element"] == "TAVG") \
    .groupBy("country", "state", "year", "month") \
    .agg(
        avg("filled_value").alias("avg_temperature"),
        min("filled_value").alias("min_temperature"),
        max("filled_value").alias("max_temperature")
    ) \
    .orderBy("country", "state", "year", "month")

# Precipitation analysis
precip_agg_df = parsed_df.filter(parsed_df["element"] == "PRCP") \
    .groupBy("country", "state", "year", "month") \
    .agg(sum("filled_value").alias("total_precipitation")) \
    .orderBy("country", "state", "year", "month")

# Long-term trend identification
long_term_trend_df = parsed_df \
    .groupBy("country", "state", "year") \
    .agg(
        avg("filled_value").alias("avg_temperature"),
        sum("filled_value").alias("total_precipitation")
    )

# Regional comparisons
regional_comparison_df = long_term_trend_df \
    .groupBy("country") \
    .agg(
        avg("avg_temperature").alias("avg_avg_temperature"),
        avg("total_precipitation").alias("avg_total_precipitation")
    )

# Show the results
print("Temperature Analysis:")
temp_agg_df.show()

print("Precipitation Analysis:")
precip_agg_df.show()

print("Long-term Trend Identification:")
long_term_trend_df.show()

print("Regional Comparisons:")
regional_comparison_df.show()

# Stop the Spark session
spark.stop()
