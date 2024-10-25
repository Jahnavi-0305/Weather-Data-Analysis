from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, lag, lead, row_number, when
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Weather Data Cleaning") \
    .getOrCreate()

# Load data
weather_data = spark.read.csv("weather_data.csv", header=True, inferSchema=True)
station_info = spark.read.csv("station_info.csv", header=True, inferSchema=True)

# Filter and augment data
weather_data = weather_data \
    .filter((col("date") >= '1974-01-01') & (col("date") <= '2024-12-31')) \
    .filter(col("country").isin("US", "CA", "MX")) \
    .select("station_id", "date", "TMIN", "TMAX", "TAVG", "PRCP")

augmented_data = weather_data.join(station_info, on="station_id")

# Handle missing or incomplete data
window_spec = Window.partitionBy("station_id").orderBy("date")

augmented_data = augmented_data.withColumn(
    "consecutive_missing_days",
    row_number().over(window_spec)
)

augmented_data = augmented_data.filter(
    count(col("consecutive_missing_days")) <= 10
)

# Impute missing temperature data
augmented_data = augmented_data.withColumn(
    "TAVG",
    when(col("TAVG").isNull(), (col("TMIN") + col("TMAX")) / 2).otherwise(col("TAVG"))
)

# Handle temperature outliers
augmented_data = augmented_data.withColumn(
    "TMIN",
    when(col("TMIN").isNull(), (lag(col("TMIN"), 3).over(window_spec) + lead(col("TMIN"), 3).over(window_spec)) / 2).otherwise(col("TMIN"))
)

augmented_data = augmented_data.withColumn(
    "TMAX",
    when(col("TMAX").isNull(), (lag(col("TMAX"), 3).over(window_spec) + lead(col("TMAX"), 3).over(window_spec)) / 2).otherwise(col("TMAX"))
)

# Impute missing precipitation data
augmented_data = augmented_data.withColumn(
    "PRCP",
    when(col("PRCP").isNull(), avg(col("PRCP")).over(window_spec)).otherwise(col("PRCP"))
)

# Handle precipitation outliers
augmented_data = augmented_data.withColumn(
    "PRCP",
    when(col("PRCP").isNull(), avg(col("PRCP")).over(window_spec)).otherwise(col("PRCP"))
)

# Write cleaned data
augmented_data.write.format("csv").save("2022clean.csv")

# Stop Spark session
spark.stop()
