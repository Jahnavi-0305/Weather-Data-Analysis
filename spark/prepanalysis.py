from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder \
    .appName("HeavyRainfallDroughtAnalysis") \
    .getOrCreate()

# Load precipitation data from CSV file
precipitation_data = spark.read.csv("10yearsclean.csv", header=True, inferSchema=True)

# Filter data for PRCP (precipitation) element
precipitation_data = precipitation_data.filter(precipitation_data.element == "PRCP")

# Calculate average precipitation
average_precipitation = precipitation_data.agg({"filled_value": "avg"}).collect()[0][0]

# Define threshold values for heavy rainfall and drought
heavy_rainfall_threshold = average_precipitation * 2
drought_threshold = average_precipitation / 2

# Identify periods of heavy rainfall or drought
heavy_rainfall_periods = precipitation_data.filter(col("filled_value") > heavy_rainfall_threshold)
drought_periods = precipitation_data.filter(col("filled_value") < drought_threshold)

# Display results
print("Periods of Heavy Rainfall:")
heavy_rainfall_periods.show()

print("Periods of Drought:")
drought_periods.show()

# Stop Spark session
spark.stop()
