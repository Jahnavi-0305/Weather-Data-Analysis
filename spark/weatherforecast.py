from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, avg, first, abs
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("DataProcessing").getOrCreate()

# Read the data into a DataFrame
df = spark.read.csv('datafinal.csv', header=True, inferSchema=True)

# Pivot the DataFrame to have separate columns for each element (TMAX, TMIN, etc.)
pivoted_df = df.groupBy("station_id", "date").pivot("element").agg(first("filled_value"))

# Mean and standard deviation values for TMAX and TMIN
mean_tmax = 95.0867621717841
stddev_tmax = 144.35195231895892
mean_tmin = -11.043422682173412
stddev_tmin = 134.75291799306655

# Impute missing TAVG values with the average of TMAX and TMIN
imputed_df = pivoted_df.withColumn("TAVG", when(col("TAVG").isNull(), (col("TMAX") + col("TMIN")) / 2).otherwise(col("TAVG")))

# Window specification for calculating rolling average of PRCP
windowSpec = Window.partitionBy("station_id").orderBy("date").rowsBetween(-3, -1)

# Impute missing PRCP values with the average of the previous 3 days
imputed_df = imputed_df.withColumn("PRCP", when(col("PRCP").isNull(), avg(col("PRCP")).over(windowSpec)).otherwise(col("PRCP")))

# Detect and handle outliers for TMAX and TMIN based on the standard deviations calculated above
imputed_df = imputed_df.withColumn("TMAX", when(abs(col("TMAX") - mean_tmax) > 3 * stddev_tmax, None).otherwise(col("TMAX")))
imputed_df = imputed_df.withColumn("TMIN", when(abs(col("TMIN") - mean_tmin) > 3 * stddev_tmin, None).otherwise(col("TMIN")))

# Save the processed DataFrame to a new CSV file
imputed_df.write.csv('processed_datafinal.csv', header=True)

# Stop the Spark session
spark.stop()
