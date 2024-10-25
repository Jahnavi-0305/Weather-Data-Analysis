from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

def compare_temperatures(input_file, location1, location2, start_date, end_date, aggregation_unit, output_file):
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("Temperature Comparison") \
        .getOrCreate()

    # Read data into DataFrame
    df = spark.read.csv(input_file, header=True, inferSchema=True)

    # Filter data based on location and time period for location 1
    filtered_df1 = df.filter((df["station_id"] == location1) & (df["date"] >= start_date) & (df["date"] <= end_date))

    # Filter data based on location and time period for location 2
    filtered_df2 = df.filter((df["station_id"] == location2) & (df["date"] >= start_date) & (df["date"] <= end_date))

    # Group data by aggregation unit and compute averages for location 1
    if aggregation_unit == "year":
        aggregation_column = df["date"].substr(1, 4)
    elif aggregation_unit == "month":
        aggregation_column = df["date"].substr(1, 6)
    else:  # aggregation_unit == "day"
        aggregation_column = df["date"]

    result_df1 = filtered_df1.groupBy(aggregation_column).agg(
        avg(df["filled_value"]).alias("avg_min_temperature1"),
        avg(df["filled_value"]).alias("avg_max_temperature1")
    )

    # Group data by aggregation unit and compute averages for location 2
    result_df2 = filtered_df2.groupBy(aggregation_column).agg(
        avg(df["filled_value"]).alias("avg_min_temperature2"),
        avg(df["filled_value"]).alias("avg_max_temperature2")
    )

    # Join the two result DataFrames based on the aggregation unit
    joined_df = result_df1.join(result_df2, on=aggregation_column, how="inner")

    # Compute temperature difference
    joined_df = joined_df.withColumn("temperature_difference",
        joined_df["avg_max_temperature1"] - joined_df["avg_max_temperature2"]
    )

    # Write results to output file
    joined_df.write.csv(output_file, mode="overwrite", header=True)

    # Stop SparkSession
    spark.stop()

# Example usage
compare_temperatures("2022clean.csv", "CA001011500", "CA001012040", "20120101", "20120131", "month", "temperature_comparison.csv")
