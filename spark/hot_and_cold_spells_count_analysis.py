from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("HotAndColdSpellsCountAnalysis") \
    .getOrCreate()

# Read data from CSV
df = spark.read.option("header", "true").csv("2022clean.csv")

# Register as a temporary view for Spark SQL queries
df.createOrReplaceTempView("weather_data")

# Count hot spells for USA, Canada, and Mexico
hot_spells_count_df = spark.sql("""
    SELECT country,
           COUNT(DISTINCT station_id) AS hot_spells
    FROM (
        SELECT CASE
                   WHEN station_id LIKE 'AQ%' THEN 'USA'
                   WHEN station_id LIKE 'CA%' THEN 'Canada'
                   WHEN station_id LIKE 'MX%' THEN 'Mexico'
               END AS country,
               station_id
        FROM weather_data
        WHERE element = 'TMAX' AND filled_value > 30.0
        AND (station_id LIKE 'AQ%' OR station_id LIKE 'CA%' OR station_id LIKE 'MX%')
    )
    GROUP BY country
    ORDER BY country
""")

# Count cold spells for USA, Canada, and Mexico
cold_spells_count_df = spark.sql("""
    SELECT country,
           COUNT(DISTINCT station_id) AS cold_spells
    FROM (
        SELECT CASE
                   WHEN station_id LIKE 'AQ%' THEN 'USA'
                   WHEN station_id LIKE 'CA%' THEN 'Canada'
                   WHEN station_id LIKE 'MX%' THEN 'Mexico'
               END AS country,
               station_id
        FROM weather_data
        WHERE element = 'TMIN' AND filled_value < 0.0
        AND (station_id LIKE 'AQ%' OR station_id LIKE 'CA%' OR station_id LIKE 'MX%')
    )
    GROUP BY country
    ORDER BY country
""")

# Show the counts
print("\nHot Spells Count for All Countries")
print("=================================")
hot_spells_count_df.show()

print("\nCold Spells Count for All Countries")
print("=================================")
cold_spells_count_df.show()

# Stop the Spark session
spark.stop()
