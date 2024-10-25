from pyspark.sql import SparkSession

# Create Spark session
spark = SparkSession.builder \
    .appName("ExtremeWeatherAnalysis") \
    .getOrCreate()

# Read data from CSV
df = spark.read.option("header", "true").csv("2022clean.csv")

# Register as a temporary view for Spark SQL queries
df.createOrReplaceTempView("weather_data")

# Hot spells in the USA (AQ)
us_hot_spells_df = spark.sql("""
    SELECT
        station_id,
        MIN(date) AS start_date,
        MAX(date) AS end_date,
        'hot_spell' AS spell_type,
        MAX(filled_value) AS max_temperature
    FROM weather_data
    WHERE element = 'TMAX' AND filled_value > 30.0 AND station_id LIKE 'AQ%'
    GROUP BY station_id
""")

# Cold spells in the USA (AQ)
us_cold_spells_df = spark.sql("""
    SELECT
        station_id,
        MIN(date) AS start_date,
        MAX(date) AS end_date,
        'cold_spell' AS spell_type,
        MIN(filled_value) AS min_temperature
    FROM weather_data
    WHERE element = 'TMIN' AND filled_value < 0.0 AND station_id LIKE 'AQ%'
    GROUP BY station_id
""")

# Hot spells in Canada (CA)
ca_hot_spells_df = spark.sql("""
    SELECT
        station_id,
        MIN(date) AS start_date,
        MAX(date) AS end_date,
        'hot_spell' AS spell_type,
        MAX(filled_value) AS max_temperature
    FROM weather_data
    WHERE element = 'TMAX' AND filled_value > 30.0 AND station_id LIKE 'CA%'
    GROUP BY station_id
""")

# Cold spells in Canada (CA)
ca_cold_spells_df = spark.sql("""
    SELECT
        station_id,
        MIN(date) AS start_date,
        MAX(date) AS end_date,
        'cold_spell' AS spell_type,
        MIN(filled_value) AS min_temperature
    FROM weather_data
    WHERE element = 'TMIN' AND filled_value < 0.0 AND station_id LIKE 'CA%'
    GROUP BY station_id
""")

# Hot spells in Mexico (MX)
mx_hot_spells_df = spark.sql("""
    SELECT
        station_id,
        MIN(date) AS start_date,
        MAX(date) AS end_date,
        'hot_spell' AS spell_type,
        MAX(filled_value) AS max_temperature
    FROM weather_data
    WHERE element = 'TMAX' AND filled_value > 30.0 AND station_id LIKE 'MX%'
    GROUP BY station_id
""")

# Cold spells in Mexico (MX)
mx_cold_spells_df = spark.sql("""
    SELECT
        station_id,
        MIN(date) AS start_date,
        MAX(date) AS end_date,
        'cold_spell' AS spell_type,
        MIN(filled_value) AS min_temperature
    FROM weather_data
    WHERE element = 'TMIN' AND filled_value < 0.0 AND station_id LIKE 'MX%'
    GROUP BY station_id
""")

# Show the results
print("Hot Spells - USA (AQ)")
us_hot_spells_df.orderBy("station_id").show()

print("Cold Spells - USA (AQ)")
us_cold_spells_df.orderBy("station_id").show()

print("Hot Spells - Canada (CA)")
ca_hot_spells_df.orderBy("station_id").show()

print("Cold Spells - Canada (CA)")
ca_cold_spells_df.orderBy("station_id").show()

print("Hot Spells - Mexico (MX)")
mx_hot_spells_df.orderBy("station_id").show()

print("Cold Spells - Mexico (MX)")
mx_cold_spells_df.orderBy("station_id").show()

# Stop the Spark session
spark.stop()
