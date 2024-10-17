# Weather-Data-Analysis

#### **Project Overview:**
This project focuses on the analysis and comparison of weather data collected from various weather stations across multiple countries, including the USA, Canada, and Mexico. The project includes data cleaning, imputation of missing values, analysis of temperature trends over specific time periods, and a comparison of temperature patterns between different locations. The primary goal is to process large-scale weather data, identify trends in temperature, and detect extreme weather conditions such as hot and cold spells. 

#### **Key Features:**

1. **Data Cleaning and Imputation:**
   - The weather data cleaning script (`weather_data_cleaning.py`) is responsible for filtering the data based on date and geographical region (US, CA, MX), and imputing missing temperature and precipitation values. This ensures the dataset is complete and ready for further analysis. Missing values are imputed using windowing functions and averages of nearby entries, while temperature outliers are corrected based on neighboring data.

2. **Temperature Analysis by Location and Time Period:**
   - The analysis script (`temperature_analysis.py`) performs temperature analysis for a given weather station within a specified time period. It computes the average minimum, maximum, and average temperatures for each day, month, or year, providing insights into the local temperature trends at a particular location.

3. **Temperature Comparison Between Two Locations:**
   - The comparison script (`temperature_comparison.py`) allows for a direct comparison of temperature trends between two different weather stations. Users can specify two locations and a time period, and the script computes the average temperatures and the difference in temperature between the locations. This comparison helps in understanding regional climate variations over time.

4. **Extreme Weather Detection:**
   - In a subsequent phase (`extreme_weather_analysis.py`), the project detects extreme weather events such as hot spells (periods with abnormally high temperatures) and cold spells (periods with abnormally low temperatures) for stations in the US, Canada, and Mexico. The script aggregates the data, identifies these events, and groups them by country, providing insights into extreme weather patterns in North America.

#### **Technologies Used:**
- **Apache Spark**: Used for distributed data processing, ensuring efficient handling of large weather datasets.
- **Python**: The primary programming language used to implement data analysis and transformation logic.
- **PySpark SQL**: Utilized for executing SQL queries over the weather dataset to filter, aggregate, and analyze data.
- **CSV**: Data format used for storing weather data, allowing for easy data exchange and integration.
