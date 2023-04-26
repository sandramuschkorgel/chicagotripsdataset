from etl import create_spark_session


spark = create_spark_session()

# Read in cleaned data files
trips = spark.read.parquet("data/trips/*/*/*")
time = spark.read.parquet("data/time/*/*/*")
weather = spark.read.option("header", "true").csv("data/weather_cleaned.csv")
communities = spark.read.option("header", "true").csv("data/communities_cleaned.csv")

# Create temporary views to run sql queries
trips.createOrReplaceTempView("trips_table")
time.createOrReplaceTempView("time_table")
weather.createOrReplaceTempView("weather_table")
communities.createOrReplaceTempView("communities_table")

# Uncomment .show() on any query to see results
spark.sql("SELECT * FROM trips_table LIMIT 5")#.show()

spark.sql("""
    SELECT pickup_area,  COUNT(*) AS trip_count, AVG(fare) AS avg_fare
    FROM trips_table
    GROUP BY pickup_area
    ORDER BY trip_count DESC
    LIMIT 10
""")#.show()

spark.sql("""
    SELECT condition_code, AVG(fare)
    FROM trips_table
    JOIN time_table ON trips_table.trip_start_time = time_table.trip_time
    JOIN weather_table ON time_table.rounded_hour = weather_table.hour
    WHERE weather_table.rain IS NOT NULL
    GROUP BY condition_code
    LIMIT 5
""")#.show()

spark.sql("""
    SELECT trip_id, fare, pickup_area, name, asian_perc, black_perc, latino_perc, white_perc
    FROM trips_table
    JOIN communities_table
    ON trips_table.pickup_area = communities_table.id
    LIMIT 5
""").show()


# Trips mapping
# Question: In which communities do trips frequently end, when they started at the airport O'Hare?
# Partitioned by pickup area, set to 76 (which represents O'Hare)
trips_mapping = spark.read.parquet("data/trips_mapping/pickup_area=76/*")
trips_mapping.createOrReplaceTempView("trips_mapping_table")
spark.sql("""
    SELECT dropoff_area, COUNT(*) AS trips_count
    FROM trips_mapping_table
    GROUP BY dropoff_area
    ORDER BY trips_count DESC
    LIMIT 10
""")#.show()

