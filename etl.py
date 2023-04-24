import os
import logging
#import configparser
try:
    import configparser
except:
    from six.moves import configparser


import pandas as pd

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_trunc, countDistinct
from pyspark.sql.types import *

from trips_schema import Schema
from clean_trips import clean_trips
from clean_weather import clean_weather
from clean_communities import clean_communities


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['CREDENTIALS']['AWS_ACCESS_KEY_ID']
AWS_ACCESS_KEY_ID =config['CREDENTIALS']['AWS_ACCESS_KEY_ID']

os.environ['AWS_SECRET_ACCESS_KEY']=config['CREDENTIALS']['AWS_SECRET_ACCESS_KEY']
AWS_SECRET_ACCESS_KEY = config['CREDENTIALS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Creates Spark session
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .enableHiveSupport() \
        .config("spark.driver.memory", "15g") \
        .getOrCreate()
    return spark

    # spark = SparkSession \
    #     .builder\
    #     .config("spark.jars.repositories", "https://repos.spark-packages.org/")\
    #     .config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11")\
    #     .enableHiveSupport() \
    #     .getOrCreate()
    # return spark


def process_trips(spark, input_data, output_data):
    """
    Loads trips data from API and performs required transformations 
    Writes output tables to S3 in parquet format
    
    Input:
    spark - entry point to programming Spark
    input_data - link to local source data
    output_data - link to target S3 bucket 
    
    Output:
    trips_table - each trip represents one record (fact table)
    trips_mapping_table - each trips pickup and dropoff location data (fact table)
    time_table - detailed trip start time
    """
    
    sc = spark.sparkContext
    sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
    sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)


    # Read in trips data files
    trips_data = os.path.join(input_data, "trips_data/2020/1/*.json")
    trips = spark.read.json(trips_data, schema=Schema.trips_schema, mode="PERMISSIVE", columnNameOfCorruptRecord="corrupt_record")

    #print(f"{trips.count()} trips processed")

    # Clean trips data
    trips = clean_trips(trips)

    # Select columns for trips_table, trips_mapping_table and time_table
    trips_table = trips.select("trip_id", "trip_start_timestamp", "trip_end_timestamp", "year", "month", \
        "pickup_community_area", "dropoff_community_area", "trip_seconds", "trip_miles", "fare", "tip", \
        "additional_charges", "trip_total", "shared_trip_authorized", "trips_pooled") \
        .withColumnRenamed("trip_start_timestamp", "trip_start_time") \
        .withColumnRenamed("trip_end_timestamp", "trip_end_time") \
        .withColumnRenamed("pickup_community_area", "pickup_area") \
        .withColumnRenamed("dropoff_community_area", "dropoff_area")

    trips_mapping_table = trips.select("trip_id", "pickup_community_area", "dropoff_community_area", \
        "pickup_centroid_latitude", "pickup_centroid_longitude", "dropoff_centroid_latitude", "dropoff_centroid_longitude") \
        .withColumnRenamed("pickup_community_area", "pickup_area") \
        .withColumnRenamed("dropoff_community_area", "dropoff_area") \
        .withColumnRenamed("pickup_centroid_latitude", "pickup_lat") \
        .withColumnRenamed("pickup_centroid_longitude", "pickup_long") \
        .withColumnRenamed("dropoff_centroid_latitude", "dropoff_lat") \
        .withColumnRenamed("dropoff_centroid_longitude", "dropoff_long")

    time_table = trips.select("trip_start_timestamp", "year", "month", "day", "dayofweek", date_trunc("hour", col("trip_start_timestamp")).alias("rounded_hour")) \
        .withColumnRenamed("trip_start_timestamp", "trip_time")

    # Quality check: trip_id not null and unique
    try:
        trips.agg(countDistinct("trip_id")) == trips.count()
        logging.info("Data quality check on trips data passed.")
    except Exception as e:
        logging.info("Data quality check on trips data failed. No of unique trip ids doesn't match no of records")
        print(e)

    # Save cleaned tables as parquet files to output path 
    trips_table.write.partitionBy("year", "month").mode("overwrite").parquet(os.path.join(output_data, "trips_cleaned/"))
    trips_mapping_table.write.partitionBy("pickup_area").mode("overwrite").parquet(os.path.join(output_data, "trips_mapping/"))
    time_table.write.partitionBy("year", "month").mode("overwrite").parquet(os.path.join(output_data, "time"))


def process_weather(input_data, output_data):
    """
    Loads weather data from API and performs required transformations 
    Writes output table to S3 in csv format
    
    Input:
    input_data - link to local source data
    output_data - link to target S3 bucket 
    
    Output:
    weather_table - hourly weather data from Chicago (dim table)
    """

    # Read in weather data file
    weather = pd.read_csv(os.path.join(input_data, "weather_2020.csv"))

    # Clean weather data
    weather_table = clean_weather(weather)

    # Data quality check: 365 days x 12 hours no of rows
    try:
        weather_table.hour.nunique() == weather_table.count()
        weather_table.hour.nunique() == 4380
        logging.info("Data quality check on weather table passed.")
    except Exception as e:
        logging.info("Data quality check on weather table failed.")
        print(e)

    # Save cleaned table as csv file to output path 
    weather_table.to_csv(os.path.join(output_data, "weather_cleaned.csv"))


def process_communities(input_data, output_data):
    """
    Loads communities data from local storage and performs required transformations 
    Writes output table to S3 in csv format
    
    Input:
    input_data - link to local source data
    output_data - link to target S3 bucket 
    
    Output:
    communities_table - community area data (dim table)
    """

    # Read in communities data files
    df1 = pd.read_csv(os.path.join(input_data, "areas.csv"), sep="\t")
    df2 = pd.read_csv(os.path.join(input_data, "communities_chicago.csv"), encoding="utf-16", sep="\t")

    # Clean and merge communities data
    communities_table = clean_communities(df1, df2)

    # Data quality check: 77 communities in the dataset
    try:
        communities_table.name.nunique() == 77
        logging.info("Data Quality check on communities table passed.")
    except Exception as e:
        logging.info("Data quality check on communities table failed. Incorrect number of community areas")
        print(e)

    # Save cleaned table as csv file to output path 
    communities_table.to_csv(os.path.join(output_data, "communities_cleaned.csv"))


def main():
    """
    Provides Spark session, input and output data links
    Processes all four datasets and loads transformed data into the data model
    """
    
    spark = create_spark_session()
    print(spark)
    input_data = "data/"
    #input_data = "s3a://dendbucketin/data/"
    ##uncomment to save parquet files locally
    output_data = "data/"
    ##uncomment to load parquet files to S3 | put in S3 bucket name
    #output_data = "s3a://dendbucketout/"
    
    process_trips(spark, input_data, output_data)    
    process_weather(input_data, output_data)
    process_communities(input_data, output_data)


if __name__ == "__main__":
    main()