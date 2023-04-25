from pyspark.sql.functions import col, dayofmonth, dayofweek, month, year
from pyspark.sql.types import *


def clean_trips(df):
    """
    Changes datatypes according to the data model
    Adds partitioning columns

    Input:
    df - PySpark dataframe

    Output:
    df_clean - cleaned PySpark datframe
    """
    
    df = df.withColumn("trip_seconds", df["trip_seconds"].cast(IntegerType())) \
        .withColumn("pickup_community_area", df["pickup_community_area"].cast(IntegerType())) \
        .withColumn("dropoff_community_area", df["dropoff_community_area"].cast(IntegerType())) \
        .withColumn("trips_pooled", df["trips_pooled"].cast(IntegerType())) \
        .withColumn("trip_miles", df["trip_miles"].cast(FloatType())) \
        .withColumn("fare", df["fare"].cast(FloatType())) \
        .withColumn("tip", df["tip"].cast(FloatType())) \
        .withColumn("additional_charges", df["additional_charges"].cast(FloatType())) \
        .withColumn("trip_total", df["trip_total"].cast(FloatType())) \
        .withColumn("pickup_centroid_latitude", df["pickup_centroid_latitude"].cast(DoubleType())) \
        .withColumn("pickup_centroid_longitude", df["pickup_centroid_longitude"].cast(DoubleType())) \
        .withColumn("dropoff_centroid_latitude", df["dropoff_centroid_latitude"].cast(DoubleType())) \
        .withColumn("dropoff_centroid_longitude", df["dropoff_centroid_longitude"].cast(DoubleType())) 
    
    # Add year, month, day and day of week as columns
    df_clean = df.withColumn("year", year(col("trip_start_timestamp"))) \
        .withColumn("month", month(col("trip_start_timestamp"))) \
        .withColumn("day", dayofmonth(col("trip_start_timestamp"))) \
        .withColumn("dayofweek", dayofweek(col("trip_start_timestamp"))) \

    return df_clean