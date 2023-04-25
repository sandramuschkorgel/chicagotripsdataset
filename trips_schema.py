from pyspark.sql.types import *

class Schema:
    trips_schema = StructType([
        StructField("trip_id", StringType()),
        StructField("trip_start_timestamp", TimestampType()),
        StructField("trip_end_timestamp", TimestampType()),
        StructField("trip_seconds", StringType()),
        StructField("trip_miles", StringType()),
        StructField("pickup_census_tract", StringType()),
        StructField("dropoff_census_tract", StringType()),
        StructField("pickup_community_area", StringType()),
        StructField("dropoff_community_area", StringType()),
        StructField("fare", StringType()),
        StructField("tip", StringType()),
        StructField("additional_charges", StringType()),
        StructField("trip_total", StringType()),
        StructField("shared_trip_authorized", BooleanType()),
        StructField("trips_pooled", StringType()),
        StructField("pickup_centroid_latitude", StringType()),
        StructField("pickup_centroid_longitude", StringType()),
        StructField("pickup_centroid_location", StructType([
            StructField("type", StringType()), 
            StructField("coordinates", ArrayType(DoubleType(), True))
        ])),
        StructField("dropoff_centroid_latitude", StringType()),
        StructField("dropoff_centroid_longitude", StringType()),
        StructField("dropoff_centroid_location", StructType([
            StructField("type", StringType()), 
            StructField("coordinates", ArrayType(DoubleType(), True))
        ]))
    ])