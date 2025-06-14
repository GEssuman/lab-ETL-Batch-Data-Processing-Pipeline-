from pyspark.sql import SparkSession
import pyspark.sql.functions as F 
from pyspark.sql.types import *
import os
from datetime import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, BooleanType

from helper_func import *


apartment_attributes_properties = {
        "db_name":"apartment_rental_db",
        "user": "dataeng_user",
        "password": "de_us$123",
        "driver": "org.postgresql.Driver",
        "table":"curated.listing_info"
    }


# ##Load env vairables
# POSTGRES_DB = os.getenv("POSTGRES_DB")
# POSTGRES_USER = os.getenv("POSTGRES_USER")
# POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
# AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")

curated_booked_listings_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("category", StringType(), True),
    StructField("body", StringType(), True),
    StructField("amenities", StringType(), True),  # Stored as text; if parsed as array, use ArrayType(StringType())
    StructField("bathrooms", IntegerType(), True),
    StructField("bedrooms", IntegerType(), True),
    StructField("fee", DecimalType(5, 2), True),
    StructField("has_photo", BooleanType(), True),
    StructField("pets_allowed", BooleanType(), True),
    StructField("price_display", StringType(), True),
    StructField("price_type", StringType(), True),
    StructField("square_feet", IntegerType(), True),
    StructField("address", StringType(), True),
    StructField("cityname", StringType(), True),
    StructField("state", StringType(), True),
    StructField("latitude", DecimalType(9, 6), True),
    StructField("longitude", DecimalType(9, 6), True)
])



spark = SparkSession.builder \
            .appName("MusicStreamingETL") \
            .config("spark.jars", "/opt/spark/resources/postgresql-42.7.3.jar") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider") \
            .getOrCreate()


# curated_booked_listings_df = spark.read.schema(curated_booked_listings_schema)\
#     .option("header", "true")\
#     .csv("s3a://your-bucket-name/raw/apartment_attributes.csv")
curated_booked_listings_df = read_from_db(spark, apartment_attributes_properties)

curated_booked_listings_df.createOrReplaceTempView("apartment_list_tb")

# occupancy_rate_summary = spark.sql("""
# WITH booking_nights AS (
#     SELECT
#         apartment_id,
#         DATE_TRUNC('month', checkin_date) AS month,
#         DATEDIFF(checkout_date, checkin_date) AS nights_booked
#     FROM apartment_list_tb
#     WHERE booking_status = 'confirmed'
# ),
# monthly_booked_nights AS (
#     SELECT
#         month,
#         SUM(nights_booked) AS total_booked_nights
#     FROM booking_nights
#     GROUP BY month
# ),
# apartment_months AS (
#     SELECT
#         DATE_TRUNC('month', d) AS month,
#         COUNT(DISTINCT id) AS apartment_count
#     FROM apartments a,
#          LATERAL (SELECT sequence(to_date('2020-01-01'), current_date, interval 1 day) AS days) AS seq,
#          LATERAL VIEW explode(seq.days) AS d
#     WHERE DATE_TRUNC('month', d) BETWEEN '2020-01-01' AND current_date
#     GROUP BY DATE_TRUNC('month', d)
# ),
# available_nights AS (
#     SELECT
#         month,
#         apartment_count * DAY(LAST_DAY(month)) AS total_available_nights
#     FROM apartment_months
# )
# SELECT
#     a.month,
#     ROUND(COALESCE(b.total_booked_nights, 0) * 100.0 / a.total_available_nights, 2) AS occupancy_rate_percentage
# FROM available_nights a
# LEFT JOIN monthly_booked_nights b ON a.month = b.month
# ORDER BY a.month;


# """)

# most_popular_location = spark.sql("""

# """)

# top_performing_listing = spark.sql("""""")

total_bookings_per_user = spark.sql("""
SELECT
    user_id,
    COUNT(*) AS total_bookings
FROM
    apartment_list_tb
WHERE
    booking_status = 'confirmed'
GROUP BY user_id
""")

# average_booking_duration = spark.sql("""""")

# repeat_customer_rate = spark.sql("""""")



total_bookings_per_user.show()









apartments_with_usd.createOrReplaceTempView("apartment_list_tb")

