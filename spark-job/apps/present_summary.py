from pyspark.sql import SparkSession
import pyspark.sql.functions as F 
from pyspark.sql.types import *
import os
from datetime import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, BooleanType, DateType

from helper_func import *

##Load env vairables
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")

apartment_attributes_properties = {
        "db_name":POSTGRES_DB,
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
        "table":"curated.apartment_bookings"
    }



curated_booked_listings_schema = StructType([
    StructField("booking_id", IntegerType(), False),
    StructField("apartment_id", StringType(), True),  # Stored as text; if parsed as array, use ArrayType(StringType())
    StructField("user_id", IntegerType(), True),
    StructField("category", StringType(), True),
    StructField("body", StringType(), True),
    StructField("cityname", StringType(), True),
    StructField("state", StringType(), True),
    StructField("title", StringType(), True),
    StructField("source", StringType(), True),
    StructField("listing_created_on", DateType()),
    StructField("is_active", BooleanType(), True),
    StructField("booking_date", DateType()),
    StructField("checkin_date", DateType()),
    StructField("checkout_date", DateType()),
    StructField("booking_status", StringType(), True),
    StructField("total_price_usd", DecimalType(9, 6), True)
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

curated_booked_listings_df = curated_booked_listings_df.select(
    F.col("booking_id").cast("int"),
    F.col("apartment_id").cast("int"),
    F.col("user_id").cast("int"),
    F.col("category").cast("string"),
    F.col("body").cast("string"),
    F.col("cityname").cast("string"),
    F.col("state").cast("string"),
    F.col("title").cast("string"),
    F.col("source").cast("string"),
    F.col("listing_created_on").cast(DateType()),
    F.col("is_active").cast("string"),
    F.col("booking_date").cast(DateType()),
    F.col("checkin_date").cast(DateType()),
    F.col("checkout_date").cast(DateType()),
    F.col("booking_status").cast("string"),
    F.col("total_price_usd").cast("double")
)

curated_booked_listings_df.createOrReplaceTempView("apartment_list_tb")

occupancy_rate_summary = spark.sql("""
SELECT
    DATE_TRUNC('month', checkin_date) AS month,
    COUNT(*) AS total_bookings,
    SUM(DATEDIFF(checkout_date, checkin_date)) AS total_booked_nights,
    ROUND(
        SUM(DATEDIFF(checkout_date, checkin_date)) * 100.0 / 
        (
            COUNT(DISTINCT apartment_id) * 
            DAY(LAST_DAY(ANY_VALUE(checkin_date)))
        ), 2
    ) AS occupancy_rate_percent
FROM apartment_list_tb
WHERE booking_status = 'confirmed'
GROUP BY DATE_TRUNC('month', checkin_date)
ORDER BY month;
""")

most_popular_location = spark.sql("""
SELECT
    DATE_TRUNC('week', booking_date) AS week,
    cityname,
    COUNT(*) AS total_bookings
FROM apartment_list_tb
WHERE booking_status = 'confirmed'
GROUP BY 1, 2
ORDER BY 1, total_bookings DESC;
""")

top_performing_listing = spark.sql("""
SELECT
    DATE_TRUNC('week', booking_date) AS week,
    apartment_id,
    SUM(total_price_usd) AS weekly_revenue
FROM apartment_list_tb
WHERE booking_status = 'confirmed'
GROUP BY 1, 2
ORDER BY 1, weekly_revenue DESC;
""")

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

average_booking_duration = spark.sql("""
SELECT
    DATE_TRUNC('month', checkin_date) AS month,
    ROUND(AVG(DATEDIFF(checkout_date, checkin_date)), 2) AS avg_booking_duration_days
FROM apartment_list_tb
WHERE booking_status = 'confirmed'
GROUP BY 1
ORDER BY 1;
""")

repeat_customer_rate = spark.sql("""
WITH confirmed_bookings AS (
    SELECT
        user_id,
        booking_date,
        LAG(booking_date) OVER (PARTITION BY user_id ORDER BY booking_date) AS prev_booking_date
    FROM apartment_list_tb
    WHERE booking_status = 'confirmed'
),

repeat_flags AS (
    SELECT
        user_id,
        booking_date,
        prev_booking_date,
        CASE
            WHEN prev_booking_date IS NOT NULL
                 AND DATEDIFF(booking_date, prev_booking_date) <= 30 THEN 1
            ELSE 0
        END AS is_repeat
    FROM confirmed_bookings
),

monthly_repeat_rate AS (
    SELECT
        DATE_TRUNC('month', booking_date) AS month,
        COUNT(DISTINCT CASE WHEN is_repeat = 1 THEN user_id END) AS num_repeated_customers,
        COUNT(DISTINCT user_id) AS total_customers,
        ROUND(
            COUNT(DISTINCT CASE WHEN is_repeat = 1 THEN user_id END) * 100.0 /
            COUNT(DISTINCT user_id),
            2
        ) AS repeat_customer_rate_percent
    FROM repeat_flags
    GROUP BY 1
    ORDER BY 1
)

SELECT * FROM monthly_repeat_rate;
""")

occupancy_rate_summary_properties = {
        "db_name":POSTGRES_DB,
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
        "table":"presentation.occupancy_rate_per_month"
    }
most_popular_location_properties = {
        "db_name":POSTGRES_DB,
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
        "table":"presentation.popular_cities_per_week"
    }
top_performing_listing_properties = {
        "db_name":POSTGRES_DB,
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
        "table":"presentation.top_listings_weekly_revenue"
    }
total_bookings_per_user_properties = {
        "db_name":POSTGRES_DB,
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
        "table":"presentation.total_bookings_per_user"
    }
average_booking_duration_properties = {
        "db_name":POSTGRES_DB,
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
        "table":"presentation.avg_booking_duration_per_month"
    }
repeat_customer_rate_properties = {
        "db_name":POSTGRES_DB,
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": "org.postgresql.Driver",
        "table":"presentation.repeat_customer_rate_per_month"
    }

load_data_to_db(occupancy_rate_summary, occupancy_rate_summary_properties)
load_data_to_db(most_popular_location, most_popular_location_properties)
load_data_to_db(top_performing_listing, top_performing_listing_properties)
load_data_to_db(total_bookings_per_user, total_bookings_per_user_properties)
load_data_to_db(average_booking_duration, apartment_attributes_properties)
load_data_to_db(repeat_customer_rate,repeat_customer_rate_properties)
# occupancy_rate_summary.show()
# most_popular_location.show()
# top_performing_listing.show()
# total_bookings_per_user.show()
# average_booking_duration.show()
# repeat_customer_rate.show()











