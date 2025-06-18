from pyspark.sql import SparkSession
import pyspark.sql.functions as F 
from pyspark.sql.types import *
import os
import sys
from datetime import datetime
from awsglue.utils import getResolvedOptions
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, BooleanType
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)




# Write to S3 Presentationn
curated_booked_listings_df = glueContext.create_dynamic_frame.from_options(
    connection_type="redshift",
    connection_options = {
        "url": "jdbc:redshift://my-redshift-wg.309797288544.eu-north-1.redshift-serverless.amazonaws.com:5439/apartment_rental_db", 
        "user": "admin", 
        "password": "IYSEdsvo469+",
        "dbtable": "curated.apartment_bookings", 
        "redshiftTmpDir":"s3://aws-glue-assets-309797288544-eu-north-1/temporary/"
    } 
).toDF()




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
    F.col("is_active").cast("boolean"),
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

occupancy_rate_summary = occupancy_rate_summary.select(
    F.col("month").cast("date"),
    F.col("total_bookings").cast("int"),
    F.col("total_booked_nights").cast("int"),
    F.col("occupancy_rate_percent").cast("double")
)


most_popular_location = most_popular_location.select(
    F.col("week").cast("date"),
    F.col("cityname").cast("string"),
    F.col("total_bookings").cast("int")
)

top_performing_listing = top_performing_listing.select(
    F.col("week").cast("date"),
    F.col("apartment_id").cast("int"),
    F.col("weekly_revenue").cast("double")
)

total_bookings_per_user = total_bookings_per_user.select(
    F.col("user_id").cast("int"),
    F.col("total_bookings").cast("int")
)

average_booking_duration = average_booking_duration.select(
    F.col("month").cast("date"),
    F.col("avg_booking_duration_days").cast("double")
)

repeat_customer_rate = repeat_customer_rate.select(
    F.col("month").cast("date"),
    F.col("num_repeated_customers").cast("int"),
    F.col("total_customers").cast("int"),
    F.col("repeat_customer_rate_percent").cast("double")
)

# Convert back to DynamicFrame
occupancy_rate_summary = DynamicFrame.fromDF(occupancy_rate_summary, glueContext, "occupancy_rate_summary")
most_popular_location = DynamicFrame.fromDF(most_popular_location, glueContext, "most_popular_location")
top_performing_listing = DynamicFrame.fromDF(top_performing_listing, glueContext, "top_performing_listing")
total_bookings_per_user = DynamicFrame.fromDF(total_bookings_per_user, glueContext, "total_bookings_per_user")
average_booking_duration = DynamicFrame.fromDF(average_booking_duration, glueContext, "average_booking_duration")
repeat_customer_rate = DynamicFrame.fromDF(repeat_customer_rate, glueContext, "repeat_customer_rate")


glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=occupancy_rate_summary, 
    catalog_connection="Redshift Connection", 
    connection_options={
        "dbtable": "presentation.occupancy_rate_per_month", 
        "database":"apartment_rental_db",
    },
    redshift_tmp_dir= "s3://aws-glue-assets-309797288544-eu-north-1/temporary/", 
)

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=most_popular_location, 
    catalog_connection="Redshift Connection", 
    connection_options={
        "dbtable": "presentation.popular_cities_per_week", 
        "database":"apartment_rental_db",
    },
    redshift_tmp_dir= "s3://aws-glue-assets-309797288544-eu-north-1/temporary/", 
)

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=top_performing_listing, 
    catalog_connection="Redshift Connection", 
    connection_options={
        "dbtable": "presentation.top_listings_weekly_revenue", 
        "database":"apartment_rental_db",
    },
    redshift_tmp_dir= "s3://aws-glue-assets-309797288544-eu-north-1/temporary/", 
)

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=total_bookings_per_user, 
    catalog_connection="Redshift Connection", 
    connection_options={
        "dbtable": "presentation.total_bookings_per_user", 
        "database":"apartment_rental_db",
    },
    redshift_tmp_dir= "s3://aws-glue-assets-309797288544-eu-north-1/temporary/", 
)

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=average_booking_duration, 
    catalog_connection="Redshift Connection", 
    connection_options={
        "dbtable": "presentation.avg_booking_duration_per_month", 
        "database":"apartment_rental_db",
    },
    redshift_tmp_dir= "s3://aws-glue-assets-309797288544-eu-north-1/temporary/", 
)

glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=repeat_customer_rate, 
    catalog_connection="Redshift Connection", 
    connection_options={
        "dbtable": "presentation.repeat_customer_rate_per_month", 
        "database":"apartment_rental_db",
    },
    redshift_tmp_dir= "s3://aws-glue-assets-309797288544-eu-north-1/temporary/", 
)


job.commit()










