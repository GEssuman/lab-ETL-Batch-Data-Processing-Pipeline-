from pyspark.sql import SparkSession
import pyspark.sql.functions as F 
from pyspark.sql.types import *
import os
from datetime import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, BooleanType

from helper_func import read_from_db


##Load env vairables
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
AWS_S3_BUCKET = os.getenv("AWS_S3_BUCKET")

apartment_attributes_schema = StructType([
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


user_viewing_schema = StructType([
    StructField("user_id", IntegerType(), False),
    StructField("apartment_id", IntegerType(), False),
    StructField("viewed_at", StringType(), False),  # Can use TimestampType() if parsed later
    StructField("is_wishlisted", BooleanType(), True),
    StructField("call_to_action", StringType(), True)
])


apartments_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("title", StringType(), True),
    StructField("source", StringType(), True),
    StructField("price", DecimalType(6, 2), True),
    StructField("currency", StringType(), True),
    StructField("listing_created_on", StringType(), True),  # Or TimestampType()
    StructField("is_active", BooleanType(), True),
    StructField("last_modified_timestamp", StringType(), True)  # Or TimestampType()
])


bookings_schema = StructType([
    StructField("booking_id", IntegerType(), False),
    StructField("user_id", IntegerType(), True),
    StructField("apartment_id", IntegerType(), True),
    StructField("booking_date", StringType(), True),  # Or TimestampType()
    StructField("checkin_date", StringType(), True),
    StructField("checkout_date", StringType(), True),
    StructField("total_price", DecimalType(7, 2), True),
    StructField("currency", StringType(), True),
    StructField("booking_status", StringType(), True)
])

spark = SparkSession.builder \
            .appName("MusicStreamingETL") \
            .config("spark.jars", "/opt/spark/resources/postgresql-42.7.3.jar") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider") \
            .getOrCreate()


# #Read from s3 bucket
# apartment_attributes_df = spark.read.schema(apartment_attributes_schema).format('csv').load()
# user_viewing_df = spark.read.schema(user_viewing_schema).format('csv').load()
# apartments_df = spark.read.schema(apartments_schema).format('csv').load()
# bookings_df_usd = spark.read.schema(bookings_schema).format('csv').load()


apartment_attributes_properties = {
        "db_name":"apartment_rental_db",
        "user": "dataeng_user",
        "password": "de_us$123",
        "driver": "org.postgresql.Driver",
        "table":"raw.apartment_attributes"
    }


user_viewing_properties = {
        "db_name":"apartment_rental_db",
        "user": "dataeng_user",
        "password": "de_us$123",
        "driver": "org.postgresql.Driver",
        "table":"raw.user_viewing"
    }

apartments_properties = {
        "db_name":"apartment_rental_db",
        "user": "dataeng_user",
        "password": "de_us$123",
        "driver": "org.postgresql.Driver",
        "table":"raw.apartments"
    }


bookings_properties = {
        "db_name":"apartment_rental_db",
        "user": "dataeng_user",
        "password": "de_us$123",
        "driver": "org.postgresql.Driver",
        "table":"raw.bookings"
    }


exchange_rates_df = spark.createDataFrame([
    ("USD", 1.0),
    ("EUR", 1.1),   # 1 EUR = 1.1 USD
    ("INR", 0.012),   # 1 INR = 0.012 USD
], ["currency", "usd_rate"])


# Read from db
apartment_attributes_df = read_from_db(spark, apartment_attributes_properties)
user_viewing_df = read_from_db(spark, user_viewing_properties)
apartments_df = read_from_db(spark, apartments_properties)
bookings_df = read_from_db(spark, bookings_properties)

apartment_attributes_df = apartment_attributes_df.dropDuplicates()
user_viewing_df = user_viewing_df.dropDuplicates()
apartments_df = apartments_df.dropDuplicates()
bookings_df = bookings_df.dropDuplicates()


# Clean date fields
user_viewing_df = user_viewing_df.withColumn("viewed_at", F.date_format(F.to_date("viewed_at", "dd/MM/yyyy"), "yyyy-MM-dd"))
apartments_df = apartments_df.withColumn("listing_created_on", F.date_format(F.to_date("listing_created_on", "dd/MM/yyyy"),"yyyy-MM-dd")) \
                             .withColumn("last_modified_timestamp", F.date_format(F.to_date("last_modified_timestamp", "dd/MM/yyyy"),"yyyy-MM-dd"))
bookings_df = bookings_df.withColumn("booking_date", F.date_format(F.to_date("booking_date", "dd/MM/yyyy"), "yyyy-MM-dd")) \
                         .withColumn("checkin_date", F.date_format(F.to_date("checkin_date", "dd/MM/yyyy"),"yyyy-MM-dd")) \
                         .withColumn("checkout_date", F.date_format(F.to_date("checkout_date", "dd/MM/yyyy"), "yyyy-MM-dd"))

apartment_attributes_df = apartment_attributes_df.select(
    F.col("id").cast("int"),
    F.col("category").cast("string"),
    F.col("body").cast("string"),
    F.col("cityname").cast("string"),
    F.col("state").cast("string")
)


apartments_df = apartments_df.join(exchange_rates_df, on="currency", how="left")
apartments_df = apartments_df.withColumn("price_usd", F.col("price") * F.col("usd_rate"))


apartments_df  = apartments_df.select(
    F.col("id").cast("int"),
    F.col("title").cast("string"),
    F.col("source").cast("string"),
    F.col("listing_created_on"),
    F.col("is_active").cast("boolean"),
    F.col("price_usd").cast("double")
)

bookings_df = bookings_df.select(
    F.col("booking_id").cast("int"),
    F.col("apartment_id").cast("int"),
    F.col("user_id").cast("string"),
    F.col("booking_date"),
    F.col("checkin_date"),
    F.col("checkout_date"),
    F.col("total_price").cast("double"),
    F.col("currency").cast("string"),
    F.col("booking_status").cast("string")
)

# SQL for presentation metrics
apartments_df.createOrReplaceTempView("apartment_list_tb")
avg_listing_price = spark.sql("""
    SELECT
        DATE_TRUNC('week', listing_created_on) AS week_start,
        AVG(price_usd) AS avg_price_usd
    FROM apartment_list_tb
    GROUP BY week_start
""")


listings_df = (
    bookings_df.alias("b")
    .join(apartments_df.alias("a"), F.col("b.apartment_id") == F.col("a.id"), "left")
    .join(apartment_attributes_df.alias("attr"), F.col("a.id") == F.col("attr.id"), "left")
)



listings_df = listings_df.join(exchange_rates_df, on="currency", how="left")
listings_df = listings_df.withColumn("total_price_usd", F.col("total_price") * F.col("usd_rate"))

listings_df = listings_df.select(
    F.col("booking_id").cast("int"),
    F.col("apartment_id").cast("int"),
    F.col("user_id").cast("int"),
    F.col("category").cast("string"),
    F.col("body").cast("string"),
    F.col("cityname").cast("string"),
    F.col("state").cast("string"),
    F.col("title").cast("string"),
    F.col("source").cast("string"),
    F.col("listing_created_on").cast("date"),
    F.col("is_active").cast("boolean"),
    F.col("booking_date").cast("date"),
    F.col("checkin_date").cast("date"),
    F.col("checkout_date").cast("date"),
    F.col("booking_status").cast("string"),
    F.col("total_price_usd").cast("double")
)




listings_df.show()

avg_listing_price.show()



listings_df.write \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://rental_postgres:5432/apartment_rental_db") \
    .option("dbtable", "curated.apartment_bookings") \
    .option("user","dataeng_user") \
    .option("password", "de_us$123") \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

