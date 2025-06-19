import sys
from pyspark.sql import functions as F
from pyspark.sql.types import *
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame
import logging


logger = logging.getLogger()
logger.setLevel(logging.INFO)


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)



# Define schemas

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
    StructField("viewed_at", StringType(), False),
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

def get_data_from_s3(spark_session, s3_bucket, schema, format="csv", header=True, ctx_name=""):
    return spark_session.read.option("header", header).schema(schema).csv(s3_bucket).dropDuplicates()

try:
    logger.info("Loading data from S3...")
    apartment_attributes_df = get_data_from_s3(
        spark,
        "s3://apartment-rental-db-gke.amalitech/raw_data/apartment_attributes/",
        apartment_attributes_schema
        )
    
    user_viewing_df = get_data_from_s3(
        spark,
        "s3://apartment-rental-db-gke.amalitech/raw_data/user_viewing/",
        user_viewing_schema
        )
    
    apartments_df = get_data_from_s3(
        spark, 
        "s3://apartment-rental-db-gke.amalitech/raw_data/apartments/",
        apartments_schema
        )
    bookings_df = get_data_from_s3(
        spark, 
        "s3://apartment-rental-db-gke.amalitech/raw_data/bookings/",
        bookings_schema
        )
    
    logger.info("All datasets loaded successfully.")
except Exception as e:
    logger.error("Failed to load one or more datasets.", exc_info=True)
    raise e
try:
    logger.info("Applying transformations...")
    # Clean and format date fields
    user_viewing_df = user_viewing_df.withColumn("viewed_at", F.date_format(F.to_date("viewed_at", "dd/MM/yyyy"), "yyyy-MM-dd"))
    apartments_df = apartments_df.withColumn("listing_created_on", F.date_format(F.to_date("listing_created_on", "dd/MM/yyyy"), "yyyy-MM-dd")) \
                                 .withColumn("last_modified_timestamp", F.date_format(F.to_date("last_modified_timestamp", "dd/MM/yyyy"), "yyyy-MM-dd"))
    bookings_df = bookings_df.withColumn("booking_date", F.date_format(F.to_date("booking_date", "dd/MM/yyyy"), "yyyy-MM-dd")) \
                             .withColumn("checkin_date", F.date_format(F.to_date("checkin_date", "dd/MM/yyyy"), "yyyy-MM-dd")) \
                             .withColumn("checkout_date", F.date_format(F.to_date("checkout_date", "dd/MM/yyyy"), "yyyy-MM-dd"))
    
    
    
    
    
    # Add exchange rates
    exchange_rates_df = spark.createDataFrame([
        ("USD", 1.0),
        ("EUR", 1.1),
        ("INR", 0.012),
    ], ["currency", "usd_rate"])
    
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
        F.col("listing_created_on"),
        F.col("is_active").cast("boolean"),
        F.col("source").cast("string"),
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
    
    
    listings_df = (
        bookings_df.alias("b")
        .join(apartments_df.alias("a"), F.col("b.apartment_id") == F.col("a.id"), "left")
        .join(apartment_attributes_df.alias("attr"), F.col("a.id") == F.col("attr.id"), "left")
    )
    
    
    
    listings_df = listings_df.join(exchange_rates_df, on="currency", how="left")
    listings_df = listings_df.withColumn("total_price_usd", F.col("total_price") * F.col("usd_rate"))
    
    listings_df = listings_df.drop("a.id","a.price_usd", "usd_rate", "total_price", "usd_rate")
    
    
    
    
    
    
    
    # SQL for presentation metrics
    apartments_df.createOrReplaceTempView("apartment_list_tb")
    
    
    avg_listing_price = spark.sql("""
        SELECT
            DATE_TRUNC('week', listing_created_on) AS week_start,
            AVG(price_usd) AS average_price
        FROM apartment_list_tb
        GROUP BY week_start
    """)
    
    
    avg_listing_price = avg_listing_price.select(
        F.col("week_start").cast("date"),
        F.col("average_price").cast("double").alias("average_price")
    )
    
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
    
    # Convert back to DynamicFrame
    listings_dyf = DynamicFrame.fromDF(listings_df, glueContext, "listings_dyf")
    avg_price_dyf = DynamicFrame.fromDF(avg_listing_price, glueContext, "avg_price_dyf")
    logger.info("Transformations Completed Successfully...")
except Exception as e:
    logger.error("Transformation step failed.", exc_info=True)
    raise e

try:
    logger.info("Writing curated data to Redshift (curated.apartment_bookings)...")
    # Write to S3 (Curated and Presentation)
    # Write Most Popular Track per Genre to Redshift
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=listings_dyf, 
        catalog_connection="Redshift Connection", 
        connection_options={
            "dbtable": "curated.apartment_bookings", 
            "database":"apartment_rental_db",
        },
        redshift_tmp_dir= "s3://aws-glue-assets-309797288544-eu-north-1/temporary/", 
    )
    
    logger.info("Writing presentation data to Redshift (presentation.average_listing_price)...")

    # Write Most Popular Track per Genre to Redshift
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=avg_price_dyf, 
        catalog_connection="Redshift Connection",
        connection_options={
            "dbtable": "presentation.average_listing_price", 
            "database":"apartment_rental_db",
        },
        redshift_tmp_dir= "s3://aws-glue-assets-309797288544-eu-north-1/temporary/", 
    )
except Exception as e:
    logger.error("Failed to write data to Redshift.", exc_info=True)
    raise e
    
try:
    job.commit()
    logger.info("Glue job committed successfully.")
except Exception as e:
    logger.error("Glue job commit failed.", exc_info=True)
    raise e
