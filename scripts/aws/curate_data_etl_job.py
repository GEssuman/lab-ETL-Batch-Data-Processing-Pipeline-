import sys
from pyspark.sql import functions as F
from pyspark.sql.types import *
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.dynamicframe import DynamicFrame

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



apartment_attributes_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://apartment-rental-db-gke.amalitech/raw_data/apartment_attributes/"], "recurse": True},
    format_options={"withHeader": True},
    transformation_ctx="apartment_attributes_dyf"
)
apartment_attributes_df = apartment_attributes_dyf.toDF().dropDuplicates()

user_viewing_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://apartment-rental-db-gke.amalitech/raw_data/user_viewing/"]},
    format_options={"withHeader": True},
    transformation_ctx="user_viewing_dyf"
)
user_viewing_df = user_viewing_dyf.toDF().dropDuplicates()

apartments_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://apartment-rental-db-gke.amalitech/raw_data/apartments/"]},
    format_options={"withHeader": True},
    transformation_ctx="apartments_dyf"
)
apartments_df = apartments_dyf.toDF().dropDuplicates()

bookings_dyf = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://apartment-rental-db-gke.amalitech/raw_data/bookings/"]},
    format_options={"withHeader": True},
    transformation_ctx="bookings_dyf"
)
bookings_df = bookings_dyf.toDF().dropDuplicates()

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

# Join with currency rates
apartments_df = apartments_df.join(exchange_rates_df, on="currency", how="left")
bookings_df = bookings_df.join(exchange_rates_df, on="currency", how="left")

apartments_df = apartments_df.withColumn("price_usd", F.col("price") * F.col("usd_rate"))
bookings_df = bookings_df.withColumn("total_price_usd", F.col("total_price") * F.col("usd_rate"))

# Drop unnecessary fields
user_viewing_df = user_viewing_df.drop("viewed_at", "is_wishlisted")
apartments_cur_df = apartments_df.drop("last_modified_timestamp", "currency", "price") \
                                 .select("id", "title", "source", "is_active", "listing_created_on", "price_usd")
bookings_df = bookings_df.drop("booking_status", "currency", "total_price")

apartment_attributes_df = apartment_attributes_df.drop("category", "body", "fee", "pets_allowed", "state",
                                                       "square_feet", "has_photo", "address", "latitude", "longitude")

# SQL for presentation metrics
apartments_df.createOrReplaceTempView("apartment_list_tb")
avg_listing_price = spark.sql("""
    SELECT
        DATE_TRUNC('week', listing_created_on) AS week_start,
        AVG(price_usd) AS avg_price
    FROM apartment_list_tb
    GROUP BY week_start
""")

# Join to build curated listings
listings_info = bookings_df.join(apartments_cur_df, bookings_df["apartment_id"] == apartments_cur_df["id"], "left") \
                           .join(apartment_attributes_df, apartments_cur_df["id"] == apartment_attributes_df["id"], "left").drop("id")

# Convert back to DynamicFrame
listings_dyf = DynamicFrame.fromDF(listings_info, glueContext, "listings_dyf")
avg_price_dyf = DynamicFrame.fromDF(avg_listing_price, glueContext, "avg_price_dyf")

# Write to S3 (Curated and Presentation)
glueContext.write_dynamic_frame.from_options(
    frame=listings_dyf,
    connection_type="s3",
    connection_options={"path": "s3://apartment-rental-db-gke.amalitech/curated/intermediated_listings/"},
    format="csv",
    format_options={"writeHeader": True},
    transformation_ctx="write_curated"
)

glueContext.write_dynamic_frame.from_options(
    frame=avg_price_dyf,
    connection_type="s3",
    connection_options={"path": "s3://apartment-rental-db-gke.amalitech/presentation/average_weekly_listings/"},
    format="csv",
    format_options={"writeHeader": True},
    transformation_ctx="write_presentation"
)

job.commit()
