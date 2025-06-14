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






curated_booked_listings_df = glueContext.create_dynamic_frame.from_catalog(
        database="apartment_rental_db",
        table_name="intermediated_listings"
    ).toDF()
    
    
curated_booked_listings_df.createOrReplaceTempView("apartment_list_tb")

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

# Convert back to DynamicFrame
total_bookings_per_user_dyf = DynamicFrame.fromDF(total_bookings_per_user, glueContext, "listings_dyf")


# Write to S3 Presentationn
glueContext.write_dynamic_frame.from_options(
    frame=total_bookings_per_user_dyf,
    connection_type="s3",
    connection_options={"path": "s3://apartment-rental-db-gke.amalitech/presentation/total_bookings_per_user/"},
    format="csv",
    format_options={"writeHeader": True},
    transformation_ctx="write_presentation"
)

job.commit()










