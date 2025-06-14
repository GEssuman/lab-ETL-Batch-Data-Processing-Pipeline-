from pyspark.sql import SparkSession
import pyspark.sql.functions as F 
from pyspark.sql.types import *
import os
from datetime import datetime
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType


def create_spark_session():
    spark = SparkSession.builder \
            .appName("ApartmentRental") \
            .config("spark.jars", "/opt/spark/resources/postgresql-42.7.3.jar") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider") \
            .getOrCreate()
    return spark



def load_from_s3(spark, schema, s3_bucket, is_header):
    data = spark.read\
    .option("header", is_header)\
        .schema(schema)\
            .csv(s3_bucket)

            # .csv(f"s3a://{AWS_S3_BUCKET}/stream-data/")\
    
    return data

def load_to_s3(df, s3_bucket):
    df.coalesce(1).write.mode("append") \
        .format("csv") \
        .save(s3_bucket)


def load_data_to_db(df, db_info):
    df.write \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://rental_postgres:5432/{db_info["db_name"]}") \
    .option("dbtable", db_info["table"]) \
    .option("user", db_info["user"]) \
    .option("password", db_info["password"]) \
    .option("driver", db_info["driver"]) \
    .mode("append") \
    .save()




def validate_columns(df, required_columns):
    return set(required_columns).issubset(set(df.columns))



def read_from_db(spark, db_info):
    return spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://rental_postgres:5432/{db_info["db_name"]}") \
    .option("dbtable", db_info["table"]) \
    .option("user", db_info["user"]) \
    .option("password", db_info["password"]) \
    .option("driver", db_info["driver"]) \
    .load()

