from pyspark.sql import SparkSession

from pathlib import Path
from dotenv import load_dotenv
import os
import sys

from schemas import SCHEMAS

load_dotenv(Path(__file__).parent / ".env")
def connect_to_s3(schema_table_name: str, minio_csv_name: str):
    ENDPOINT_URL = os.getenv("ENDPOINT_URL")
    ACCESS_KEY = os.getenv("ACCESS_KEY")
    SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")

    POSTGRES_DB = os.getenv("POSTGRES_DB")
    SCHEMA_TABLE_NAME = schema_table_name
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    GP_URL = os.getenv("GP_URL") + "/" + POSTGRES_DB
    DRIVER = "org.postgresql.Driver"

    spark = SparkSession.builder.master("local[*]").appName("save_file") \
        .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT_URL)\
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)\
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_ACCESS_KEY)\
        .config("spark.hadoop.fs.s3a.path.style.access", "true").getOrCreate()

    schema = SCHEMAS[schema_table_name]
    df = spark.read.option("header", "true").schema(schema).csv(f"s3a://raw/{minio_csv_name}.csv").coalesce(1)

    df.write.format("jdbc")\
        .option("url", GP_URL)\
        .option("dbtable", SCHEMA_TABLE_NAME)\
        .option("user", POSTGRES_USER)\
        .option("password", POSTGRES_PASSWORD)\
        .option("driver", DRIVER)\
        .option("batchsize", 2000) \
        .option("reWriteBatchedInserts", "true") \
        .mode("append").save()
    
    spark.stop()

connect_to_s3(sys.argv[1], sys.argv[2])
