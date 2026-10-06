from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DateType

from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv(Path(__file__).parent / ".env")
ENDPOINT_URL = os.getenv("ENDPOINT_URL")
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")

POSTGRES_DB = os.getenv("POSTGRES_DB")
TABLE_NAME = os.getenv("TABLE_NAME")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
GP_URL = os.getenv("GP_URL") + "/" + POSTGRES_DB
DRIVER = "org.postgresql.Driver"

def connect_to_s3(): # 7 мин загружалось - это не дело, надо что-то придумать и мб переделать под остальные табл (желаетльно без DRY)
    spark = SparkSession.builder.master("local[*]").appName("save_file") \
        .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT_URL)\
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)\
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_ACCESS_KEY)\
        .config("spark.hadoop.fs.s3a.path.style.access", "true").getOrCreate()

    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("order_purchase_timestamp", TimestampType(), True),
        StructField("order_approved_at", TimestampType(), True),
        StructField("order_delivered_carrier_date", TimestampType(), True),
        StructField("order_delivered_customer_date", TimestampType(), True),
        StructField("order_estimated_delivery_date", DateType(), True)
    ])

    df = spark.read.option("header", "true").schema(schema).csv("s3a://raw/olist_orders_dataset.csv")

    df.write.format("jdbc")\
        .option("url", GP_URL)\
        .option("dbtable", TABLE_NAME)\
        .option("user", POSTGRES_USER)\
        .option("password", POSTGRES_PASSWORD)\
        .option("driver", DRIVER)\
        .mode("overwrite").save()


connect_to_s3()