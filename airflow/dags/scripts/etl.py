from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv(Path(__file__).parent / ".env")
ENDPOINT_URL = os.getenv("ENDPOINT_URL")
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")

def connect_to_s3():
    spark = SparkSession.builder.master("local[*]").appName("save_file") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \  
        .config("spark.hadoop.fs.s3a.endpoint", ENDPOINT_URL)\
        .config("spark.hadoop.fs.s3a.access.key", ACCESS_KEY)\
        .config("spark.hadoop.fs.s3a.secret.key", SECRET_ACCESS_KEY)\
        .config("spark.hadoop.fs.s3a.path.style.access", "true").getOrCreate()

    df = spark.read.csv("s3a://raw/olist_orders_dataset.csv")

    df.show()

connect_to_s3()