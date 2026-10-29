import sys
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from schemas import SCHEMAS
from dataClass import minioConfig, optionsConfig


def connect_to_s3(logical_date: str, schema_table_name: str, minio_csv_name: str):
    spark = (
        SparkSession.builder.appName("save_file")
        .config("spark.hadoop.fs.s3a.endpoint", minioConfig.endpoint_url)
        .config("spark.hadoop.fs.s3a.access.key", minioConfig.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minioConfig.secret_access_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )

    schema = SCHEMAS[schema_table_name]
    df = (
        spark.read.option("header", "true")
        .schema(schema)
        .csv(f"s3a://raw/{minio_csv_name}.csv")
        .withColumn("logical_date", F.to_timestamp(F.lit(logical_date)))
        .coalesce(1)
    )

    df.write.format("jdbc").options(**optionsConfig.get_dict_options_for_write()).options(
        dbtable=schema_table_name).mode("append").save()

    spark.stop()


connect_to_s3(sys.argv[1], sys.argv[2], sys.argv[3])
