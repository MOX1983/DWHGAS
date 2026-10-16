from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import sys

from config import DATA as data, OPTIONS,OPTIONS_WRITE
from connect_db import delete_by_logical_date

# Проверить !!
def dds_t_dim_customers(logical_date: str):
    DIM_TABLE = f"{data['schemas'][1]}.{data['table_dds'][5]}"
    spark = SparkSession.builder.master("local[*]").appName("save_file").getOrCreate()

    df_customers = spark.read.format('jdbc') \
        .options(**OPTIONS).options(dbtable=f"{data['schemas'][0]}.{data['table'][1]}").load()

    df_dim_customers = df_customers.select("customer_id", "customer_unique_id",
                                           "customer_zip_code_prefix", "customer_city",
                                           "customer_state", "logical_date")\
        .filter(df_customers["logical_date"] >= F.lit(logical_date))

    delete_by_logical_date(DIM_TABLE, logical_date)

    df_dim_customers.write.format("jdbc")\
        .options(**OPTIONS_WRITE) \
        .options(dbtable=DIM_TABLE) \
        .mode("append").save()


dds_t_dim_customers(sys.argv[1])