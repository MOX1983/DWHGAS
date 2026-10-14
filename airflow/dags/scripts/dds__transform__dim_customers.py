from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import sys

from config import DATA as data, OPTIONS

# Проверить !!
def dds_t_dim_customers(logical_date: str):
    spark = SparkSession.builder.master("local[*]").appName("save_file").getOrCreate()

    df_customers = spark.read.format('jdbc') \
        .options(**OPTIONS).options(dbtable=f"{data['schemas'][0]}.{data['table'][1]}").load()

    df_dim_customers = df_customers.select("customer_id", "customer_unique_id",
                                           "customer_zip_code_prefix", "customer_city", "customer_state")\
        .filter("logical_date" >= "logical_date")

    print(df_dim_customers.count())
    df_dim_customers.explain(mode="formatted")


dds_t_dim_customers(sys.argv[1])