from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import sys

from config import DATA as data, OPTIONS

# Проверить !!
def dds_t_df_dim_sellers(logical_date: str):
    spark = SparkSession.builder.master("local[*]").appName("save_file").getOrCreate()

    df_sellers = spark.read.format('jdbc') \
        .options(**OPTIONS).options(dbtable=f"{data['schemas'][0]}.{data['table'][4]}").load()

    df_dim_products = df_sellers.select("seller_id", "seller_zip_code_prefix",
                                         "seller_city", "seller_state")\
        .filter("logical_date" >= logical_date)

    print(df_dim_products.count())
    df_dim_products.explain(mode="formatted")


dds_t_df_dim_sellers(sys.argv[1])