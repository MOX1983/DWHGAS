import sys

import pyspark.sql.functions as F
from connect_db import delete_by_logical_date
from pyspark.sql import SparkSession

from config import DATA as data
from config import OPTIONS, OPTIONS_WRITE


def dds_t_df_dim_sellers(logical_date: str):
    DIM_TABLE = f"{data['schemas'][1]}.{data['table_dds'][6]}"
    spark = SparkSession.builder.master("local[*]").appName("save_file").getOrCreate()

    df_sellers = (
        spark.read.format("jdbc")
        .options(**OPTIONS)
        .options(dbtable=f"{data['schemas'][0]}.{data['table'][4]}")
        .load()
    )

    df_dim_products = df_sellers.select(
        "seller_id",
        "seller_zip_code_prefix",
        "seller_city",
        "seller_state",
        "logical_date",
    ).filter(df_sellers["logical_date"] >= F.lit(logical_date))

    delete_by_logical_date(DIM_TABLE, logical_date)

    df_dim_products.write.format("jdbc").options(**OPTIONS_WRITE).options(
        dbtable=DIM_TABLE
    ).mode("append").save()


dds_t_df_dim_sellers(sys.argv[1])
