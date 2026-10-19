import sys

import pyspark.sql.functions as F
from connect_db import delete_by_logical_date
from pyspark.sql import SparkSession

from config import DATA as data
from config import OPTIONS, OPTIONS_WRITE


def dds_t_df_dim_products(logical_date: str):
    DIM_TABLE = f"{data['schemas'][1]}.{data['table_dds'][4]}"
    spark = SparkSession.builder.master("local[*]").appName("save_file").getOrCreate()

    df_products = (
        spark.read.format("jdbc")
        .options(**OPTIONS)
        .options(dbtable=f"{data['schemas'][0]}.{data['table'][3]}")
        .load()
    )

    df_dim_products = df_products.select(
        "product_id",
        "product_category_name",
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "logical_date",
    ).filter(df_products["logical_date"] >= F.lit(logical_date))

    delete_by_logical_date(DIM_TABLE, logical_date)

    df_dim_products.write.format("jdbc").options(**OPTIONS_WRITE).options(
        dbtable=DIM_TABLE
    ).mode("append").save()


dds_t_df_dim_products(sys.argv[1])
