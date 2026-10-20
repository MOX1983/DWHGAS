import sys

import pyspark.sql.functions as F
from connect_db import delete_by_logical_date
from pyspark.sql import SparkSession

from dataClass import optionsConfig, STGschema_config, DDSschema_config

def dds_t_df_dim_products(logical_date: str):
    DIM_TABLE = DDSschema_config.dim_products
    spark = SparkSession.builder.master("local[*]").appName("dds_t_df_dim_products").getOrCreate()

    df_products = (
        spark.read.format("jdbc")
        .options(**optionsConfig.get_dict_options())
        .options(dbtable=STGschema_config.products_dataset)
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

    df_dim_products.write.format("jdbc").options(**optionsConfig.get_dict_options_for_write()).options(
        dbtable=DIM_TABLE
    ).mode("append").save()


dds_t_df_dim_products(sys.argv[1])
