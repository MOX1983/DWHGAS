import sys

import pyspark.sql.functions as F
from connect_db import delete_by_logical_date
from pyspark.sql import SparkSession

from dataClass import optionsConfig, STGschema_config, DDSschema_config

def dds_t_df_dim_sellers(logical_date: str):
    DIM_TABLE = DDSschema_config.dim_sellers
    spark = SparkSession.builder.master("local[*]").appName("dds_t_df_dim_sellers").getOrCreate()

    df_sellers = (
        spark.read.format("jdbc")
        .options(**optionsConfig.get_dict_options())
        .options(dbtable=STGschema_config.sellers_dataset)
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

    df_dim_products.write.format("jdbc").options(**optionsConfig.get_dict_options_for_write()).options(
        dbtable=DIM_TABLE
    ).mode("append").save()


dds_t_df_dim_sellers(sys.argv[1])
