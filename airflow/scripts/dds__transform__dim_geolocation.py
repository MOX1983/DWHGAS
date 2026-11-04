import sys

import pyspark.sql.functions as F
from connect_db import delete_by_logical_date
from pyspark.sql import SparkSession

from dataClass import optionsConfig, STGschema_config, DDSschema_config

def dds_t_df_dim_geolocation(logical_date: str):
    DIM_TABLE = DDSschema_config.dim_geolocation
    spark = SparkSession.builder.appName("dds_t_df_dim_geolocation").getOrCreate()

    df_geolocation = (
        spark.read.format("jdbc")
        .options(**optionsConfig.get_dict_options())
        .options(dbtable=STGschema_config.geolocation_dataset)
        .load()
    )

    df_dim_geolocation = df_geolocation.select(
        "geolocation_zip_code_prefix",
        "geolocation_lat",
        "geolocation_lng",
        "geolocation_city",
        "geolocation_state",
        "logical_date",
    ).filter(df_geolocation["logical_date"] >= F.lit(logical_date))

    delete_by_logical_date(DIM_TABLE, logical_date)

    df_dim_geolocation.write.format("jdbc").options(**optionsConfig.get_dict_options_for_write()).options(
        dbtable=DIM_TABLE
    ).mode("append").save()


dds_t_df_dim_geolocation(sys.argv[1])
