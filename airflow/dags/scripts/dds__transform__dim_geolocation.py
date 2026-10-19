import sys

import pyspark.sql.functions as F
from connect_db import delete_by_logical_date
from pyspark.sql import SparkSession

from config import DATA as data
from config import OPTIONS, OPTIONS_WRITE


# Проверить !!
def dds_t_df_dim_geolocation(logical_date: str):
    DIM_TABLE = f"{data['schemas'][1]}.{data['table_dds'][7]}"
    spark = SparkSession.builder.master("local[*]").appName("save_file").getOrCreate()

    df_geolocation = (
        spark.read.format("jdbc")
        .options(**OPTIONS)
        .options(dbtable=f"{data['schemas'][0]}.{data['table'][5]}")
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

    df_dim_geolocation.write.format("jdbc").options(**OPTIONS_WRITE).options(
        dbtable=DIM_TABLE
    ).mode("append").save()


dds_t_df_dim_geolocation(sys.argv[1])
