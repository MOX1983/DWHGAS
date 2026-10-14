from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import sys

from config import DATA as data, OPTIONS

# Проверить !!
def dds_t_df_dim_geolocation(logical_date: str):
    spark = SparkSession.builder.master("local[*]").appName("save_file").getOrCreate()

    df_geolocation = spark.read.format('jdbc') \
        .options(**OPTIONS).options(dbtable=f"{data['schemas'][0]}.{data['table'][5]}").load()

    df_dim_geolocation = df_geolocation.select("geolocation_zip_code_prefix", "geolocation_lat",
                                               "geolocation_lng", "geolocation_city",
                                               "geolocation_state")\
        .filter("logical_date" >= logical_date)

    print(df_dim_geolocation.count())
    df_dim_geolocation.show(10)
    df_dim_geolocation.explain(mode="formatted")


dds_t_df_dim_geolocation(sys.argv[1])