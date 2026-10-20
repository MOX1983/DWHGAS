import sys

import pyspark.sql.functions as F
from connect_db import delete_by_logical_date
from pyspark.sql import SparkSession

from dataClass import optionsConfig, STGschema_config, DDSschema_config

def dds_t_dim_customers(logical_date: str):
    DIM_TABLE = DDSschema_config.dim_customers
    spark = SparkSession.builder.master("local[*]").appName("dds_t_dim_customers").getOrCreate()

    df_customers = (
        spark.read.format("jdbc")
        .options(**optionsConfig.get_dict_options())
        .options(dbtable=STGschema_config.customers_dataset)
        .load()
    )

    df_dim_customers = df_customers.select(
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
        "logical_date",
    ).filter(df_customers["logical_date"] >= F.lit(logical_date))

    delete_by_logical_date(DIM_TABLE, logical_date)

    df_dim_customers.write.format("jdbc").options(**optionsConfig.get_dict_options_for_write()).options(
        dbtable=DIM_TABLE
    ).mode("append").save()


dds_t_dim_customers(sys.argv[1])
