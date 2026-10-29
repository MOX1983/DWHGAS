import sys

import pyspark.sql.functions as F
from connect_db import delete_by_logical_date
from pyspark.sql import SparkSession

from dataClass import optionsConfig, STGschema_config,DDSschema_config

def dds_t_fact_payments(logical_date: str):
    FACT_TABLE = DDSschema_config.fact_payments
    spark = SparkSession.builder.appName("dds_t_fact_payments").getOrCreate()

    df_orders = (
        spark.read.format("jdbc")
        .options(**optionsConfig.get_dict_options())
        .options(dbtable=STGschema_config.orders_dataset)
        .load()
    )

    df_payments = (
        spark.read.format("jdbc")
        .options(**optionsConfig.get_dict_options())
        .options(dbtable=STGschema_config.order_payments_dataset)
        .load()
    )

    df_fact_payments = df_orders.join(
        F.broadcast(df_payments),
        (df_orders["order_id"] == df_payments["order_id"])
        & (df_orders["logical_date"] == df_payments["logical_date"])
        & (df_orders["logical_date"] == F.lit(logical_date)),
        "left",
    ).select(
        df_orders["order_id"],
        df_payments["payment_sequential"],
        df_payments["payment_type"],
        df_payments["payment_installments"],
        df_payments["payment_value"],
        df_orders["logical_date"],
    )

    delete_by_logical_date(FACT_TABLE, logical_date)

    df_fact_payments.write.format("jdbc").options(**optionsConfig.get_dict_options_for_write()).options(
        dbtable=FACT_TABLE
    ).mode("append").save()


dds_t_fact_payments(sys.argv[1])
