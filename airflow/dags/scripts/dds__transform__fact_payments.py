import sys

import pyspark.sql.functions as F
from connect_db import delete_by_logical_date
from pyspark.sql import SparkSession

from config import DATA as data
from config import OPTIONS, OPTIONS_WRITE


def dds_t_fact_payments(logical_date: str):
    FACT_TABLE = f"{data['schemas'][1]}.{data['table_dds'][1]}"
    spark = SparkSession.builder.master("local[*]").appName("save_file").getOrCreate()

    df_orders = (
        spark.read.format("jdbc")
        .options(**OPTIONS)
        .options(dbtable=f"{data['schemas'][0]}.{data['table'][0]}")
        .load()
    )

    df_payments = (
        spark.read.format("jdbc")
        .options(**OPTIONS)
        .options(dbtable=f"{data['schemas'][0]}.{data['table'][6]}")
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

    df_fact_payments.write.format("jdbc").options(**OPTIONS_WRITE).options(
        dbtable=FACT_TABLE
    ).mode("append").save()


dds_t_fact_payments(sys.argv[1])
