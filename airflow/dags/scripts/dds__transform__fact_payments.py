from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import sys

from config import DATA as data, OPTIONS

def dds_t_fact_payments(logical_date: str):
    spark = SparkSession.builder.master("local[*]").appName("save_file").getOrCreate()

    df_orders = spark.read.format('jdbc') \
        .options(**OPTIONS).options(dbtable=f"{data['schemas'][0]}.{data['table'][0]}").load()

    df_payments = spark.read.format('jdbc')\
        .options(**OPTIONS).options(dbtable=f"{data['schemas'][0]}.{data['table'][6]}").load()

    df_fact_payments = df_orders.join(
        F.broadcast(df_payments),
        (df_orders["order_id"] == df_payments["order_id"])
        & (df_orders["logical_date"] == df_payments["logical_date"])
        & (df_orders["logical_date"] == F.lit(logical_date)),
        "left").select(df_orders["order_id"], df_payments["payment_sequential"], df_payments["payment_type"],
                        df_payments["payment_installments"], df_payments["payment_value"])

    print(df_fact_payments.count())
    df_fact_payments.explain(mode="formatted")

dds_t_fact_payments(sys.argv[1])