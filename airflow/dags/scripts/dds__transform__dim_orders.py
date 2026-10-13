from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import sys

from config import DATA as data, OPTIONS


def dds_t_dim_orders(logical_date: str):
    spark = SparkSession.builder.master("local[*]").appName("save_file").getOrCreate()

    df_orders = spark.read.format('jdbc') \
        .options(**OPTIONS).options(dbtable=f"{data['schemas'][0]}.{data['table'][0]}").load()

    df_order_items = spark.read.format('jdbc') \
        .options(**OPTIONS).options(dbtable=f"{data['schemas'][0]}.{data['table'][2]}").load()

    df_dim_orders = df_order_items.join(
        F.broadcast(df_orders),
        (df_orders["order_id"] == df_order_items["order_id"])
        & (df_orders["logical_date"] == df_order_items["logical_date"])
        & (df_orders["logical_date"] == F.lit(logical_date)),
        "left").select(df_orders["order_id"], df_orders["order_status"], df_orders["order_approved_at"],
                       df_orders["order_delivered_carrier_date"], df_orders["order_delivered_customer_date"],
                       df_orders["order_estimated_delivery_date"], df_orders["order_purchase_timestamp"],
                       df_order_items["shipping_limit_date"])

    print(df_dim_orders.count())
    df_dim_orders.explain(mode="formatted")


dds_t_dim_orders(sys.argv[1])