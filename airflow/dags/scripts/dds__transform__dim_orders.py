import sys

import pyspark.sql.functions as F
from connect_db import delete_by_logical_date
from pyspark.sql import SparkSession

from dataClass import optionsConfig, STGschema_config, DDSschema_config

def dds_t_dim_orders(logical_date: str):
    DIM_TABLE = DDSschema_config.dim_orders
    spark = SparkSession.builder.master("local[*]").appName("dds_t_dim_orders").getOrCreate()

    df_orders = (
        spark.read.format("jdbc")
        .options(**optionsConfig.get_dict_options())
        .options(dbtable=STGschema_config.orders_dataset)
        .load()
    )

    df_order_items = (
        spark.read.format("jdbc")
        .options(**optionsConfig.get_dict_options())
        .options(dbtable=STGschema_config.order_items_dataset)
        .load()
    )

    df_order_items = (
        df_order_items.filter(df_order_items["logical_date"] == F.lit(logical_date))
        .groupBy("order_id")
        .agg(
            F.min("shipping_limit_date").alias("shipping_limit_date"),
            F.max("logical_date").alias("logical_date"),
        )
    )

    df_dim_orders = df_order_items.join(
        F.broadcast(df_orders),
        (df_orders["order_id"] == df_order_items["order_id"])
        & (df_orders["logical_date"] == df_order_items["logical_date"])
        & (df_orders["logical_date"] == F.lit(logical_date)),
        "left",
    ).select(
        df_orders["order_id"],
        df_orders["order_status"],
        df_orders["order_approved_at"],
        df_orders["order_delivered_carrier_date"],
        df_orders["order_delivered_customer_date"],
        df_orders["order_estimated_delivery_date"],
        df_orders["order_purchase_timestamp"],
        df_order_items["shipping_limit_date"],
        df_orders["logical_date"],
    )

    delete_by_logical_date(DIM_TABLE, logical_date)

    df_dim_orders.write.format("jdbc").options(**optionsConfig.get_dict_options_for_write()).options(
        dbtable=DIM_TABLE
    ).mode("append").save()


dds_t_dim_orders(sys.argv[1])
