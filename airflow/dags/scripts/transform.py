import sys

import pyspark.sql.functions as F
from connect_db import delete_by_logical_date
from pyspark.sql import SparkSession

from dataClass import STGschema_config, DDSschema_config, optionsConfig

def dds_t_fact_orders(logical_date: str):
    FACT_TABLE = DDSschema_config.fact_orders

    spark = SparkSession.builder.appName("dds_t_fact_orders").getOrCreate()

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

    df_fact_orders = df_order_items.join(
        F.broadcast(df_orders),
        (df_orders["order_id"] == df_order_items["order_id"])
        & (df_orders["logical_date"] == df_order_items["logical_date"])
        & (df_orders["logical_date"] == F.lit(logical_date)),
        "inner",
    ).select(
        df_orders["order_id"],
        df_order_items["order_item_id"],
        df_order_items["product_id"],
        df_order_items["price"],
        df_order_items["freight_value"],
        df_orders["customer_id"],
        df_order_items["seller_id"],
        df_orders["logical_date"],
    )
    print(df_fact_orders.count())

    delete_by_logical_date(FACT_TABLE, logical_date)

    df_fact_orders.write.format("jdbc").options(**optionsConfig.get_dict_options_for_write()).options(
        dbtable=FACT_TABLE
    ).mode("append").save()

    spark.stop()


dds_t_fact_orders(sys.argv[1])
