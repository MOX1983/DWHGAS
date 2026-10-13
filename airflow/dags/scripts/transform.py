from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import sys

from config import DATA as data, OPTIONS

# ПРОВЕРИТЬ !!!!!!!! ПЛАН !!!!!!!! а то мне кажется он все строки читает
def dds_t_fact_orders(logical_date: str):

    spark = SparkSession.builder.master("local[*]").appName("save_file").getOrCreate()

    df_orders = spark.read.format('jdbc')\
        .options(**OPTIONS).options(dbtable=f"{data['schemas'][0]}.{data['table'][0]}").load()

    df_order_items = spark.read.format('jdbc')\
        .options(**OPTIONS).options(dbtable=f"{data['schemas'][0]}.{data['table'][2]}").load()

    df_fact_orders = df_order_items.join(
        F.broadcast(df_orders),
        (df_orders["order_id"] == df_order_items["order_id"])
        & (df_orders["logical_date"] == df_order_items["logical_date"])
        & (df_orders["logical_date"] == F.lit(logical_date)),
        "inner").select(df_orders["order_id"], df_order_items["order_item_id"], df_order_items["product_id"],
                        df_order_items["price"], df_order_items["freight_value"], df_orders["customer_id"],
                        df_order_items["seller_id"])

    print(df_fact_orders.count())
    df_fact_orders.explain(mode="formatted")

    # не забыть про идемпотентность on conflict при insert



dds_t_fact_orders(sys.argv[1])