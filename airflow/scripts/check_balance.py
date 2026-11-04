import sys
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from dataClass import optionsConfig, DDSschema_config, STGschema_config

# я оставлю этот гениальный шаг
# сравнение не реализованно , сравнивать distinct key

# этой таской буду проверять 2 таблицы (в таске 1 таблицу, а вызывать 2 раза буду )
# order_payments_dataset c fact_payments
# и
# order_items_dataset c fact_orders


def check_balance_stg_dds(logical_date: str):
    dds_table_name = DDSschema_config.fact_orders
    stg_table_name = STGschema_config.order_items_dataset
    spark = SparkSession.builder.appName("check_balance_stg_dds").getOrCreate()

    df_dds_table = spark.read.format("jdbc").options(**optionsConfig.get_dict_options())\
        .options(dbtable=dds_table_name).load()

    df_dds_table = df_dds_table.filter(F.col("logical_date") == F.lit(logical_date))\
        .select(F.sum(F.col("price") + F.col("freight_value")).alias("all_price")).collect()
    print("df_dds_table\t", df_dds_table)

    df_stg_table = spark.read.format("jdbc").options(**optionsConfig.get_dict_options())\
        .options(dbtable=stg_table_name).load()

    df_stg_table = df_stg_table.filter(F.col("logical_date") == F.lit(logical_date))\
        .select(F.sum(F.col("price") + F.col("freight_value")).alias("all_price"))

    print("df_stg_table\t", df_stg_table)


check_balance_stg_dds(sys.argv[1])