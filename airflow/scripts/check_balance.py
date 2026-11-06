import sys
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from testPlug import optionsConfig, dqCheckStg

# этой таской буду проверять 2 таблицы
# order_payments_dataset c fact_payments
# и
# order_items_dataset c fact_orders


def check_balance_stg_dds(logical_date: str, fact_orders: str, order_items_dataset: str):
    dds_table_name = fact_orders
    stg_table_name = order_items_dataset
    limit = dqCheckStg.limits["check_balance"]

    spark = SparkSession.builder.appName("check_balance_stg_dds").getOrCreate()

    df_dds_table = spark.read.format("jdbc").options(**optionsConfig.get_dict_options())\
        .options(dbtable=dds_table_name).load()

    df_dds_table = df_dds_table.filter(F.col("logical_date") == F.lit(logical_date))\
        .select(F.sum(F.col("price") + F.col("freight_value")).alias("all_price")).collect()
    balance_dds_table = float(df_dds_table[0]["all_price"])
    print("df_dds_table\t", df_dds_table[0]["all_price"])

    df_stg_table = spark.read.format("jdbc").options(**optionsConfig.get_dict_options())\
        .options(dbtable=stg_table_name).load()

    df_stg_table = df_stg_table.filter(F.col("logical_date") == F.lit(logical_date))\
        .select(F.sum(F.col("price") + F.col("freight_value")).alias("all_price")).collect()
    balance_stg_table = float(df_stg_table[0]["all_price"])
    print("df_stg_table\t", df_stg_table[0]["all_price"])

    try:
        res = abs(balance_stg_table - balance_dds_table) * 100.0 / balance_stg_table
        if res >= limit:
            raise Exception(f"balance {res} >= limit {limit}")
    except ZeroDivisionError as e:
        print(f"balance_stg_table {balance_stg_table} \t {e}")
    except Exception as e:
        print(e)


check_balance_stg_dds(sys.argv[1], sys.argv[2], sys.argv[3])