import sys
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from testPlug import optionsConfig, dqCheckStg

def check_null_stg_dds(logical_date: str, table_name: str):
    spark = SparkSession.builder.appName("check_null_stg_dds").getOrCreate()

    df_order = spark.read.format("jdbc").options(**optionsConfig.get_dict_options())\
        .options(dbtable=table_name).load()

    columns = dqCheckStg.table[table_name]
    limit = dqCheckStg.limits["not_null"]

    df_order = df_order.filter(F.col("logical_date") == F.lit(logical_date))
    res_all_order = df_order.count()

    for i in columns:
        try:
            null_order = df_order.filter(F.col(i).isNull()).count()
            res = 100.0 * null_order / res_all_order
            print(f"{table_name} {i} \t {res}")
            if res >= limit:
                raise Exception(f"count null row in table {table_name} column {i} > limit")
        except ZeroDivisionError as e:
            print(f"res_all_order = {res_all_order} \t {e}")
        except Exception as e:
            print(e)



check_null_stg_dds(sys.argv[1], sys.argv[2])