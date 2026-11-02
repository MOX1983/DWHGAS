import sys
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from plugins.testPlug.dataClass import optionsConfig, dqCheckStg

def check_null_stg_dds(logical_date: str, table_name: str):
    # отличная идея взять для dwh данные которые нудны для аналитиков
    # данные читые нулей нет, сылочная целостность, уникальные поля уникальные
    # но dq надо реализовать на нули и целостность и свертка балансов(частично есть ) и тригеры на витрины и норм (я забыла что витрины я переделала на view)
    spark = SparkSession.builder.appName("check_null_stg_dds").getOrCreate()

    df_order = spark.read.format("jdbc").options(**optionsConfig.get_dict_options())\
        .options(dbtable=table_name).load()

    columns = dqCheckStg.table["table_name"]
    limit = dqCheckStg.limits["not_null"]

    df_order = df_order.filter(F.col("logical_date") == F.lit(logical_date))

    for i in columns:
        try:
            res = df_order.select(F.count(F.when(F.col(i).isNull())).alias(f"cnt"))
            print(f"{table_name}{i} \t {res}")
            if res >= limit:
                raise Exception(f"count null row in table {table_name} column {i} > limit")
        except Exception as e:
            print(e)



check_null_stg_dds(sys.argv[1], sys.argv[2])