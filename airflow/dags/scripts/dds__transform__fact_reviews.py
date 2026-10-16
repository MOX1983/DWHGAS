from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import sys

from config import DATA as data, OPTIONS, OPTIONS_WRITE
from connect_db import delete_by_logical_date

def dds_t_fact_reviews(logical_date: str):
    FACT_TABLE = f"{data['schemas'][1]}.{data['table_dds'][2]}"
    spark = SparkSession.builder.master("local[*]").appName("save_file").getOrCreate()

    df_orders = spark.read.format('jdbc') \
        .options(**OPTIONS).options(dbtable=f"{data['schemas'][0]}.{data['table'][0]}").load()

    df_order_reviews = spark.read.format('jdbc')\
        .options(**OPTIONS).options(dbtable=f"{data['schemas'][0]}.{data['table'][7]}").load()
    
    df_fact_reviews = df_order_reviews.join(
        F.broadcast(df_orders),
        (df_orders["order_id"] == df_order_reviews["order_id"])
        & (df_orders["logical_date"] == df_order_reviews["logical_date"])
        & (df_orders["logical_date"] == F.lit(logical_date)),
        "left").select(df_order_reviews["review_id"], df_orders["order_id"], df_order_reviews["review_score"],
                       df_order_reviews["review_comment_title"], df_order_reviews["review_comment_message"],
                       df_order_reviews["review_creation_date"],  df_order_reviews["review_answer_timestamp"],
                       df_orders["logical_date"])

    delete_by_logical_date(FACT_TABLE, logical_date)

    df_fact_reviews.write.format("jdbc") \
        .options(**OPTIONS_WRITE) \
        .options(dbtable=FACT_TABLE) \
        .mode("append").save()



dds_t_fact_reviews(sys.argv[1])