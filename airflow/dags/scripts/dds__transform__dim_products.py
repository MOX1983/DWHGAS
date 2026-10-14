from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import sys

from config import DATA as data, OPTIONS

# Проверить !!
def dds_t_df_dim_products(logical_date: str):
    spark = SparkSession.builder.master("local[*]").appName("save_file").getOrCreate()

    df_products = spark.read.format('jdbc') \
        .options(**OPTIONS).options(dbtable=f"{data['schemas'][0]}.{data['table'][3]}").load()

    df_dim_products = df_products.select("product_id", "product_category_name",
                                         "product_name_lenght", "product_description_lenght",
                                         "product_photos_qty", "product_weight_g",
                                         "product_length_cm", "product_height_cm",
                                         "product_width_cm")\
        .filter("logical_date" >= logical_date)

    print(df_dim_products.count())
    df_dim_products.explain(mode="formatted")


dds_t_df_dim_products(sys.argv[1])