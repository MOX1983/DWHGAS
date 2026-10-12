from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from pathlib import Path
from dotenv import load_dotenv
import os
import sys
import yaml
from yaml.loader import SafeLoader

load_dotenv(Path(__file__).parent / ".env")
# мб унести куда-то
PATH_NAME = Path(__file__).parent.parent / "schemas_table_name.yml"
def load_yaml():
    with open(PATH_NAME, "r") as f:
        return yaml.load(f, Loader=SafeLoader)

def raw_to_kimall(run_id: str):
    POSTGRES_DB = os.getenv("POSTGRES_DB")
    POSTGRES_USER = os.getenv("POSTGRES_USER")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
    GP_URL = os.getenv("GP_URL") + "/" + POSTGRES_DB
    DRIVER = "org.postgresql.Driver"

    data = load_yaml()
    options = {
        "url": GP_URL,
        "user": POSTGRES_USER,
        "password": POSTGRES_PASSWORD,
        "driver": DRIVER,
        "partitionColumn": 'run_id'
    }

    spark = SparkSession.builder.master("local[*]").appName("save_file").getOrCreate()

    df_orders = spark.read.format('io.pivotal.greenplum.spark.GreenplumRelationProvider')\
        .options(**options).options(dbtable=data["table"][0]).load()

    df_orders.show(3)

    df_order_items = spark.read.format('io.pivotal.greenplum.spark.GreenplumRelationProvider')\
        .options(**options).options(dbtable=data["table"][2]).load()

    df_order_items.show(3)

    df_fact_orders = df_order_items.join(
        F.broadcast(df_orders),
        (df_orders["order_id"] == df_order_items["order_id"])
        & (df_orders["run_id"] == df_order_items["run_id"]),
        "inner").select(df_orders["order_id"], df_order_items["order_item_id"], df_order_items["product_id"],
                        df_order_items["price"], df_order_items["freight_value"], df_orders["customer_id"],
                        df_order_items["seller_id"])

    df_fact_orders.show(10)







raw_to_kimall(sys.argv[1])