import sys
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from schemas import SCHEMAS
from dataClass import minioConfig, optionsConfig

# этой таской буду проверять 2 таблицы (в таске 1 таблицу, а вызывать 2 раза буду )
# order_payments_dataset c fact_payments
# и
# order_items_dataset c fact_orders


def check_balance_stg_dds(logical_date: str):
    dds_table_name =