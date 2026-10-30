import sys
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from dataClass import optionsConfig, DDSschema_config, STGschema_config



def check_null_stg_dds(logical_date: str):
    # отличная идея взять для dwh данные которые нудны для аналитиков
    # данные читые нулей нет, сылочная целостность, уникальные поля уникальные
    # но dq надо реализовать на нули и целостность и свертка балансов(частично есть ) и тригеры на витрины и норм
    pass

check_null_stg_dds(sys.argv[1])