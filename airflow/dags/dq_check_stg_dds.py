from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from scripts.dataClass import dagConfig, STGschema_config

from datetime import datetime


with DAG(dag_id="dq_check_stg_dds", start_date=datetime(2026, 1, 1), catchup=False) as dag:
# наверное надо сначала удлять каку, а потом сверять балансы, но хоть так сделаю, а потом переделаю
# а это все потому, что Ксюша зажмотила 2 часа на обзор данных (и что там фу, а что не фу)
# и из-за этого не уверена что балансы совпадут
# я в инсте увидела,что в сете есть ловушки (


