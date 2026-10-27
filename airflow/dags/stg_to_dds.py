from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import chain
from datetime import datetime
from scripts.dataClass import dagConfig, STGschema_config, DDSschema_config




with DAG(dag_id="stg_to_dds", start_date=datetime(2026, 1, 1), catchup=False) as dag:

