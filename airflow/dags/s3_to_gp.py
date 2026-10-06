from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from datetime import datetime

with DAG(dag_id="s3_to_gp", start_date=datetime(2026, 1, 1), catchup=False) as dag:

    orders_dataset = SparkSubmitOperator(
        task_id="orders_dataset",
        application="/opt/airflow/dags/scripts/etl.py",
        conn_id="spark_default"
    )
