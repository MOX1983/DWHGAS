from datetime import datetime
from pathlib import Path

import os
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from testPlug import dagConfig, STGschema_config, dag_name, load_yaml


def test_path():
    print("AIRFLOW_PROJ_DIR\t" + dag_name.s3_to_gp)


with (DAG(dag_id="test_dag_configs", start_date=datetime(2026, 1, 1), catchup=False) as dag):

    task = PythonOperator(
        task_id="test",
        python_callable=test_path
    )
    task