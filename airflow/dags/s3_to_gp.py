from datetime import datetime
from pathlib import Path

import yaml
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from scripts.dataClass import dagConfig, STGschema_config
from yaml.loader import SafeLoader

PATH_NAME = Path(__file__).parent / "schemas_table_name.yml"


def load_yaml():
    with open(PATH_NAME, "r") as f:
        return yaml.load(f, Loader=SafeLoader)


def spark_task(application: str):
    def decorator(func):
        def wrapper(*args, **kwargs):
            return func(application=application, *args, **kwargs)

        return wrapper

    return decorator


with DAG(dag_id="s3_to_gp", start_date=datetime(2026, 1, 1), catchup=False) as dag:
    data = load_yaml()
    task = []

    for i in range(9):
        orders_dataset = SparkSubmitOperator(
            task_id=f"orders_dataset_{i}",
            application=dagConfig.dag_task.etl_py,
            conn_id=dagConfig.conn_id,
            application_args=[
                "{{ ts }}",
                STGschema_config.schema.get_schema() + data["table"][i],
                data["file"][i],
            ],
            pool=dagConfig.pool
        )
        task.append(orders_dataset)

    trigger__dq_check_stg_dds = TriggerDagRunOperator(
        task_id="trigger_dq_check_stg_dds",
        trigger_dag_id="dq_check_stg_dds",
        logical_date="{{ ts }}"
    )

    task >> trigger__dq_check_stg_dds