from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import chain
from datetime import datetime
from testPlug import marts_schema_config
from marts_refresh import refresh_materialized_view

with DAG(dag_id="mart_mv", start_date=datetime(2026, 1, 1), catchup=False) as dag:
    task = []
    for i in range(len(marts_schema_config.mv)):
        refresh_mv = PythonOperator(
            task_id=f"refresh_mv_{marts_schema_config.mv[0]}",
            python_callable=refresh_materialized_view,
            op_kwargs={"schema_name_mv": marts_schema_config.mv[0]}
        )

    chain(task)