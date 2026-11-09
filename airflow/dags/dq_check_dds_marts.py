from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import chain
from testPlug import dagDqConfig, dag_name, STGschema_config, spark_task
from datetime import datetime



with DAG(dag_id="dq_check_dds_marts", start_date=datetime(2026, 1, 1), catchup=False) as dag:


    trigger__mart_mv = TriggerDagRunOperator(
        task_id="trigger__mart_mv",
        trigger_dag_id=dag_name.mart_mv,
        logical_date="{{ ts }}"
    )

    chain(
        trigger__mart_mv
    )
