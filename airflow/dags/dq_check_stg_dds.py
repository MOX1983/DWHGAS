from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import chain
from scripts.dataClass import dagDqConfig, dag_name
from scripts.decorator import spark_task
from datetime import datetime


with DAG(dag_id="dq_check_stg_dds", start_date=datetime(2026, 1, 1), catchup=False) as dag:
# я гений...
# надо переделать удаление нулевых и чего-то ещё

    @spark_task(application=dagDqConfig.dag_task.check_balance_py)
    def check_balans__fact_orders__order_items_dataset(application: str):
        return SparkSubmitOperator(
            task_id="check_balans__fact_orders__order_items_dataset",
            application=application,
            conn_id=dagDqConfig.conn_id,
            pool=dagDqConfig.pool,
            jars=dagDqConfig.jars,
            application_args=["{{ ts }}"]
        )

    trigger__stg_to_dds = TriggerDagRunOperator(
        task_id="trigger__stg_to_dds",
        trigger_dag_id=dag_name.stg_to_dds,
        logical_date="{{ ts }}"
    )


    chain(
        check_balans__fact_orders__order_items_dataset(),
        trigger__stg_to_dds
    )
