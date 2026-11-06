from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import chain
from testPlug import dagDqConfig, dag_name, STGschema_config, spark_task
from datetime import datetime


with DAG(dag_id="dq_check_stg_dds", start_date=datetime(2026, 1, 1), catchup=False) as dag:
# сделать генерацию тасок на все табл
    @spark_task(application=dagDqConfig.dag_task.check_null_py)
    def check_null__stg_orders_dataset(application: str):
        return SparkSubmitOperator(
            task_id="check_null__stg_orders_dataset",
            application=application,
            conn_id=dagDqConfig.conn_id,
            pool=dagDqConfig.pool,
            jars=dagDqConfig.jars,
            application_args=["{{ ts }}", STGschema_config.orders_dataset]
        )

    trigger__stg_to_dds = TriggerDagRunOperator(
        task_id="trigger__stg_to_dds",
        trigger_dag_id=dag_name.stg_to_dds,
        logical_date="{{ ts }}"
    )


    chain(
        check_null__stg_orders_dataset(),
        trigger__stg_to_dds
    )
