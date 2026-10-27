from datetime import datetime
from pathlib import Path

import yaml
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import chain
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

    @spark_task(application=dagConfig.dag_task.transform_py)
    def dds_t_fact_orders(application: str):
        return SparkSubmitOperator(
            task_id="dds_t_fact_orders",
            application=application,
            conn_id=dagConfig.conn_id,
            pool=dagConfig.pool,
            jars=dagConfig.jars,
            application_args=["{{ ts }}"],
        )

    @spark_task(application=dagConfig.dag_task.dds__transform__fact_payments_py)
    def dds_t_fact_payments(application: str):
        return SparkSubmitOperator(
            task_id="dds_t_fact_payments",
            application=application,
            conn_id=dagConfig.conn_id,
            pool=dagConfig.pool,
            jars=dagConfig.jars,
            application_args=["{{ ts }}"],
        )

    @spark_task(application=dagConfig.dag_task.dds__transform__fact_reviews_py)
    def dds_t_fact_reviews(application: str):
        return SparkSubmitOperator(
            task_id="dds_t_fact_reviews",
            application=application,
            conn_id=dagConfig.conn_id,
            pool=dagConfig.pool,
            jars=dagConfig.jars,
            application_args=["{{ ts }}"],
        )

    @spark_task(application=dagConfig.dag_task.dds__transform__dim_orders_py)
    def dds_t_dim_orders(application: str):
        return SparkSubmitOperator(
            task_id="dds_t_dim_orders",
            application=application,
            conn_id=dagConfig.conn_id,
            pool=dagConfig.pool,
            jars=dagConfig.jars,
            application_args=["{{ ts }}"],
        )

    @spark_task(application=dagConfig.dag_task.dds__transform__dim_products_py)
    def dds_t_df_dim_products(application: str):
        return SparkSubmitOperator(
            task_id="dds_t_df_dim_products",
            application=application,
            conn_id=dagConfig.conn_id,
            pool=dagConfig.pool,
            jars=dagConfig.jars,
            application_args=["{{ ts }}"],
        )

    @spark_task(application=dagConfig.dag_task.dds__transform__dim_customers_py)
    def dds_t_dim_customers(application: str):
        return SparkSubmitOperator(
            task_id="dds_t_dim_customers",
            application=application,
            conn_id=dagConfig.conn_id,
            pool=dagConfig.pool,
            jars=dagConfig.jars,
            application_args=["{{ ts }}"],
        )


    @spark_task(application=dagConfig.dag_task.dds__transform__dim_sellers_py)
    def dds_t_df_dim_sellers(application: str):
        return SparkSubmitOperator(
            task_id="dds_t_df_dim_sellers",
            application=application,
            conn_id=dagConfig.conn_id,
            pool=dagConfig.pool,
            jars=dagConfig.jars,
            application_args=["{{ ts }}"],
        )


    @spark_task(application=dagConfig.dag_task.dds__transform__dim_geolocation_py)
    def dds_t_df_dim_geolocation(application: str):
        return SparkSubmitOperator(
            task_id="dds_t_df_dim_geolocation",
            application=application,
            conn_id=dagConfig.conn_id,
            pool=dagConfig.pool,
            jars=dagConfig.jars,
            application_args=["{{ ts }}"],
        )

    chain(
        task,
        dds_t_fact_orders(),
        dds_t_fact_payments(),
        dds_t_fact_reviews(),
        dds_t_dim_orders(),
        dds_t_df_dim_products(),
        dds_t_dim_customers(),
        dds_t_df_dim_sellers(),
        dds_t_df_dim_geolocation(),
    )
