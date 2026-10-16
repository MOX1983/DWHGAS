from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import chain

from datetime import datetime
from pathlib import Path
import yaml
from yaml.loader import SafeLoader

PATH_NAME = Path(__file__).parent / "schemas_table_name.yml"
def load_yaml():
    with open(PATH_NAME, "r") as f:
        return yaml.load(f, Loader=SafeLoader)

with DAG(dag_id="s3_to_gp", start_date=datetime(2026, 1, 1), catchup=False) as dag:

    data = load_yaml()
    task = []

#ну что мне делать.... как мне в future сделать неприрывную обработу ?????
# если бы это была не учебная работа, как я гарантирую, что у меня будет появляться файл с новыми клиентами ???(города....)
# для каждой таблицы отдельный даг? или распилить их на entity (мб, надо подумать )

    for i in range(9):
        orders_dataset = SparkSubmitOperator(
            task_id=f"orders_dataset_{i}",
            application=data["dag_task"][0],
            conn_id="spark_default",
            application_args=["{{ ts }}",
                data["schemas"][0] + "." + data["table"][i],
                data["file"][i]],
            pool="two_pool"
        )
        task.append(orders_dataset)
# DRY (хотя я мб буду уносить в другие даги)
    dds_t_fact_orders = SparkSubmitOperator(
        task_id=f"dds_t_fact_orders",
        application=data["dag_task"][1],
        conn_id="spark_default",
        pool="two_pool",
        jars=data['jars'],
        application_args=["{{ ts }}"]
    )

    dds_t_fact_payments = SparkSubmitOperator(
        task_id=f"dds_t_fact_payments",
        application=data["dag_task"][2],
        conn_id="spark_default",
        pool="two_pool",
        jars=data['jars'],
        application_args=["{{ ts }}"]
    )

    dds_t_fact_reviews = SparkSubmitOperator(
        task_id=f"dds_t_fact_reviews",
        application=data["dag_task"][3],
        conn_id="spark_default",
        pool="two_pool",
        jars=data['jars'],
        application_args=["{{ ts }}"]
    )

    dds_t_dim_orders = SparkSubmitOperator(
        task_id=f"dds_t_dim_orders",
        application=data["dag_task"][4],
        conn_id="spark_default",
        pool="two_pool",
        jars=data['jars'],
        application_args=["{{ ts }}"]
    )

    dds_t_df_dim_products = SparkSubmitOperator(
        task_id=f"dds_t_df_dim_products",
        application=data["dag_task"][5],
        conn_id="spark_default",
        pool="two_pool",
        jars=data['jars'],
        application_args=["{{ ts }}"]
    )
    dds_t_dim_customers = SparkSubmitOperator(
        task_id=f"dds_t_dim_customers",
        application=data["dag_task"][6],
        conn_id="spark_default",
        pool="two_pool",
        jars=data['jars'],
        application_args=["{{ ts }}"]
    )

    dds_t_df_dim_sellers = SparkSubmitOperator(
        task_id=f"dds_t_df_dim_sellers",
        application=data["dag_task"][7],
        conn_id="spark_default",
        pool="two_pool",
        jars=data['jars'],
        application_args=["{{ ts }}"]
    )

    dds_t_df_dim_geolocation = SparkSubmitOperator(
        task_id=f"dds_t_df_dim_geolocation",
        application=data["dag_task"][8],
        conn_id="spark_default",
        pool="two_pool",
        jars=data['jars'],
        application_args=["{{ ts }}"]
    )

    chain(task, dds_t_fact_orders, dds_t_fact_payments, dds_t_fact_reviews, dds_t_dim_orders,
          dds_t_df_dim_products, dds_t_dim_customers, dds_t_df_dim_sellers, dds_t_df_dim_geolocation
          )



