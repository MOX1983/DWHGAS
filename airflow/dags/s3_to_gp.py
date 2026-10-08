from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

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

    for i in range(9):
        orders_dataset = SparkSubmitOperator(
            task_id=f"orders_dataset_{i}",
            application="/opt/airflow/dags/scripts/etl.py",
            conn_id="spark_default",
            application_args=[
                data["schemas"][0] + "." + data["table"][i],
                data["file"][i]],
            pool="two_pool"
        )
