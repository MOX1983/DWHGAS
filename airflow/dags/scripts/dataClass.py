import os
from dataclasses import dataclass
from pathlib import Path

import yaml
from dotenv import load_dotenv
from yaml.loader import SafeLoader

load_dotenv(Path(__file__).parent / ".env")
PATH_NAME = Path(__file__).parent.parent / "schemas_table_name.yml"


@dataclass
class Schema:
    schema: str

    def get_schema(self):
        return f"{self.schema}."


@dataclass
class StgSchema:
    schema: Schema
    orders_dataset: str
    customers_dataset: str
    order_items_dataset: str
    products_dataset: str
    sellers_dataset: str
    geolocation_dataset: str
    order_payments_dataset: str
    order_reviews_dataset: str
    product_category_name_translation: str


@dataclass
class DdsSchema:
    schema: Schema
    fact_orders: str
    fact_payments: str
    fact_reviews: str
    dim_orders: str
    dim_products: str
    dim_customers: str
    dim_sellers: str
    dim_geolocation: str


@dataclass
class File_dataset:
    olist_orders_dataset: str
    olist_customers_dataset: str
    olist_order_items_dataset: str
    olist_products_dataset: str
    olist_sellers_dataset: str
    olist_geolocation_dataset: str
    olist_order_payments_dataset: str
    olist_order_reviews_dataset: str
    product_category_name_translation: str


@dataclass
class Task:
    etl_py: str
    transform_py: str
    dds__transform__fact_payments_py: str
    dds__transform__fact_reviews_py: str
    dds__transform__dim_orders_py: str
    dds__transform__dim_products_py: str
    dds__transform__dim_customers_py: str
    dds__transform__dim_sellers_py: str
    dds__transform__dim_geolocation_py: str

@dataclass
class DqTask:
    check_balance_py: str

@dataclass
class DAGTasks:
    conn_id: str
    pool: str
    jars: str

    dag_task: Task | DqTask

# возможно лучше сделать через лист но в DAGTasks, ибо много классов
@dataclass
class DagName:
    s3_to_gp : str
    dq_check_stg_dds : str
    stg_to_dds : str

@dataclass
class MinIo:
    endpoint_url: str
    access_key: str
    secret_access_key: str


@dataclass
class DB:
    dbname: str
    user: str
    password: str
    host: str
    port: str
    _url: str

    def get_url(self):
        return self._url + "/" + self.dbname


@dataclass
class Options:
    db_config: DB
    driver: str = "org.postgresql.Driver"
    batchsize: int = 2000
    reWriteBatchedInserts: str = "true"

    def get_dict_options(self):
        return {
            "url": self.db_config.get_url(),
            "user": self.db_config.user,
            "password": self.db_config.password,
            "driver": self.driver,
        }

    def get_dict_options_for_write(self):
        data = self.get_dict_options()
        data["batchsize"] = self.batchsize
        data["reWriteBatchedInserts"] = self.reWriteBatchedInserts
        return data


def load_yaml():
    with open(PATH_NAME, "r") as f:
        return yaml.load(f, Loader=SafeLoader)


minioConfig = MinIo(
    os.getenv("ENDPOINT_URL"), os.getenv("ACCESS_KEY"), os.getenv("SECRET_ACCESS_KEY")
)

db = DB(
    os.getenv("POSTGRES_DB"),
    os.getenv("POSTGRES_USER"),
    os.getenv("POSTGRES_PASSWORD"),
    os.getenv("HOST"),
    os.getenv("PORT"),
    os.getenv("GP_URL"),
)

optionsConfig = Options(db)

data = load_yaml()

schema_stg_config = Schema(data["schemas"][0])
schema_dds_config = Schema(data["schemas"][1])

STGschema_config = StgSchema(
    schema_stg_config,
    schema_stg_config.get_schema() + data["table"][0],
    schema_stg_config.get_schema() + data["table"][1],
    schema_stg_config.get_schema() + data["table"][2],
    schema_stg_config.get_schema() + data["table"][3],
    schema_stg_config.get_schema() + data["table"][4],
    schema_stg_config.get_schema() + data["table"][5],
    schema_stg_config.get_schema() + data["table"][6],
    schema_stg_config.get_schema() + data["table"][7],
    schema_stg_config.get_schema() + data["table"][8],
)

DDSschema_config = DdsSchema(
    schema_dds_config,
    schema_dds_config.get_schema() + data["table_dds"][0],
    schema_dds_config.get_schema() + data["table_dds"][1],
    schema_dds_config.get_schema() + data["table_dds"][2],
    schema_dds_config.get_schema() + data["table_dds"][3],
    schema_dds_config.get_schema() + data["table_dds"][4],
    schema_dds_config.get_schema() + data["table_dds"][5],
    schema_dds_config.get_schema() + data["table_dds"][6],
    schema_dds_config.get_schema() + data["table_dds"][7],
)


task_file = Task(
    data["DAG"]["task"]["stg"][0],
    data["DAG"]["task"]["stg"][1],
    data["DAG"]["task"]["stg"][2],
    data["DAG"]["task"]["stg"][3],
    data["DAG"]["task"]["stg"][4],
    data["DAG"]["task"]["stg"][5],
    data["DAG"]["task"]["stg"][6],
    data["DAG"]["task"]["stg"][7],
    data["DAG"]["task"]["stg"][8],
)

dq_task_file = DqTask(data["DAG"]["task"]["dq"][0])

dagConfig = DAGTasks(data["DAG"]["conn_id"],
                     data["DAG"]["pool"],
                     data["DAG"]["jars"],
                     task_file)

dagDqConfig = DAGTasks(data["DAG"]["conn_id"],
                       data["DAG"]["pool"],
                       data["DAG"]["jars"],
                       dq_task_file)

dag_name = DagName(data["DAG"]["name"][0],
                   data["DAG"]["name"][1],
                   data["DAG"]["name"][2])
