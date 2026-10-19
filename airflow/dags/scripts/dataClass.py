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
class DAGTask:
    etl_py: str
    transform_py: str
    dds__transform__fact_payments_py: str
    dds__transform__fact_reviews_py: str
    dds__transform__dim_orders_py: str
    dds__transform__dim_products_py: str
    dds__transform__dim_customers_py: str
    dds__transform__dim_sellers_py: str
    dds__transform__dim_geolocation_py: str


def load_yaml():
    with open(PATH_NAME, "r") as f:
        return yaml.load(f, Loader=SafeLoader)


data = load_yaml()

STGschema = StgSchema(
    Schema(data["schemas"][0]),
    data["table"][0],
    data["table"][1],
    data["table"][2],
    data["table"][3],
    data["table"][4],
    data["table"][5],
    data["table"][6],
    data["table"][7],
    data["table"][8],
)

DDSschema = DdsSchema(
    Schema(data["schemas"][1]),
    data["table_dds"][0],
    data["table_dds"][1],
    data["table_dds"][2],
    data["table_dds"][3],
    data["table_dds"][4],
    data["table_dds"][5],
    data["table_dds"][6],
    data["table_dds"][7],
)

DAGtask_file = DAGTask(
    data["dag_task"][0],
    data["dag_task"][1],
    data["dag_task"][2],
    data["dag_task"][3],
    data["dag_task"][4],
    data["dag_task"][5],
    data["dag_task"][6],
    data["dag_task"][7],
    data["dag_task"][8],
)
