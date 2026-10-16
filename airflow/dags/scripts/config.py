import os
from pathlib import Path
from dotenv import load_dotenv
import yaml
from yaml.loader import SafeLoader

load_dotenv(Path(__file__).parent / ".env")
PATH_NAME = Path(__file__).parent.parent / "schemas_table_name.yml"

def load_yaml():
    with open(PATH_NAME, "r") as f:
        return yaml.load(f, Loader=SafeLoader)

DATA = load_yaml()

POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
GP_URL = os.getenv("GP_URL") + "/" + POSTGRES_DB
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
DRIVER = "org.postgresql.Driver"

OPTIONS = {
    "url": GP_URL,
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": DRIVER
}

OPTIONS_WRITE = dict(OPTIONS)
OPTIONS_WRITE["batchsize"] = 2000
OPTIONS_WRITE["reWriteBatchedInserts"] = "true"