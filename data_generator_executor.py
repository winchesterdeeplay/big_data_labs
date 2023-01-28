from os.path import join
from pathlib import Path

import keyring

from lib.clickhouse_hook import ClickhouseNativeHook
from lib.data_generator import ShopDataGenerator
from lib.load_query import load_query
import logging
import random

HOST = "ch_server"  # change to localhost
# HOST = "localhost"
GENERATE_DATA = True
QUERIES_PATH = "queries"
DIR_PATH = Path(__file__).parent.resolve()
SEED = 42

ENTITIES = [
    "delivery",
    "delivery_products_line",
    "invoice",
    "invoice_products_line",
    "product",
    "provider",
    "storage",
    "shop",
]

# keyring example, in prod version should be hide
keyring.set_password("clickhouse_lab1", "username", "default")
keyring.set_password("clickhouse_lab1", "password", "default")

CLICKHOUSE_LOGIN = keyring.get_password("clickhouse_lab1", "username")
CLICKHOUSE_PASSWORD = keyring.get_password("clickhouse_lab1", "password")

if __name__ == "__main__":
    random.seed(SEED)
    client = ClickhouseNativeHook(login=CLICKHOUSE_LOGIN, password=CLICKHOUSE_PASSWORD, host=HOST).get_connection()
    # create tables if not exists
    for entity in ENTITIES:
        query = load_query(join(DIR_PATH, join(QUERIES_PATH, f"{entity}.sql")))
        client.execute(query=query)

    logging.info("Tables created successfully.")
    # generate data
    if GENERATE_DATA:
        data_generator = ShopDataGenerator(clickhouse_connection=client)
        data_generator.execute()
        logging.info("Data inserted successfully.")
