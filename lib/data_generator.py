import logging
import time
from dataclasses import asdict
from datetime import datetime
from random import randrange, choice, random
from typing import Dict, Any, Tuple, Optional

import pandas as pd
from clickhouse_driver import Client
from pandas import DataFrame

from lib.data_classes import (
    Provider,
    Storage,
    Delivery,
    Invoice,
    Product,
    DeliveryProductsLine,
    InvoiceProductsLine,
)


class DataGeneratorClickhouse:
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

    def __init__(self, clickhouse_connection: Client):
        self.client = clickhouse_connection

    def execute(self) -> None:
        raise NotImplementedError

    def generate_next_uint_id(self, table_name: str, id_field_name: str = "ID") -> int:
        """Simple autoincrement implementation"""
        next_id = 1
        query = f"SELECT max({id_field_name}) FROM {table_name}"
        current_max_id = self.client.execute(query=query)[0][0]
        if current_max_id not in (0, None):
            next_id = current_max_id + 1
        return next_id

    def check_name_value_available(self, new_name: str, field_name: str, table_name: str) -> bool:
        query = f"""WITH
Names AS (SELECT {field_name} FROM {table_name} GROUP BY {field_name})
SELECT if({new_name!r} IN Names, 1, 0) AS Result"""
        return self.client.execute(query=query)[0][0]

    @staticmethod
    def generate_random_uint_value(min_value: int = 0, max_value: int = 2000) -> int:
        return randrange(min_value, max_value)

    @staticmethod
    def generate_random_datetime(
        min_value: str = "2022-01-01 00:00:00",
        max_value: str = "2022-12-31 00:00:00",
        time_format: str = "%Y-%m-%d %H:%M:%S",
    ) -> datetime:
        stime = time.mktime(time.strptime(min_value, time_format))
        etime = time.mktime(time.strptime(max_value, time_format))

        ptime = stime + random() * (etime - stime)
        return datetime.strptime(time.strftime(time_format, time.localtime(ptime)), time_format)


class ShopDataGenerator(DataGeneratorClickhouse):
    PROVIDER_NAMES = ["China", "Russia", "USA", "Canada", "EU", "Africa", "Thailand", "Australia", "Georgia"]
    STORAGE_NAMES = ["Warehouse", "Shop", "Secret", "Home", "Virtual"]
    ADDRESSES_NAMES = ["Vladivostok", "Moscow", "Petersburg", "Yekaterinburg", "Yakutsk", "Sakhalin"]
    PRODUCT_NAMES = ["Milk", "Computer", "Tea", "Coffee", "Sweet", "Beer", "Wear"]

    def __init__(
        self,
        insert_query="INSERT INTO {table} VALUES",
        select_id_query="SELECT {id_field_name} FROM {table}",
        id_field_name: str = "ID",
        provider_count: int = 50,
        storage_count: int = 50,
        delivery_count: int = 100,
        product_count: int = 100,
        delivery_products_line_count: int = 70,
        invoice_product_line_count: int = 50,
        provider_table_name: str = "shop.provider",
        provider_name_field_name: str = "Name",
        storage_table_name: str = "shop.storage",
        storage_name_field_name: str = "Name",
        storage_address_field_name: str = "Address",
        delivery_table_name: str = "shop.delivery",
        delivery_provider_id_field_name: str = "ProviderID",
        delivery_storage_id_field_name: str = "StorageID",
        delivery_delivery_date_time_field_name: str = "DeliveryDateTime",
        product_table_name: str = "shop.product",
        product_name_field_name: str = "Name",
        product_code_field_name: str = "Code",
        delivery_products_line_table_name: str = "shop.delivery_products_line",
        delivery_products_line_product_id_field_name: str = "ProductID",
        delivery_products_line_delivery_id_field_name: str = "DeliveryID",
        delivery_products_line_quantity_field_name: str = "Quantity",
        invoice_products_line_table_name: str = "shop.invoice_products_line",
        invoice_products_line_quantity_field_name: str = "Quantity",
        invoice_products_line_product_id_field_name: str = "ProductID",
        invoice_products_line_invoice_id_field_name: str = "InvoiceID",
        invoice_table_name: str = "shop.invoice",
        invoice_storage_id_field_name: str = "StorageID",
        invoice_invoice_date_time_field_name: str = "InvoiceDateTime",
        logger: Any = None,
        **kwargs,
    ):
        """
        Data Generator for default shop database.

        :param insert_query: insert query for clickhouse 'insert_dataframe' method
        :param select_id_query: query to select IDs from specified table
        :param id_field_name: ID field name for entities
        :param provider_count: provider entities quantity to generate
        :param storage_count: storage entities quantity to generate
        :param delivery_count: delivery entities quantity to generate
        :param product_count: product entities quantity to generate
        :param delivery_products_line_count: delivery_products_line entities quantity to generate
        :param invoice_product_line_count: invoice_product_line_count entities quantity to generate
        ...
        """
        super().__init__(**kwargs)
        self.insert_query = insert_query
        self.select_id_query = select_id_query
        self.id_field_name = id_field_name
        self.provider_count = provider_count
        self.storage_count = storage_count
        self.delivery_count = delivery_count
        self.product_count = product_count
        self.delivery_products_line_count = delivery_products_line_count
        self.invoice_product_line_count = invoice_product_line_count

        self.provider_table_name = provider_table_name
        self.provider_name_field_name = provider_name_field_name

        self.storage_name_field_name = storage_name_field_name
        self.storage_table_name = storage_table_name
        self.storage_address_field_name = storage_address_field_name

        self.delivery_table_name = delivery_table_name
        self.delivery_provider_id_field_name = delivery_provider_id_field_name
        self.delivery_storage_id_field_name = delivery_storage_id_field_name
        self.delivery_delivery_date_time_field_name = delivery_delivery_date_time_field_name

        self.product_table_name = product_table_name
        self.product_name_field_name = product_name_field_name
        self.product_code_field_name = product_code_field_name

        self.delivery_products_line_table_name = delivery_products_line_table_name
        self.delivery_products_line_product_id_field_name = delivery_products_line_product_id_field_name
        self.delivery_products_line_delivery_id_field_name = delivery_products_line_delivery_id_field_name
        self.delivery_products_line_quantity_field_name = delivery_products_line_quantity_field_name

        self.invoice_products_line_table_name = invoice_products_line_table_name
        self.invoice_products_line_quantity_field_name = invoice_products_line_quantity_field_name
        self.invoice_products_line_product_id_field_name = invoice_products_line_product_id_field_name
        self.invoice_products_line_invoice_id_field_name = invoice_products_line_invoice_id_field_name

        self.invoice_table_name = invoice_table_name
        self.invoice_storage_id_field_name = invoice_storage_id_field_name
        self.invoice_invoice_date_time_field_name = invoice_invoice_date_time_field_name

        self.logger = logger if logger is not None else logging.getLogger()

        self.products_cache = None
        self.product_cache = None
        self.delivery_cache = None

    def generate_provider_row(self, to_insert: bool = True) -> Provider:
        name_available_state = 1
        new_name = ""
        new_id = self.generate_next_uint_id(table_name=self.provider_table_name, id_field_name=self.id_field_name)
        while name_available_state:
            new_name = f"{choice(self.PROVIDER_NAMES)}_{self.generate_random_uint_value()}"
            name_available_state = self.check_name_value_available(
                new_name, self.provider_name_field_name, self.provider_table_name
            )
            if name_available_state and self.logger:
                self.logger.warning(f"Попытка добавить существующего провайдера, отказано. {new_name}")
        provider = Provider(ID=new_id, Name=new_name)
        if to_insert:
            self._insert_record(asdict(provider), self.provider_table_name)
        return provider

    def generate_storage_row(self, to_insert: bool = True) -> Storage:
        name_available_state, address_available_state = 1, 1
        new_name, new_address = "", ""

        new_id = self.generate_next_uint_id(table_name=self.storage_table_name, id_field_name=self.id_field_name)

        while name_available_state:
            new_name = f"{choice(self.STORAGE_NAMES)}_{self.generate_random_uint_value()}"
            name_available_state = self.check_name_value_available(
                new_name, self.storage_name_field_name, self.storage_table_name
            )
            if name_available_state and self.logger:
                self.logger.warning(f"Попытка добавить существующий склад, отказано. {new_name}")

        while address_available_state:
            new_address = f"{choice(self.ADDRESSES_NAMES)}_{self.generate_random_uint_value()}"
            address_available_state = self.check_name_value_available(
                new_address, self.storage_address_field_name, self.storage_table_name
            )
        storage = Storage(ID=new_id, Name=new_name, Address=new_address)
        if to_insert:
            self._insert_record(asdict(storage), self.storage_table_name)
        return storage

    def generate_delivery_row(self, to_insert: bool = True) -> Delivery:
        new_id = self.generate_next_uint_id(table_name=self.delivery_table_name, id_field_name=self.id_field_name)
        new_datetime = self.generate_random_datetime()
        select_storage_query = self.select_id_query.format(
            id_field_name=self.id_field_name, table=self.storage_table_name
        )
        select_provider_query = self.select_id_query.format(
            id_field_name=self.id_field_name, table=self.provider_table_name
        )

        storages = self.client.query_dataframe(select_storage_query)
        providers = self.client.query_dataframe(select_provider_query)

        provider, storage = providers.sample(n=1)[self.id_field_name], storages.sample(n=1)[self.id_field_name]
        delivery = Delivery(ID=new_id, StorageID=storage, ProviderID=provider, DeliveryDateTime=new_datetime)
        if to_insert:
            self._insert_record(asdict(delivery), self.delivery_table_name)
        return delivery

    def generate_product_row(
        self, to_insert: bool = True, min_value: int = 10000000, max_value: int = 99999999
    ) -> Product:
        code_available_state = 1
        new_code = 10000000
        new_id = self.generate_next_uint_id(table_name=self.product_table_name, id_field_name=self.id_field_name)
        new_name = f"{choice(self.PRODUCT_NAMES)}_{self.generate_random_uint_value()}"
        while code_available_state:
            new_code = self.generate_random_uint_value(min_value=min_value, max_value=max_value)
            code_available_state = self.check_name_value_available(
                new_name, self.product_code_field_name, self.product_table_name
            )
            if code_available_state and self.logger:
                self.logger.warning(f"Попытка добавить товар с неуникальным кодом, отказано. {new_code}")
        product = Product(ID=new_id, Name=new_name, Code=new_code)
        if to_insert:
            self._insert_record(asdict(product), self.product_table_name)
        return product

    def generate_delivery_product_line_row(
        self, to_insert: bool = True, min_quantity: int = 1, max_quantity: int = 100
    ) -> DeliveryProductsLine:
        new_id = self.generate_next_uint_id(
            table_name=self.delivery_products_line_table_name, id_field_name=self.id_field_name
        )
        select_product_query = self.select_id_query.format(
            id_field_name=self.id_field_name, table=self.product_table_name
        )
        select_delivery_query = self.select_id_query.format(
            id_field_name=self.id_field_name, table=self.delivery_table_name
        )
        if self.product_cache is None and self.delivery_cache is None:
            products = self.client.query_dataframe(select_product_query)
            deliveries = self.client.query_dataframe(select_delivery_query)
            self.product_cache = products
            self.delivery_cache = deliveries
        else:
            products = self.product_cache
            deliveries = self.delivery_cache

        product, delivery = products.sample(n=1)[self.id_field_name], deliveries.sample(n=1)[self.id_field_name]

        quantity = self.generate_random_uint_value(min_value=min_quantity, max_value=max_quantity)

        delivery_products_line = DeliveryProductsLine(
            ID=new_id, ProductID=product, DeliveryID=delivery, Quantity=quantity
        )

        if to_insert:
            self._insert_record(asdict(delivery_products_line), self.delivery_products_line_table_name)

        return delivery_products_line

    def generate_invoice__invoice_product_line_rows(
        self, to_insert: bool = True
    ) -> Optional[Tuple[InvoiceProductsLine, Invoice]]:
        new_id = self.generate_next_uint_id(
            table_name=self.invoice_products_line_table_name, id_field_name=self.id_field_name
        )

        new_invoice_id = self.generate_next_uint_id(
            table_name=self.invoice_table_name, id_field_name=self.id_field_name
        )

        select_product_query = self.select_id_query.format(
            id_field_name=self.delivery_products_line_product_id_field_name,
            table=self.delivery_products_line_table_name,
        )
        if self.products_cache is None:
            products = self.client.query_dataframe(select_product_query)
            self.products_cache = products
        else:
            products = self.products_cache
        product = int(products.sample(n=1)[self.delivery_products_line_product_id_field_name])

        delivery_quantity_sum_by_storage_id_query = f"""SELECT 
	StorageID, 
	sum(Quantity) AS QuantitySum, 
	min(DeliveryDateTime) AS DeliveryDateTimeMin 
FROM(
	SELECT 
	    {self.delivery_products_line_quantity_field_name} AS Quantity, 
	    {self.delivery_products_line_delivery_id_field_name} AS DeliveryID
	FROM {self.delivery_products_line_table_name}
	WHERE {self.delivery_products_line_product_id_field_name}  == {product}
) AS Main
LEFT JOIN 
(
	SELECT 
	    {self.id_field_name} AS ID, 
	    {self.delivery_storage_id_field_name} AS StorageID, 
	    {self.delivery_delivery_date_time_field_name} AS DeliveryDateTime 
	FROM {self.delivery_table_name}
) AS Delivery
ON Main.DeliveryID == Delivery.ID
GROUP BY StorageID;"""

        invoice_quantity_sum_by_storage_id_query = f"""SELECT 
	StorageID, 
	sum(Quantity) AS QuantitySum 
FROM(
	SELECT 
	    {self.invoice_products_line_quantity_field_name} AS Quantity, 
	    {self.invoice_products_line_invoice_id_field_name} AS InvoiceID 
	FROM {self.invoice_products_line_table_name}
	WHERE {self.invoice_products_line_product_id_field_name}  == {product}
) AS Main
LEFT JOIN 
(
	SELECT 
	    {self.id_field_name} AS ID,
	    {self.invoice_storage_id_field_name} AS StorageID 
	FROM {self.invoice_table_name}
) AS Invoice
ON Main.InvoiceID == Invoice.ID
GROUP BY StorageID;"""
        delivery_quantity = self.client.query_dataframe(query=delivery_quantity_sum_by_storage_id_query)
        invoice_quantity = self.client.query_dataframe(query=invoice_quantity_sum_by_storage_id_query)
        if invoice_quantity.empty:
            max_value = delivery_quantity.sort_values("QuantitySum", ascending=False).iloc[0, :]
            quantity = self.generate_random_uint_value(min_value=1, max_value=int(max_value["QuantitySum"] * 1.5))
            if quantity > int(max_value["QuantitySum"]):
                if self.logger:
                    self.logger.warning(
                        f"Попытка сделать заказ большего количества чем есть, отказано. {quantity} > {int(max_value['QuantitySum'])}"
                    )
                return
        else:
            merged = pd.merge(delivery_quantity, invoice_quantity, on="StorageID", suffixes=("_x", "_y"))
            merged["QuantityDiff"] = merged["QuantitySum_x"] - merged["QuantitySum_y"]
            max_value = merged.sort_values("QuantityDiff", ascending=False).iloc[0, :]
            if int(max_value["QuantityDiff"]) < 0:
                if self.logger:
                    self.logger.error("Ошибка оформления заказа")
                return
            if int(max_value["QuantityDiff"]) == 1:
                quantity = 1
            else:
                quantity = self.generate_random_uint_value(min_value=1, max_value=int(max_value["QuantityDiff"] * 1.5))
                if quantity > int(max_value["QuantityDiff"]):
                    if self.logger:
                        self.logger.warning(
                            f"Попытка сделать заказ большего количества чем есть, отказано. {quantity} > {int(max_value['QuantityDiff'])}"
                        )
                    return
        storage_id = int(max_value["StorageID"])
        min_datetime = pd.to_datetime(max_value["DeliveryDateTimeMin"]).strftime(self.DATETIME_FORMAT)
        invoice_datetime = self.generate_random_datetime(min_value=min_datetime, time_format=self.DATETIME_FORMAT)
        invoice = Invoice(ID=new_invoice_id, InvoiceDateTime=invoice_datetime, StorageID=storage_id)
        invoice_product_line = InvoiceProductsLine(
            ID=new_id, ProductID=product, Quantity=quantity, InvoiceID=new_invoice_id
        )

        if to_insert:
            self._insert_record(asdict(invoice), self.invoice_table_name)
            self._insert_record(asdict(invoice_product_line), self.invoice_products_line_table_name)

        return invoice_product_line, invoice

    def execute(self) -> None:
        for _ in range(self.provider_count):
            self.generate_provider_row()
        self.logger.info("Providers successfully inserted.")
        for _ in range(self.storage_count):
            self.generate_storage_row()
        self.logger.info("Storages successfully inserted.")
        for _ in range(self.delivery_count):
            self.generate_delivery_row()
        self.logger.info("Deliveries successfully inserted.")
        for _ in range(self.product_count):
            self.generate_product_row()
        self.logger.info("Products successfully inserted.")
        for _ in range(self.delivery_products_line_count):
            self.generate_delivery_product_line_row()
        self.logger.info("Delivery_product_lines successfully inserted.")
        for _ in range(self.invoice_product_line_count):
            self.generate_invoice__invoice_product_line_rows()
        self.logger.info("Invoices successfully inserted.")

    def _insert_record(self, record: Dict[str, Any], table_name: str) -> None:
        self.client.insert_dataframe(
            query=self.insert_query.format(table=table_name),
            dataframe=DataFrame.from_records(data=[record]),
            settings={"use_numpy": True},
        )
