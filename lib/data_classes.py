from dataclasses import dataclass
from datetime import datetime


@dataclass
class Provider:
    ID: int
    Name: str
    TableName: str = "shop.provider"


@dataclass
class Storage:
    ID: int
    Name: str
    Address: str
    TableName: str = "shop.storage"


@dataclass
class Delivery:
    ID: int
    ProviderID: int
    StorageID: int
    DeliveryDateTime: datetime
    TableName: str = "shop.delivery"


@dataclass
class Invoice:
    ID: int
    StorageID: int
    InvoiceDateTime: datetime
    TableName: str = "shop.invoice"


@dataclass
class Product:
    ID: int
    Name: str
    Code: int
    TableName: str = "shop.product"


@dataclass
class DeliveryProductsLine:
    ID: int
    ProductID: int
    DeliveryID: int
    Quantity: int
    TableName: str = "shop.delivery_products_line"


@dataclass
class InvoiceProductsLine:
    ID: int
    ProductID: int
    InvoiceID: int
    Quantity: int
    TableName: str = "shop.invoice_products_line"
