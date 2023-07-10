import json
from datetime import datetime
from typing import List, Optional
from logging import Logger
from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel

from examples.dds.dds_dag.dds_settings_repository import DDSEtlSettingsRepository, EtlSetting
from examples.dds.dds_dag.rest_loader import RestJsonObj, RestDdsObj, RestDestRepository

class ProductDdsObj(BaseModel):
    id: int

    product_id: str
    product_name: str
    product_price: float

    active_from: datetime
    active_to: datetime

    restaurant_id: int


class ProductsRawRepository:
    def load_raw_products(self, conn: Connection, last_loaded_record_id: int) -> List[RestJsonObj]:
        with conn.cursor(row_factory=class_row(RestJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value,
                        update_ts
                    FROM stg.ordersystem_restaurants
                    WHERE id > %(last_loaded_record_id)s;
                """,
                {"last_loaded_record_id": last_loaded_record_id},
            )
            objs = cur.fetchall()
        return objs

class ProductDdsRepository:
    def insert_dds_products(self, conn: Connection, products: List[ProductDdsObj]) -> None:
        with conn.cursor() as cur:
            for product in products:
                cur.execute(
                    """
                        INSERT INTO dds.dm_products(
                            product_id,
                            product_name,
                            product_price,
                            active_from,
                            active_to,
                            restaurant_id)
                        VALUES (
                            %(product_id)s,
                            %(product_name)s,
                            %(product_price)s,
                            %(active_from)s,
                            %(active_to)s,
                            %(restaurant_id)s);
                    """,
                    {
                        "product_id": product.product_id,
                        "product_name": product.product_name,
                        "product_price": product.product_price,
                        "active_from": product.active_from,
                        "active_to": product.active_to,
                        "restaurant_id": product.restaurant_id
                    },
                )

    def get_product(self, conn: Connection, product_id: str) -> Optional[ProductDdsObj]:
        with conn.cursor(row_factory=class_row(ProductDdsObj)) as cur:
            cur.execute(
                """
                    SELECT id, product_id, product_name, product_price, active_from, active_to, restaurant_id
                    FROM dds.dm_products
                    WHERE product_id = %(product_id)s;
                """,
                {"product_id": product_id},
            )
            obj = cur.fetchone()
            
        return obj

    def list_products(self, conn: Connection) -> List[ProductDdsObj]:
        with conn.cursor(row_factory=class_row(ProductDdsObj)) as cur:
            cur.execute(
                """
                    SELECT id, product_id, product_name, product_price, active_from, active_to, restaurant_id
                    FROM dds.dm_products;
                """
            )
            obj = cur.fetchall()
        return obj


class ProductLoader:
    WF_KEY = "menu_products_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg: PgConnect, settings_repository: DDSEtlSettingsRepository, log: Logger) -> None:
        self.dwh = pg
        self.raw = ProductsRawRepository()
        self.dds_products = ProductDdsRepository()
        self.dds_restaurants = RestDestRepository()
        self.settings_repository = settings_repository
        self.log = log

    def parse_restaurants_menu(self, restaurant_raw: RestJsonObj, restaurant_version_id: int) -> List[ProductDdsObj]:
        res = []
        rest_json = json.loads(restaurant_raw.object_value)
        for prod_json in rest_json['menu']:
            t = ProductDdsObj(id=0,
                              product_id=prod_json['_id'],
                              product_name=prod_json['name'],
                              product_price=prod_json['price'],
                              active_from=datetime.strptime(rest_json['update_ts'], "%Y-%m-%d %H:%M:%S"),
                              active_to=datetime(year=2099, month=12, day=31),
                              restaurant_id=restaurant_version_id
                              )

            res.append(t)
        return res

    def load_products(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            
            
            load_queue = self.raw.load_raw_products(conn, last_loaded_id)
            # self.log.info('LOG' + str(load_queue))
            load_queue.sort(key=lambda x: x.id)
        
            products = self.dds_products.list_products(conn)
            prod_dict = {}
            for p in products:
                prod_dict[p.product_id] = p
            
        
            for restaurant in load_queue:
                
                restaurant_version = self.dds_restaurants.get_rest(conn, restaurant.object_id)
                
                if not restaurant_version:
                    return
                self.log.info("------------ LOG 2" + str(restaurant_version[0]) + " -------------")
        
                products_to_load = self.parse_restaurants_menu(restaurant, restaurant_version[0])
                # self.log.info("------------ LOG 3" + str(products_to_load) + " -------------")
            
                products_to_load = [p for p in products_to_load if p.product_id not in prod_dict]
                self.dds_products.insert_dds_products(conn, products_to_load)

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = restaurant.id
                self.settings_repository.save_setting(conn, wf_setting)
