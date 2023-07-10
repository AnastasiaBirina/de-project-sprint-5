from typing import List, Optional

from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel


class OrderJsonObj(BaseModel):
    id: int
    object_id: str
    object_value: str


class OrderRawRepository:
    def load_raw_orders(self, conn: Connection, last_loaded_record_id: int) -> List[OrderJsonObj]:
        with conn.cursor(row_factory=class_row(OrderJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value
                    FROM stg.ordersystem_orders
                    WHERE id > %(last_loaded_record_id)s
                    ORDER BY id ASC;
                """,
                {"last_loaded_record_id": last_loaded_record_id},
            )
            objs = cur.fetchall()
        objs.sort(key=lambda x: x.id)
        return objs


class OrderDdsObj(BaseModel):
    id: int
    order_key: str
    restaurant_id: int
    timestamp_id: int
    user_id: int
    order_status: str
    delivery_id: int


class OrderDdsRepository:

    def insert_order(self, conn: Connection, order: OrderDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_orders(order_key, restaurant_id, timestamp_id, user_id, order_status, delivery_id)
                    VALUES (%(order_key)s, %(restaurant_id)s, %(timestamp_id)s, %(user_id)s, %(order_status)s, %(delivery_id)s)
                    ON CONFLICT (order_key) DO UPDATE
                    SET
                        restaurant_id = EXCLUDED.restaurant_id,
                        timestamp_id = EXCLUDED.timestamp_id,
                        user_id = EXCLUDED.user_id,
                        order_status = EXCLUDED.order_status,
                        delivery_id = EXCLUDED.delivery_id
                    ;
                """,
                {
                    "order_key": order.order_key,
                    "restaurant_id": order.restaurant_id,
                    "timestamp_id": order.timestamp_id,
                    "user_id": order.user_id,
                    "order_status": order.order_status,
                    "delivery_id": order.delivery_id
                    
                },
            )

    def get_order(self, conn: Connection, order_id: str) -> Optional[OrderDdsObj]:
        with conn.cursor(row_factory=class_row(OrderDdsObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        order_key,
                        restaurant_id,
                        timestamp_id,
                        user_id,
                        order_status,
                        delivery_id
                    FROM dds.dm_orders
                    WHERE order_key = %(order_id)s;
                """,
                {"order_id": order_id},
            )
            obj = cur.fetchone()
        return obj

    def get_delivery(self, conn: Connection, delivery_id: str):
        with conn.cursor(row_factory=class_row(OrderDdsObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id
                    FROM dds.dm_deliveries
                    WHERE delivery_id = %(delivery_id)s;
                """,
                {"delivery_id": delivery_id},
            )
            obj = cur.fetchone()
        return obj
