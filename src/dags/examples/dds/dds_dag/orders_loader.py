import json
from datetime import datetime

from lib import PgConnect
from logging import Logger

from examples.dds.dds_dag.dds_settings_repository import DDSEtlSettingsRepository, EtlSetting
from examples.dds.dds_dag.rest_loader import RestDestRepository
from examples.dds.dds_dag.ts_loader import TimestampDdsRepository
from examples.dds.dds_dag.users_loader import UserDestRepository
from examples.dds.dds_dag.order_repositories import (OrderDdsObj, OrderDdsRepository, OrderJsonObj,
                                OrderRawRepository)


class OrderLoader:
    WF_KEY = "orders_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg: PgConnect, settings_repository: DDSEtlSettingsRepository, log: Logger) -> None:
        self.dwh = pg
        self.raw = OrderRawRepository()
        self.dds_users = UserDestRepository()
        self.dds_timestamps = TimestampDdsRepository()
        self.dds_restaurants = RestDestRepository()  
        self.dds_orders = OrderDdsRepository()
        self.settings_repository = settings_repository
        self.log = log
    def parse_order(self, order_raw: OrderJsonObj, restaurant_id: int, timestamp_id: int, user_id: int, delivery_id: int) -> OrderDdsObj:
        order_json = json.loads(order_raw.object_value)
        t = OrderDdsObj(id=0,
                        order_key=order_json['_id'],
                        restaurant_id=restaurant_id,
                        timestamp_id=timestamp_id,
                        user_id=user_id,
                        delivery_id=delivery_id,
                        order_status=order_json['final_status']
                        )

        return t
    
    def load_orders(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

            load_queue = self.raw.load_raw_orders(conn, last_loaded_id)
            load_queue.sort(key=lambda x: x.id)
            for order_raw in load_queue:
                order_json = json.loads(order_raw.object_value)
                
                restaurant = self.dds_restaurants.get_rest(conn, str(order_json['restaurant']['id']))
                if not restaurant:
                    break

                dt = datetime.strptime(order_json['date'], "%Y-%m-%d %H:%M:%S")
                timestamp = self.dds_timestamps.get_timestamp2(conn, dt)
                if not timestamp:
                    break

                user = self.dds_users.get_user(conn, order_json['user']['id'])
                if not user:
                    break
                delivery = self.dds_orders.get_delivery(conn, order_json['_id'])
                self.log.info('LOG11' + str(delivery))
                if delivery is None:
                    delivery = -1
                self.log.info('LOG2' + str(delivery))
                order_to_load = self.parse_order(order_raw, restaurant[0], timestamp.id, user[0], delivery)
                self.dds_orders.insert_order(conn, order_to_load)

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = order_raw.id
                self.settings_repository.save_setting(conn, wf_setting)
