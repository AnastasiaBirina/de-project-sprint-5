# from logging import Logger
# from typing import List
# import json
# from typing import List, Optional
# from examples.dds import EtlSetting, DDSEtlSettingsRepository
# from lib import PgConnect
# from lib.dict_util import json2str
# from psycopg import Connection
# from psycopg.rows import class_row
# from pydantic import BaseModel
# from datetime import datetime, time, date


# class TSJsonObj(BaseModel):
#     id: int
#     object_id: str
#     object_value: str


# class TSDdsObj(BaseModel):
#     id: int
#     dt: datetime
#     year: int
#     month: int
#     day: int
#     time: time
#     date: date


# # CREATE TABLE dds.dm_timestamps (
# # 	id serial NOT null primary key,
# # 	ts timestamp NOT null,
# # 	year smallint CHECK(year >= 2022 and year < 2500) NOT null,
# # 	month smallint CHECK(month >= 1 and month <= 12) NOT null,
# # 	day smallint CHECK(day >= 1 and day <= 31) NOT null,
# # 	time  time NOT null,
# # 	date date NOT null
# # );
# class TSRawRepository:
#     def load_raw_ts(self, conn: Connection, last_loaded_record_id: int) -> List[TSJsonObj]:
#         with conn.cursor(row_factory=class_row(TSJsonObj)) as cur:
#             cur.execute(
#                 """
#                     SELECT
#                         id,
#                         object_id,
#                         object_value
#                     FROM stg.ordersystem_orders
#                     WHERE id > %(last_loaded_record_id)s
#                     ORDER BY id ASC;
#                 """,
#                 {"last_loaded_record_id": last_loaded_record_id},
#             )
#             objs = cur.fetchall()
#         objs.sort(key=lambda x: x.id)
#         return objs

# class TSDestRepository:

#     def insert_ts(self, conn: Connection, ts: TSDdsObj) -> None:
#         with conn.cursor() as cur:
#             cur.execute(
#                 """
#                     INSERT INTO dds.dm_timestamps(ts, year, month, day, time, date)
#                     VALUES (%(dt)s, %(year)s, %(month)s, %(day)s, %(time)s, %(date)s)
                    
#                 """,
#                 {
#                     "ts": ts.dt,
#                     "year": ts.year,
#                     "month": ts.month,
#                     "day": ts.day,
#                     "time": ts.time,
#                     "date": ts.date
#                 },
#             )
#     # def get_ts(self, conn: Connection, dt: datetime) -> Optional[TSDdsObj]:
#     #     with conn.cursor() as cur:
#     #         cur.execute(
#     #             """
#     #                 SELECT ts, year, month, day, time, date
#     #                 FROM dds.dm_timestamps
#     #                 WHERE ts = (%(dt)s)
#     #             """,
#     #             {
#     #                 "ts": dt
#     #             }
#     #         )


# class TSLoader:
#     WF_KEY = "ts_from_stg_to_dds_workflow"
#     LAST_LOADED_ID_KEY = "last_loaded_id"
    
#     def __init__(self, pg_conn: PgConnect, settings_repository: DDSEtlSettingsRepository, log: Logger) -> None:
#         self.conn = pg_conn
#         self.dds = TSDestRepository()
#         self.raw = TSRawRepository()
#         self.settings_repository = settings_repository
#         self.log = log

#     def parse_order_ts(self, order_raw: TSJsonObj) -> TSDdsObj:
#         order_json = json.loads(order_raw.object_value)
#         dt = datetime.strptime(order_json['date'], "%Y-%m-%d %H:%M:%S")
#         self.log.info("------------ERROR DATE [ " + order_json['date'] + " ]-------------")
#         t = TSDdsObj(id=0,
#                             ts=dt,
#                             year=dt.year,
#                             month=dt.month,
#                             day=dt.day,
#                             time=dt.time(),
#                             date=dt.date()
#                             )

#         return t
    
#     def load_ts(self):
#         with self.conn.connection() as conn:
#             wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
#             if not wf_setting:
#                 wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

#             last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

#             load_queue = self.raw.load_raw_ts(conn, last_loaded_id)
#             for ts in load_queue:

#                 ts_to_load = self.parse_order_ts(ts)
#                 self.dds.insert_ts(conn, ts_to_load)

#                 wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = ts.id
#                 self.settings_repository.save_setting(conn, wf_setting)
                

import json
from datetime import date, datetime, time
from typing import Optional

from lib import PgConnect
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
import logging

from examples.dds.dds_dag.dds_settings_repository import DDSEtlSettingsRepository, EtlSetting
from examples.dds.dds_dag.order_repositories import OrderJsonObj, OrderRawRepository

log = logging.getLogger(__name__)

class TimestampDdsObj(BaseModel):
    id: int
    ts: datetime
    year: int
    month: int
    day: int
    time: time
    date: date


class TimestampDdsRepository:
    def insert_dds_timestamp(self, conn: Connection, timestamp: TimestampDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_timestamps(ts, year, month, day, time, date)
                    VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(time)s, %(date)s)
                    ;
                """,
                {
                    "ts": timestamp.ts,
                    "year": timestamp.year,
                    "month": timestamp.month,
                    "day": timestamp.day,
                    "time": timestamp.time,
                    "date": timestamp.date
                },
            )

    def get_timestamp(self, conn: Connection, timestamp: TimestampDdsObj) -> Optional[TimestampDdsObj]:
        with conn.cursor(row_factory=class_row(TimestampDdsObj)) as cur:
            cur.execute(
                """
                    SELECT id, ts, year, month, day, time, date
                    FROM dds.dm_timestamps
                    WHERE ts = %(dt)s;
                """,
                {"dt": timestamp.ts},
            )
            obj = cur.fetchone()
        return obj
    
    def get_timestamp2(self, conn: Connection, timestamp: datetime) -> Optional[TimestampDdsObj]:
        with conn.cursor(row_factory=class_row(TimestampDdsObj)) as cur:
            cur.execute(
                """
                    SELECT id, ts, year, month, day, time, date
                    FROM dds.dm_timestamps
                    WHERE ts = %(dt)s;
                """,
                {"dt": timestamp},
            )
            obj = cur.fetchone()
        return obj

class TimestampLoader:
    WF_KEY = "timestamp_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_order_id"

    def __init__(self, pg: PgConnect, settings_repository: DDSEtlSettingsRepository) -> None:
        self.dwh = pg
        self.raw_orders = OrderRawRepository()
        self.dds = TimestampDdsRepository()
        self.settings_repository = settings_repository

    def parse_order_ts(self, order_raw: OrderJsonObj) -> TimestampDdsObj:
        order_json = json.loads(order_raw.object_value)
        dt = datetime.strptime(order_json['date'], "%Y-%m-%d %H:%M:%S")
        t = TimestampDdsObj(id=0,
                            ts=dt,
                            year=dt.year,
                            month=dt.month,
                            day=dt.day,
                            time=dt.time(),
                            date=dt.date()
                            )

        return t

    def load_timestamps(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            log.info('LOG!! ' + str(last_loaded_id))
            load_queue = self.raw_orders.load_raw_orders(conn, last_loaded_id)
            for order in load_queue:
                ts_to_load = self.parse_order_ts(order)
                if not self.dds.get_timestamp(conn, ts_to_load):
                    self.dds.insert_dds_timestamp(conn, ts_to_load)

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = order.id
                self.settings_repository.save_setting(conn, wf_setting)
