from logging import Logger
from typing import List
import json
from typing import List, Optional
from examples.dds import EtlSetting, DDSEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from psycopg import Connection
from psycopg.rows import class_row
from pydantic import BaseModel
from datetime import datetime


class RestJsonObj(BaseModel):
    id: int
    object_id: str
    object_value: str
    update_ts: datetime


class RestDdsObj(BaseModel):
    id: int
    restaurant_id: str
    restaurant_name: str
    active_from: datetime
    active_to: datetime

# CREATE TABLE IF NOT EXISTS dds.dm_restaurants (
# 	id serial NOT null primary key,
# 	restaurant_id varchar NOT NULL,
# 	restaurant_name  varchar NOT NULL,
# 	active_from timestamp NOT null,
# 	active_to timestamp not null
# );
class RestRawRepository:
    def load_raw_rest(self, conn: Connection, last_loaded_record_id: int) -> List[RestJsonObj]:
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

class RestDestRepository:

    def insert_rest(self, conn: Connection, rest: RestDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
                    VALUES (%(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s)
                    
                """,
                {
                    "restaurant_id": rest.restaurant_id,
                    "restaurant_name": rest.restaurant_name,
                    "active_from": rest.active_from,
                    "active_to": rest.active_to 
                },
            )
    
    def get_rest(self, conn: Connection, restaurant_id: str) -> Optional[RestDdsObj]:
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT id, restaurant_id, restaurant_name, active_from, active_to
                    FROM dds.dm_restaurants
                    WHERE restaurant_id = (%(restaurant_id)s)
                """,
                {
                    "restaurant_id": restaurant_id
                }
            )
            obj = cur.fetchone()
            
        return obj 

class RestLoader:
    WF_KEY = "rest_from_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    
    def __init__(self, pg_conn: PgConnect, settings_repository: DDSEtlSettingsRepository) -> None:
        self.conn = pg_conn
        self.dds = RestDestRepository()
        self.raw = RestRawRepository()
        self.settings_repository = settings_repository

    def parser_js(self, raws: List[RestJsonObj]) -> List[RestDdsObj]:
        res = []
        for r in raws:
            rest_json = json.loads(r.object_value)
            t = RestDdsObj(id=r.id,
                           restaurant_id=rest_json['_id'],
                           restaurant_name=rest_json['name'],
                           active_from = r.update_ts,
                           active_to = '2099-12-31 00:00:00.000'
                           )

            res.append(t)
        return res
    
    def load_rest(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.conn.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            loaded_queue = self.raw.load_raw_rest(conn, last_loaded_id)
            loaded_queue.sort(key=lambda x: x.id)
            rest_load = self.parser_js(loaded_queue)

            for rest in rest_load:
                check_rest = self.dds.get_rest(conn, rest.restaurant_id)
                if not check_rest:
                    self.dds.insert_rest(conn,rest)
                
                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = rest.id
                self.settings_repository.save_setting(conn, wf_setting)
