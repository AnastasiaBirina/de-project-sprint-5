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


class UserJsonObj(BaseModel):
    id: int
    object_id: str
    object_value: str


class UserDdsObj(BaseModel):
    id: int
    user_id: str
    user_name: str
    user_login: str


class UserRawRepository:
    def load_raw_users(self, conn: Connection, last_loaded_record_id: int) -> List[UserJsonObj]:
        with conn.cursor(row_factory=class_row(UserJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        object_id,
                        object_value
                    FROM stg.ordersystem_users
                    WHERE id > %(last_loaded_record_id)s;
                """,
                {"last_loaded_record_id": last_loaded_record_id},
            )
            objs = cur.fetchall()
        return objs
# id serial NOT null primary key,
# 	user_id varchar NOT NULL,
# 	user_name  varchar NOT NULL,
# 	user_login varchar NOT NULL

class UserDestRepository:

    def insert_user(self, conn: Connection, user: UserDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_users(user_id, user_name, user_login)
                    VALUES (%(user_id)s, %(user_name)s, %(user_login)s)
                    
                """,
                {
                    "user_id": user.user_id,
                    "user_name": user.user_name,
                    "user_login": user.user_login
                },
            )
    def get_user(self, conn: Connection, user_id: str) -> Optional[UserDdsObj]:
        with conn.cursor() as cur:
            cur.execute(
                """
                    SELECT id, user_id, user_name, user_login
                    FROM dds.dm_users
                    WHERE user_id = (%(user_id)s)
                """,
                {
                    "user_id": user_id
                }
            )
            obj = cur.fetchone()
            
        return obj 

class UserLoader:
    WF_KEY = "users_from_stg_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    
    def __init__(self, pg_conn: PgConnect, settings_repository: DDSEtlSettingsRepository) -> None:
        self.conn = pg_conn
        self.dds = UserDestRepository()
        self.raw = UserRawRepository()
        self.settings_repository = settings_repository

    def parser_js(self, raws: List[UserJsonObj]) -> List[UserDdsObj]:
        res = []
        for r in raws:
            user_json = json.loads(r.object_value)
            t = UserDdsObj(id=r.id,
                           user_id=user_json['_id'],
                           user_name=user_json['name'],
                           user_login=user_json['login'],
                           )

            res.append(t)
        return res
    
    def load_users(self):
        # открываем транзакцию.
        # Транзакция будет закоммичена, если код в блоке with пройдет успешно (т.е. без ошибок).
        # Если возникнет ошибка, произойдет откат изменений (rollback транзакции).
        with self.conn.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            loaded_queue = self.raw.load_raw_users(conn, last_loaded_id)
            loaded_queue.sort(key=lambda x: x.id)
            users_load = self.parser_js(loaded_queue)

            for user in users_load:
                check_user = self.dds.get_user(conn, user.user_id)
                if not check_user:
                    self.dds.insert_user(conn,user)
                
                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = user.id
                self.settings_repository.save_setting(conn, wf_setting)
