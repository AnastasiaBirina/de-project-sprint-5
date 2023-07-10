import logging

import pendulum
from examples.dds import DDSEtlSettingsRepository
from airflow.decorators import dag, task
from examples.dds.dds_dag.fct_products_loader import FctProductsLoader
from examples.dds.dds_dag.users_loader import UserLoader
from examples.dds.dds_dag.rest_loader import RestLoader
from examples.dds.dds_dag.ts_loader import TimestampLoader
from examples.dds.dds_dag.couriers_loader import CourierLoader
from examples.dds.dds_dag.products_loader import ProductLoader
from examples.dds.dds_dag.orders_loader import OrderLoader
from examples.dds.dds_dag.deliveries_loader import DeliveryLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2023, 6, 22, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'dds', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def dds_load_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="load_users")
    def load_users():
        user_loader = UserLoader(dwh_pg_connect, DDSEtlSettingsRepository())
        user_loader.load_users()  # Вызываем функцию, которая перельет данные.

    @task(task_id="load_rest")
    def load_rest():
        rest_loader = RestLoader(dwh_pg_connect, DDSEtlSettingsRepository())
        rest_loader.load_rest()  # Вызываем функцию, которая перельет данные.

    @task(task_id="load_ts")
    def load_ts():
        ts_loader = TimestampLoader(dwh_pg_connect, DDSEtlSettingsRepository())
        ts_loader.load_timestamps()  # Вызываем функцию, которая перельет данные.

    @task(task_id="load_couriers")
    def load_couriers():
        couriers_loader = CourierLoader(dwh_pg_connect, DDSEtlSettingsRepository())
        couriers_loader.load_couriers()  # Вызываем функцию, которая перельет данные.

    @task(task_id="load_deliveries")
    def load_deliveries():
        deliveries_loader = DeliveryLoader(dwh_pg_connect, DDSEtlSettingsRepository(), log)
        deliveries_loader.load_deliveries()  # Вызываем функцию, которая перельет данные.

    @task(task_id="load_products")
    def load_products():
        products_loader = ProductLoader(dwh_pg_connect, DDSEtlSettingsRepository(), log)
        products_loader.load_products()  # Вызываем функцию, которая перельет данные.

    @task(task_id="load_orders")
    def load_orders():
        orders_loader = OrderLoader(dwh_pg_connect, DDSEtlSettingsRepository(), log)
        orders_loader.load_orders()  # Вызываем функцию, которая перельет данные.

    @task(task_id="load_fcts")
    def load_fcts():
        fcts_loader = FctProductsLoader(dwh_pg_connect, DDSEtlSettingsRepository())
        fcts_loader.load_product_facts()  # Вызываем функцию, которая перельет данные.

    # Инициализируем объявленные tasks.
    users_load = load_users()
    rest_load = load_rest()
    ts_load = load_ts()
    products_load = load_products()
    orders_load = load_orders()
    fcts_load = load_fcts()
    couriers_load = load_couriers()
    deliveries_load = load_deliveries()


    # Далее задаем последовательность выполнения tasks.
    [users_load, rest_load, ts_load, couriers_load] >> deliveries_load >> products_load >> orders_load >> fcts_load 


dds_dag = dds_load_dag()
