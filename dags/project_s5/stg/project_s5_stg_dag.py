import logging
import pendulum
from airflow.decorators import dag, task
from lib import ConnectionBuilder
from lib.api_connect import SimpleAPIClient
from project_s5.stg.api_reader import ExternalAPIReader
from project_s5.stg.pg_saver_from_api import PgSaver
from project_s5.stg.api_loader import APILoader


log = logging.getLogger(__name__)


@dag(
    schedule_interval='0 0 1 * *',  # Задаем расписание выполнения дага - каждые 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'project', 'deliverysystem'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def project_sprint5_stg_delivery_system_dag():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="load_restaurants_from_deliverysystem")
    def load_restaurants():
        api_client = SimpleAPIClient()  # Создаём API-клиент. Параметры внутри класса
        api_reader = ExternalAPIReader(api_client)  # Создаём читатель API
        pg_saver = PgSaver()  # Создаём записчик в STG
        loader = APILoader(api_reader, pg_saver, dwh_pg_connect, log)  # Создаём загрузчик
        return loader.sync_restaurants()  # Запускаем загрузку. Возвращает count (int)

    # Загрузка курьеров
    @task(task_id="load_couriers_from_deliverysystem")
    def load_couriers():
        api_client = SimpleAPIClient()
        api_reader = ExternalAPIReader(api_client)
        pg_saver = PgSaver()
        loader = APILoader(api_reader, pg_saver, dwh_pg_connect, log)
        return loader.sync_couriers()

    # Загрузка доставок
    @task(task_id="load_deliveries_from_deliverysystem")
    def load_deliveries():
        api_client = SimpleAPIClient()
        api_reader = ExternalAPIReader(api_client)
        pg_saver = PgSaver()
        loader = APILoader(api_reader, pg_saver, dwh_pg_connect, log)

        # Доставки загружаются для каждого ресторана
        restaurants = api_reader.fetch_all_restaurants()
        total = 0
        for restaurant in restaurants:
            total += loader.sync_deliveries(restaurant_id=restaurant["_id"])
        return total

    # Инициализация задач
    restaurants_task = load_restaurants()
    couriers_task = load_couriers()
    deliveries_task = load_deliveries()

    # Зависимости: сначала справочники, потом факты
    [restaurants_task, couriers_task] >> deliveries_task

    # Возвращаем задачи
    return [restaurants_task, couriers_task, deliveries_task]


dag_load_from_api_to_stg = project_sprint5_stg_delivery_system_dag()
