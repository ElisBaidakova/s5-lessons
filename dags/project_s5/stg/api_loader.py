import json
from datetime import datetime, timedelta
from logging import Logger
from typing import Dict, List

from examples.stg import EtlSetting, StgEtlSettingsRepository
from project_s5.stg.api_reader import ExternalAPIReader
from project_s5.stg.pg_saver_from_api import PgSaver
from lib import PgConnect
from lib.dict_util import json2str


# КЛАСС ЗАГРУЗЧИКА
class APILoader:
    """
    Оркестрация загрузки данных из API в STG-слой.
    Поддерживает инкрементальную загрузку через srv_wf_settings.
    """
    # Ключи workflow для отслеживания прогресса
    WF_KEY_RESTAURANTS = "deliverysystem_restaurants_to_stg"
    WF_KEY_COURIERS = "deliverysystem_couriers_to_stg"
    WF_KEY_DELIVERIES = "deliverysystem_deliveries_to_stg"
    LAST_UPDATED_KEY = "last_updated"

    def __init__(
        self,
        api_reader: ExternalAPIReader,
        pg_saver: PgSaver,
        pg_dest: PgConnect,
        logger: Logger
    ):
        self.api_reader = api_reader
        self.pg_saver = pg_saver
        self.pg_dest = pg_dest
        self.settings = StgEtlSettingsRepository()
        self.log = logger

    # ЗАГРУЗКА РЕСТОРАНОВ
    def sync_restaurants(self) -> int:
        """Загружает рестораны с отслеживанием прогресса"""
        with self.pg_dest.connection() as conn:
            # 1. Читаем прогресс
            wf_setting = self.settings.get_setting(conn, self.WF_KEY_RESTAURANTS)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY_RESTAURANTS,
                    workflow_settings={self.LAST_UPDATED_KEY: None}
                )

            last_updated = wf_setting.workflow_settings.get(self.LAST_UPDATED_KEY)
            self.log.info(f"Starting restaurants sync from: {last_updated}")

            # 2. Загружаем из API все данные
            restaurants: List[Dict] = self.api_reader.fetch_all_restaurants()

            self.log.info(f"Fetched {len(restaurants)} restaurants from API")

            if not restaurants:
                self.log.info("No restaurants to load. Quitting.")
                return 0

            # 3. Сохраняем в STG
            loaded_count = 0
            for restaurant in restaurants:
                self.pg_saver.save_restaurant(
                    conn=conn,
                    restaurant_id=restaurant["_id"],  # Бизнес-ключ
                    update_ts=datetime.now(),
                    val=restaurant
                )
                loaded_count += 1

            # 4. Сохраняем прогресс
            wf_setting.workflow_settings[self.LAST_UPDATED_KEY] = datetime.now().isoformat()
            self.settings.save_setting(
                conn,
                wf_setting.workflow_key,
                json2str(wf_setting.workflow_settings)
            )

            self.log.info(f"Loaded {loaded_count} restaurants to STG")
            return loaded_count

    # ЗАГРУЗКА КУРЬЕРОВ
    def sync_couriers(self) -> int:
        """Загружает курьеров с отслеживанием прогресса"""
        with self.pg_dest.connection() as conn:
            # 1. Читаем прогресс
            wf_setting = self.settings.get_setting(conn, self.WF_KEY_COURIERS)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY_COURIERS,
                    workflow_settings={self.LAST_UPDATED_KEY: None}
                )

            last_updated = wf_setting.workflow_settings.get(self.LAST_UPDATED_KEY)
            self.log.info(f"Starting couriers sync from: {last_updated}")

            # 2. Загружаем из API все данные
            couriers: List[Dict] = self.api_reader.fetch_all_couriers()

            self.log.info(f"Fetched {len(couriers)} couriers from API")

            if not couriers:
                self.log.info("No couriers to load. Quitting.")
                return 0

            # 3. Сохраняем в STG
            loaded_count = 0
            for courier in couriers:
                self.pg_saver.save_courier(
                    conn=conn,
                    courier_id=courier["_id"],  # Бизнес-ключ
                    update_ts=datetime.now(),
                    val=courier
                )
                loaded_count += 1

            # 4. Сохраняем прогресс
            wf_setting.workflow_settings[self.LAST_UPDATED_KEY] = datetime.now().isoformat()
            self.settings.save_setting(
                conn,
                wf_setting.workflow_key,
                json2str(wf_setting.workflow_settings)
            )

            self.log.info(f"Loaded {loaded_count} couriers to STG")
            return loaded_count

    # ЗАГРУЗКА ДОСТАВОК
    def sync_deliveries(self, restaurant_id: str) -> int:
        """
        Загружает доставки для конкретного ресторана за 7 дней.
        restaurant_id: ID ресторана для фильтрации доставок
        """
        with self.pg_dest.connection() as conn:
            # 1. Читаем прогресс
            wf_setting = self.settings.get_setting(conn, self.WF_KEY_DELIVERIES)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY_DELIVERIES,
                    workflow_settings={
                        self.LAST_UPDATED_KEY: None,
                        "last_restaurant_id": None
                    }
                )

            # 2. Вычисляем период (7 дней)
            to_date = datetime.now()
            from_date = to_date - timedelta(days=7)

            self.log.info(f"Starting deliveries sync for restaurant {restaurant_id}")
            self.log.info(f"Period: {from_date.strftime('%Y-%m-%d %H:%M:%S')} to {to_date.strftime('%Y-%m-%d %H:%M:%S')}")

            # 3. Загружаем доставки из API
            deliveries: List[Dict] = self.api_reader.fetch_all_deliveries(
                restaurant_id=restaurant_id,
                days_back=7
            )

            self.log.info(f"Fetched {len(deliveries)} deliveries from API")

            if not deliveries:
                self.log.info(f"No deliveries for restaurant {restaurant_id}. Quitting.")
                return 0

            # 4. Сохраняем в STG
            loaded_count = 0
            for delivery in deliveries:
                self.pg_saver.save_delivery(
                    conn=conn,
                    delivery_id=delivery["delivery_id"],  # Бизнес-ключ
                    update_ts=datetime.now(),
                    val=delivery
                )
                loaded_count += 1

            # 5. Сохраняем прогресс
            wf_setting.workflow_settings[self.LAST_UPDATED_KEY] = to_date.isoformat()
            wf_setting.workflow_settings["last_restaurant_id"] = restaurant_id
            self.settings.save_setting(
                conn,
                wf_setting.workflow_key,
                json2str(wf_setting.workflow_settings)
            )

            self.log.info(f"Loaded {loaded_count} deliveries for restaurant {restaurant_id}")
            return loaded_count
