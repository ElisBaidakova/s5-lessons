import logging
import pendulum
import json

from airflow.decorators import dag, task
from datetime import date, datetime, time
from lib import ConnectionBuilder
from psycopg.rows import dict_row
from typing import Dict


def str2json(s: str) -> Dict:
    """Преобразует JSON-строку в словарь Python"""
    return json.loads(s)


log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'dds', 'project', 'restaurants', 'couriers', 'deliveries'],
    is_paused_upon_creation=True,
)
def project_sprint5_dds_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def load_couriers_to_dds():
        # Читает из stg.deliverysystem_couriers, парсит JSON, вставляет в dds.dm_couriers
        with dwh_pg_connect.connection() as conn:
            # row_factory передаём при создании курсора
            with conn.cursor(row_factory=dict_row) as cur:
                # Читаем данные из STG
                cur.execute("""
                    SELECT courier_id, courier_data
                    FROM stg.deliverysystem_couriers
                    ORDER BY courier_id
                """)
                rows = cur.fetchall()

                log.info(f"Found {len(rows)} couriers in stg.deliverysystem_couriers")

                if not rows:
                    log.info("No couriers to load. Quitting.")
                    return 0

                # Парсим JSON и вставляем в DDS
                loaded_count = 0
                for row in rows:
                    courier_data = row['courier_data']
                    # Парсим JSON-строку в словарь
                    courier_details = str2json(courier_data)

                    # Извлекаем поля из JSON
                    courier_id = courier_details.get('_id', '')
                    courier_name = courier_details.get('name', '')

                    # Вставляем в целевую таблицу
                    cur.execute("""
                        INSERT INTO dds.dm_couriers(courier_id, courier_name)
                        VALUES (%(courier_id)s, %(courier_name)s)
                        ON CONFLICT (courier_id) DO UPDATE
                        SET
                            courier_name = EXCLUDED.courier_name;
                    """, {
                        "courier_id": courier_id,
                        "courier_name": courier_name
                    })
                    loaded_count += 1
                log.info(f"Successfully loaded {loaded_count} couriers to dds.dm_couriers")
                return loaded_count

    couriers_task = load_couriers_to_dds()

    @task()
    def load_deliveries_to_dds():
        with dwh_pg_connect.connection() as conn:
            # row_factory передаём при создании курсора
            with conn.cursor(row_factory=dict_row) as cur:
                # Читаем данные из STG
                cur.execute("""
                    SELECT delivery_id, delivery_data
                    FROM stg.deliverysystem_deliveries
                    ORDER BY delivery_id
                """)
                rows = cur.fetchall()

                log.info(f"Found {len(rows)} deliveries in stg.deliverysystem_deliveries")

                if not rows:
                    log.info("No deliveries to load. Quitting.")
                    return 0

                # Парсим JSON и вставляем в DDS
                loaded_count = 0
                for row in rows:
                    delivery_id = row['delivery_id']
                    delivery_data = row['delivery_data']
                    # Парсим JSON-строку в словарь
                    delivery_details = str2json(delivery_data)

                    # Извлекаем поля из JSON
                    courier_id = delivery_details.get('courier_id', '')
                    order_sum = delivery_details.get('sum', '')
                    courier_tips_sum = delivery_details.get('tip_sum', '')
                    courier_rate = delivery_details.get('rate', '')

                    # Вставляем в целевую таблицу
                    cur.execute("""
                        INSERT INTO dds.dm_deliveries(delivery_id, courier_id, order_sum, courier_tips_sum, courier_rate)
                        VALUES (%(delivery_id)s, %(courier_id)s, %(order_sum)s, %(courier_tips_sum)s, %(courier_rate)s)
                        ON CONFLICT (delivery_id) DO UPDATE
                        SET
                            courier_id = EXCLUDED.courier_id,
                            order_sum = EXCLUDED.order_sum,
                            courier_tips_sum = EXCLUDED.courier_tips_sum,
                            courier_rate = EXCLUDED.courier_rate;
                    """, {
                        "delivery_id": delivery_id,
                        "courier_id": courier_id,
                        "order_sum": order_sum,
                        "courier_tips_sum": courier_tips_sum,
                        "courier_rate": courier_rate
                    })
                    loaded_count += 1
                log.info(f"Successfully loaded {loaded_count} deliveries to dds.dm_deliveries")
                return loaded_count

    deliveries_task = load_deliveries_to_dds()

    couriers_task >> deliveries_task

project_dds_dag = project_sprint5_dds_dag()
