import logging
import pendulum
import json
from typing import Dict

from airflow.decorators import dag, task
from datetime import date, datetime, time
from lib import ConnectionBuilder
from psycopg.rows import dict_row


def str2json(s: str) -> Dict:
    """Преобразует JSON-строку в словарь Python"""
    return json.loads(s)


log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'dds', 'users', 'restaurants', 'products', 'orders', 'timestamps', 'facts'],
    is_paused_upon_creation=True,
)
def sprint5_dds_users_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def load_users_to_dds():
        # Читает из stg.ordersystem_users, парсит JSON, вставляет в dds.dm_users
        with dwh_pg_connect.connection() as conn:
            # row_factory передаём при создании курсора
            with conn.cursor(row_factory=dict_row) as cur:
                # Читаем данные из STG
                cur.execute("""
                    SELECT object_id, object_value
                    FROM stg.ordersystem_users
                    ORDER BY object_id
                """)
                rows = cur.fetchall()

                log.info(f"Found {len(rows)} users in stg.ordersystem_users")

                if not rows:
                    log.info("No users to load. Quitting.")
                    return 0

                # Парсим JSON и вставляем в DDS
                loaded_count = 0
                for row in rows:
                    object_id = row['object_id']
                    object_value = row['object_value']
                    # Парсим JSON-строку в словарь
                    user_data = str2json(object_value)

                    # Извлекаем поля из JSON
                    user_id = user_data.get('_id', str(object_id))
                    user_name = user_data.get('name', '')
                    user_login = user_data.get('login', '')

                    # Вставляем в целевую таблицу
                    cur.execute("""
                        INSERT INTO dds.dm_users(user_id, user_name, user_login)
                        VALUES (%(user_id)s, %(user_name)s, %(user_login)s)
                        ON CONFLICT (user_id) DO UPDATE
                        SET
                            user_name = EXCLUDED.user_name,
                            user_login = EXCLUDED.user_login;
                    """, {
                        "user_id": user_id,
                        "user_name": user_name,
                        "user_login": user_login
                    })
                    loaded_count += 1
                log.info(f"Successfully loaded {loaded_count} users to dds.dm_users")
                return loaded_count

    users_task = load_users_to_dds()

    @task()
    def load_restaurants_to_dds():
        # Константа для неактуальных записей
        ACTIVE_TO_DEFAULT = '2099-12-31 00:00:00'
        with dwh_pg_connect.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                # Читаем данные из STG
                cur.execute("""
                    SELECT object_id, object_value
                    FROM stg.ordersystem_restaurants
                    ORDER BY object_id
                """)
                rows = cur.fetchall()

                log.info(f"Found {len(rows)} restaurants in stg.ordersystem_restaurants")

                if not rows:
                    log.info("No restaurants to load. Quitting.")
                    return 0

                # Парсим JSON и вставляем в DDS
                loaded_count = 0
                for row in rows:
                    object_id = row['object_id']
                    object_value = row['object_value']
                    # Парсим JSON-строку в словарь
                    restaurant_data = str2json(object_value)
                    # Извлекаем поля из JSON
                    restaurant_id = restaurant_data.get('_id', str(object_id))
                    restaurant_name = restaurant_data.get('name', '')
                    active_from = restaurant_data.get('update_ts', '')

                    # Вставляем в целевую таблицу
                    cur.execute("""
                        INSERT INTO dds.dm_restaurants(restaurant_id, restaurant_name, active_from, active_to)
                        VALUES (%(restaurant_id)s, %(restaurant_name)s, %(active_from)s, %(active_to)s)
                        ON CONFLICT (restaurant_id) DO UPDATE
                        SET
                            restaurant_name = EXCLUDED.restaurant_name,
                            active_from = EXCLUDED.active_from,
                            active_to = EXCLUDED.active_to;
                    """, {
                        "restaurant_id": restaurant_id,
                        "restaurant_name": restaurant_name,
                        "active_from": active_from,
                        "active_to": ACTIVE_TO_DEFAULT
                    })
                    loaded_count += 1
                log.info(f"Successfully loaded {loaded_count} restaurants to dds.dm_restaurants")
                return loaded_count

    restaurants_task = load_restaurants_to_dds()

    @task()
    def load_timestamps_to_dds():
        with dwh_pg_connect.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                # Читаем данные из STG
                cur.execute("""
                    SELECT object_id, object_value
                    FROM stg.ordersystem_orders
                    ORDER BY object_id
                """)
                rows = cur.fetchall()

                log.info(f"Found {len(rows)} orders in stg.ordersystem_orders")

                if not rows:
                    log.info("No orders to load. Quitting.")
                    return 0

                # Парсим JSON и вставляем в DDS
                loaded_count = 0
                for row in rows:
                    object_value = row['object_value']
                    # Парсим JSON-строку в словарь
                    order_data = str2json(object_value)
                    # Фильтруем по статусу регистронезависимо
                    status = order_data.get('final_status', '').upper()
                    if status not in ('CLOSED', 'CANCELLED'):
                        continue  # Пропускаем заказы без финального статуса
                
                    ts_str = order_data.get('date', '')  # Извлекаем и парсим дату
                    if not ts_str:
                        continue
                
                    # Парсим строку в datetime
                    try:
                        ts = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                    except ValueError:
                        ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f")
                
                    # Извлекаем компоненты даты
                    year = ts.year
                    month = ts.month
                    day = ts.day
                    time_only = ts.time()
                    date_only = ts.date()

                    # Вставляем в целевую таблицу
                    cur.execute("""
                        INSERT INTO dds.dm_timestamps(ts, year, month, day, time, date)
                        VALUES (%(ts)s, %(year)s, %(month)s, %(day)s, %(time)s, %(date)s)
                        ON CONFLICT (ts) DO UPDATE
                        SET
                            year = EXCLUDED.year,
                            month = EXCLUDED.month,
                            day = EXCLUDED.day,
                            time = EXCLUDED.time,
                            date = EXCLUDED.date;
                    """, {
                        "ts": ts,
                        "year": year,
                        "month": month,
                        "day": day,
                        "time": time_only,
                        "date": date_only
                    })

                    loaded_count += 1
                log.info(f"Successfully loaded {loaded_count} orders to dds.dm_timestamps")
                return loaded_count

    timestamps_task = load_timestamps_to_dds()

    @task()
    def load_products_to_dds():
        ACTIVE_TO_DEFAULT = '2099-12-31 00:00:00'
        with dwh_pg_connect.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                # ЗАГРУЖАЕМ МАППИНГ ДО ЦИКЛА
                cur.execute("""
                SELECT restaurant_id, id
                FROM dds.dm_restaurants
            """)
                # Создаём словарь
                restaurant_map = {row['restaurant_id']: row['id'] for row in cur.fetchall()}
                log.info(f"Loaded {len(restaurant_map)} restaurants to memory")

                # Читаем рестораны из STG
                cur.execute("""
                    SELECT object_id, object_value, update_ts
                    FROM stg.ordersystem_restaurants
                    ORDER BY object_id
                """)
                rows = cur.fetchall()

                log.info(f"Found {len(rows)} restaurants in stg.ordersystem_restaurants")

                if not rows:
                    log.info("No restaurants to load. Quitting.")
                    return 0

                loaded_count = 0

                # Обрабатываем каждый ресторан
                for row in rows:
                    object_value = row['object_value']
                    update_ts = row['update_ts']
                    restaurant_data = str2json(object_value)
                    # Получаем бизнес-ключ
                    business_key = restaurant_data.get('_id', '')
                    if not business_key:
                        log.warning(f"Missing _id in restaurant JSON, skipping")
                        continue
                    restaurant_id_int = restaurant_map.get(business_key)
                    if not restaurant_id_int:
                        log.warning(f"Restaurant {business_key} not found in dm_restaurants")
                        continue

                    active_from = update_ts
                    active_to = ACTIVE_TO_DEFAULT

                    # Получаем меню
                    menu_list = restaurant_data.get('menu', [])
                    if not menu_list:
                        log.info(f"Menu is empty for restaurant {business_key}, skipping...")
                        continue

                    # Вложенный цикл по продуктам
                    for product in menu_list:
                        product_id = product.get('_id', '')
                        product_name = product.get('name', '')
                        product_price = int(product.get('price', 0))

                        # Вставка в БД
                        cur.execute("""
                            INSERT INTO dds.dm_products(restaurant_id, product_id, product_name, product_price, active_from, active_to)
                            VALUES (%(restaurant_id)s, %(product_id)s, %(product_name)s, %(product_price)s, %(active_from)s, %(active_to)s)
                            ON CONFLICT (product_id) DO UPDATE
                            SET
                            restaurant_id = EXCLUDED.restaurant_id,
                            product_name = EXCLUDED.product_name,
                            product_price = EXCLUDED.product_price,
                            active_from = EXCLUDED.active_from,
                            active_to = EXCLUDED.active_to;
                        """, {
                            "restaurant_id": restaurant_id_int,
                            "product_id": product_id,
                            "product_name": product_name,
                            "product_price": product_price,
                            "active_from": active_from,
                            "active_to": active_to
                        })

                        loaded_count += 1
                # Итоговый лог ПОСЛЕ всех циклов
                log.info(f"Successfully loaded {loaded_count} products to dds.dm_products")
                return loaded_count

    products_task = load_products_to_dds()

    @task()
    def load_orders_to_dds():
        with dwh_pg_connect.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                # Кэшируем ВСЕ маппинги ДО цикла (один запрос на таблицу)
                # Маппинг ресторанов: business_key (str) → int id
                cur.execute("SELECT restaurant_id, id FROM dds.dm_restaurants")
                restaurant_map = {row['restaurant_id']: row['id'] for row in cur.fetchall()}
                log.info(f"Loaded {len(restaurant_map)} restaurants to memory")

                # Маппинг временных меток: ts (timestamp) → int id
                cur.execute("SELECT ts, id FROM dds.dm_timestamps")
                timestamp_map = {row['ts'].strftime("%Y-%m-%d %H:%M:%S"): row['id'] for row in cur.fetchall()}
                log.info(f"Loaded {len(timestamp_map)} timestamps to memory")

                # Маппинг пользователей: user_id (str) → int id
                cur.execute("SELECT user_id, id FROM dds.dm_users")
                user_map = {row['user_id']: row['id'] for row in cur.fetchall()}
                log.info(f"Loaded {len(user_map)} users to memory")

                # Маппинг курьеров: courier_id (str) → int id
                cur.execute("SELECT courier_id, id FROM dds.dm_couriers")
                courier_map = {row['courier_id']: row['id'] for row in cur.fetchall()}
                log.info(f"Loaded {len(courier_map)} couriers to memory")

                # Читаем заказы из STG
                cur.execute("""
                    SELECT object_id, object_value
                    FROM stg.ordersystem_orders
                    ORDER BY object_id
                """)
                rows = cur.fetchall()

                log.info(f"Found {len(rows)} orders in stg.ordersystem_orders")
                if not rows:
                    log.info("No orders to load. Quitting.")
                    return 0

                loaded_count = 0
                skipped_no_status = 0
                skipped_no_restaurant = 0
                skipped_no_timestamp = 0
                skipped_no_user = 0
                skipped_no_courier = 0

                # Обрабатываем заказы (маппинги уже в памяти)
                for row in rows:
                    object_value = row['object_value']
                    order_data = str2json(object_value)
                    order_key = order_data.get('_id', str(row['object_id']))
                    order_status = order_data.get('final_status', '').upper()
                    # Фильтр по статусу
                    if order_status not in ('CLOSED', 'CANCELLED'):
                        skipped_no_status += 1
                        continue

                    # Получаем restaurant_id (int)
                    restaurant_obj = order_data.get('restaurant', {})
                    restaurant_bk = restaurant_obj.get('id', '')
                    restaurant_id_int = restaurant_map.get(restaurant_bk)
                    if not restaurant_id_int:
                        skipped_no_restaurant += 1
                        log.warning(f"Restaurant {restaurant_bk} not found in dm_restaurants")
                        continue

                    # Получаем timestamp_id (int)
                    timestamp_bk = order_data.get('date', '')
                    timestamp_id_int = timestamp_map.get(timestamp_bk)
                    if not timestamp_id_int:
                        skipped_no_timestamp += 1
                        log.warning(f"Timestamp {timestamp_bk} not found in dm_timestamps")
                        continue

                    # Получаем user_id (int)
                    user_obj = order_data.get('user', {})
                    user_bk = user_obj.get('id', '')
                    user_id_int = user_map.get(user_bk)
                    if not user_id_int:
                        skipped_no_user += 1
                        log.warning(f"User {user_bk} not found in dm_users")
                        continue

                    # Получаем courier_id (int)
                    courier_obj = order_data.get('user', {})
                    courier_bk = courier_obj.get('id', '')
                    courier_id_int = courier_map.get(courier_bk)
                    if not courier_id_int:
                        skipped_no_courier += 1
                        log.warning(f"Courier {courier_bk} not found in dm_couriers")
                        continue

                    # Вставляем в целевую таблицу
                    cur.execute("""
                        INSERT INTO dds.dm_orders(order_key,order_status, restaurant_id, timestamp_id, user_id, courier_id)
                        VALUES (%(order_key)s, %(order_status)s, %(restaurant_id)s, %(timestamp_id)s, %(user_id)s, %(courier_id)s)
                        ON CONFLICT (order_key) DO UPDATE
                        SET
                            order_status = EXCLUDED.order_status,
                            restaurant_id = EXCLUDED.restaurant_id,
                            timestamp_id = EXCLUDED.timestamp_id,
                            user_id = EXCLUDED.user_id
                            courier_id = EXCLUDED.courier_id;
                    """, {
                        "order_key": order_key,
                        "order_status": order_status,
                        "restaurant_id": restaurant_id_int,
                        "timestamp_id": timestamp_id_int,
                        "user_id": user_id_int,
                        "courier_id": courier_id
                    })
                    loaded_count += 1
                # Логируем результат после цикла
                log.info(f"""
                    Orders loaded: {loaded_count}
                    Skipped (no status): {skipped_no_status}
                    Skipped (no restaurant): {skipped_no_restaurant}
                    Skipped (no timestamp): {skipped_no_timestamp}
                    Skipped (no user): {skipped_no_user}
                    Skipped (no courier): {skipped_no_courier}
                """)

                return loaded_count

    orders_task = load_orders_to_dds()

    @task
    def load_facts_to_dds():
        with dwh_pg_connect.connection() as conn:
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT product_id, id FROM dds.dm_products")
                product_map = {row['product_id']: row['id'] for row in cur.fetchall()}
                log.info(f"Loaded {len(product_map)} products to memory")

                cur.execute("SELECT order_key, id FROM dds.dm_orders")
                order_map = {row['order_key']: row['id'] for row in cur.fetchall()}
                log.info(f"Loaded {len(order_map)} orders to memory")

                # Читаем заказы из STG
                cur.execute("""
                    SELECT event_value
                    FROM stg.bonussystem_events
                    WHERE event_type = 'bonus_transaction'
                """)
                rows = cur.fetchall()

                log.info(f"Found {len(rows)} events in stg.bonussystem_events")
                if not rows:
                    log.info("No events to load. Quitting.")
                    return 0

                loaded_count = 0

                # Обрабатываем суммы
                for row in rows:
                    event_value = row['event_value']
                    fact_data = str2json(event_value)
                    # Получаем список product_payments
                    product_payments_list = fact_data.get('product_payments', [])
                    if not product_payments_list:
                        log.info(f"product_payments is empty for product {business_key}, skipping...")
                        continue
                    # Вложенный цикл по product_payments
                    for product_item in product_payments_list:
                        product_bk = product_item.get('product_id', '')
                        if product_bk not in product_map:
                            log.warning(f"Product {product_bk} not found in dm_products")
                            continue
                        product_id_int = product_map[product_bk]

                        order_bk = fact_data.get('order_id', '')
                        if order_bk not in order_map:
                            log.warning(f"Order {order_bk} not found in dm_orders")
                            continue
                        order_id_int = order_map[order_bk]

                        price = product_item.get('price', 0)
                        count = product_item.get('quantity', 0)
                        total_sum = count * price
                        bonus_payment = product_item.get('bonus_payment', 0)
                        bonus_grant = product_item.get('bonus_grant', 0)

                        # Вставляем в целевую таблицу
                        cur.execute("""
                            INSERT INTO dds.fct_product_sales(product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
                            VALUES (%(product_id)s, %(order_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s)
                            ON CONFLICT (product_id, order_id) DO UPDATE
                            SET
                            count = EXCLUDED.count,
                            price = EXCLUDED.price,
                            total_sum = EXCLUDED.total_sum,
                            bonus_payment = EXCLUDED.bonus_payment,
                            bonus_grant = EXCLUDED.bonus_grant;
                        """, {
                            "order_id": order_id_int,
                            "product_id": product_id_int,
                            "count": count,
                            "price": price,
                            "total_sum": total_sum,
                            "bonus_payment": bonus_payment,
                            "bonus_grant": bonus_grant
                        })
                        loaded_count += 1
                log.info(f"Loaded {loaded_count} facts")
                return loaded_count
    facts_task = load_facts_to_dds()

    users_task >> restaurants_task >> timestamps_task >> products_task >> orders_task >> facts_task


dds_users_dag = sprint5_dds_users_dag()
