from datetime import datetime
from typing import Any, Dict
from lib.dict_util import json2str
from psycopg import Connection


class PgSaver:
    def save_restaurant(self, conn: Connection, restaurant_id: str, update_ts: datetime, val: Dict) -> None:
        """restaurant_id: Бизнес-ключ из API (val['_id'])"""
        restaurant_data = json2str(val)

        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO stg.deliverysystem_restaurants(restaurant_id, restaurant_data, update_ts)
                VALUES (%(restaurant_id)s, %(restaurant_data)s, %(update_ts)s)
                ON CONFLICT (restaurant_id) DO UPDATE
                SET
                    restaurant_data = EXCLUDED.restaurant_data,
                    update_ts = EXCLUDED.update_ts;
            """, {
                "restaurant_id": restaurant_id,
                "restaurant_data": restaurant_data,
                "update_ts": update_ts
            })

    def save_courier(self, conn: Connection, courier_id: str, update_ts: datetime, val: Dict) -> None:

        courier_data = json2str(val)
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO stg.deliverysystem_couriers(courier_id, courier_data, update_ts)
                VALUES (%(courier_id)s, %(courier_data)s, %(update_ts)s)
                ON CONFLICT (courier_id) DO UPDATE
                SET
                    courier_data = EXCLUDED.courier_data,
                    update_ts = EXCLUDED.update_ts;
            """, {
                "courier_id": courier_id,
                "courier_data": courier_data,
                "update_ts": update_ts
            })

    def save_delivery(self, conn: Connection, delivery_id: str, update_ts: datetime, val: Dict) -> None:

        delivery_data = json2str(val)
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO stg.deliverysystem_deliveries(delivery_id, delivery_data, update_ts)
                VALUES (%(delivery_id)s, %(delivery_data)s, %(update_ts)s)
                ON CONFLICT (delivery_id) DO UPDATE
                SET
                    delivery_data = EXCLUDED.delivery_data,
                    update_ts = EXCLUDED.update_ts;
            """, {
                "delivery_id": delivery_id,
                "delivery_data": delivery_data,
                "update_ts": update_ts
            })
