import logging
import pendulum

from airflow.decorators import dag, task
from lib import ConnectionBuilder


log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'cdm'],
    is_paused_upon_creation=True,
)
def sprint5_cdm_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def load_data_to_cdm():
        with dwh_pg_connect.connection() as conn:
            with conn.cursor() as cur:
                loaded_count = 0
                # Заполнение витрины
                cur.execute("""
                    INSERT INTO cdm.dm_settlement_report(
                        restaurant_id,
                        restaurant_name,
                        settlement_date,
                        orders_count,
                        orders_total_sum,
                        orders_bonus_payment_sum,
                        orders_bonus_granted_sum,
                        order_processing_fee,
                        restaurant_reward_sum)
                    SELECT
                        dr.restaurant_id,
                        dr.restaurant_name,
                        dt.date AS settlement_date,
                        COUNT (DISTINCT dor.order_key) AS orders_count,
                        SUM(fct.total_sum) AS orders_total_sum,
                        SUM(fct.bonus_payment) AS orders_bonus_payment_sum,
                        SUM(fct.bonus_grant) AS orders_bonus_granted_sum,
                        SUM(fct.total_sum) * 0.25 AS order_processing_fee,
                        SUM(fct.total_sum) - SUM(fct.total_sum) * 0.25 - SUM(fct.bonus_payment) AS restaurant_reward_sum
                    FROM dds.dm_restaurants dr
                    JOIN dds.dm_orders dor ON dr.id = dor.restaurant_id
                    JOIN dds.dm_timestamps dt ON dor.timestamp_id = dt.id
                    JOIN dds.fct_product_sales fct ON dor.id = fct.order_id
                    WHERE dor.order_status = 'CLOSED'
                    AND dt.date >= COALESCE((SELECT MAX(settlement_date) FROM cdm.dm_settlement_report), '1970-01-01')
                    GROUP BY dr.restaurant_id, dr.restaurant_name, dt.date
                    ON CONFLICT (restaurant_id, settlement_date) DO UPDATE SET
                        restaurant_name = EXCLUDED.restaurant_name,
                        orders_count = EXCLUDED.orders_count,
                        orders_total_sum = EXCLUDED.orders_total_sum,
                        orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                        orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                        order_processing_fee = EXCLUDED.order_processing_fee,
                        restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
                """)
                loaded_count = cur.rowcount
                log.info(f'Loaded {loaded_count} rows to cdm.dm_settlement_report')
                if loaded_count == 0:
                    log.warning("No new data was loaded to cdm.dm_settlement_report")
                return loaded_count

    cdm_task = load_data_to_cdm()

    cdm_task

cdm_table_dag = sprint5_cdm_dag()
