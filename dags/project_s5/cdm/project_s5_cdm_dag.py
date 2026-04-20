import logging
import pendulum

from airflow.decorators import dag, task
from lib import ConnectionBuilder


log = logging.getLogger(__name__)


@dag(
    schedule_interval='0 2 1 * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5', 'project', 'cdm'],
    is_paused_upon_creation=True,
)
def project_sprint5_cdm_dag():
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task()
    def load_courier_data_to_cdm():
        with dwh_pg_connect.connection() as conn:
            with conn.cursor() as cur:
                loaded_count = 0
                # Заполнение витрины
                cur.execute("""
                    INSERT INTO cdm.dm_courier_ledger(
                    courier_id,
                    courier_name,
                    settlement_year,
                    settlement_month,
                    orders_count,
                    orders_total_sum,
                    rate_avg,
                    order_processing_fee,
                    courier_order_sum,
                    courier_tips_sum,
                    courier_reward_sum)
                    WITH base_calc AS (
                    SELECT
                        dcur.courier_id,
                        dcur.courier_name,
                        dt.year AS settlement_year,
                        dt.month AS settlement_month,
                        COUNT(DISTINCT dor.order_key) AS orders_count,
                        COALESCE(SUM(fct.total_sum), 0) AS orders_total_sum,
                        COALESCE(AVG(ddd.courier_rate), 0) AS rate_avg,
                        COALESCE(SUM(fct.total_sum), 0) * 0.25 AS order_processing_fee,
                        GREATEST(
                            CASE
                                WHEN COALESCE(AVG(ddd.courier_rate), 0) < 4.0 THEN COALESCE(SUM(ddd.order_sum), 0) * 0.05
                                WHEN COALESCE(AVG(ddd.courier_rate), 0) < 4.5 THEN COALESCE(SUM(ddd.order_sum), 0) * 0.07
                                WHEN COALESCE(AVG(ddd.courier_rate), 0) < 4.9 THEN COALESCE(SUM(ddd.order_sum), 0) * 0.08
                                ELSE COALESCE(SUM(ddd.order_sum), 0) * 0.10
                            END,
                            CASE
                                WHEN COALESCE(AVG(ddd.courier_rate), 0) < 4.0 THEN 100.0
                                WHEN COALESCE(AVG(ddd.courier_rate), 0) < 4.5 THEN 150.0
                                WHEN COALESCE(AVG(ddd.courier_rate), 0) < 4.9 THEN 175.0
                                ELSE 200.0
                            END
                        ) AS courier_order_sum,

                        COALESCE(SUM(ddd.courier_tips_sum), 0) AS courier_tips_sum
                    FROM dds.dm_couriers dcur
                    JOIN dds.dm_orders dor ON dcur.id = dor.courier_id
                    JOIN dds.dm_timestamps dt ON dor.timestamp_id = dt.id
                    JOIN dds.fct_product_sales fct ON dor.id = fct.order_id
                    JOIN dds.dm_deliveries ddd ON dcur.courier_id = ddd.courier_id
                    GROUP BY dcur.courier_id, dcur.courier_name, dt.year, dt.month
                )
                SELECT
                    courier_id,
                    courier_name,
                    settlement_year,
                    settlement_month,
                    orders_count,
                    orders_total_sum,
                    rate_avg,
                    order_processing_fee,
                    courier_order_sum,
                    courier_tips_sum,
                    courier_order_sum + (courier_tips_sum * 0.95) AS courier_reward_sum
                FROM base_calc
                ON CONFLICT (courier_id, settlement_year, settlement_month) DO UPDATE SET
                    courier_name = EXCLUDED.courier_name,
                    orders_count = EXCLUDED.orders_count,
                    orders_total_sum = EXCLUDED.orders_total_sum,
                    rate_avg = EXCLUDED.rate_avg,
                    order_processing_fee = EXCLUDED.order_processing_fee,
                    courier_order_sum = EXCLUDED.courier_order_sum,
                    courier_tips_sum = EXCLUDED.courier_tips_sum,
                    courier_reward_sum = EXCLUDED.courier_reward_sum;
                """)

                conn.commit()  # Фиксация транзакции

                loaded_count = cur.rowcount
                log.info(f'Loaded {loaded_count} rows to cdm.dm_courier_ledger')
                if loaded_count == 0:
                    log.warning("No new data was loaded to cdm.dm_courier_ledger")
                return loaded_count

    cdm_courier_ledger_task = load_courier_data_to_cdm()

    cdm_courier_ledger_task


cdm_project_dag = project_sprint5_cdm_dag()
