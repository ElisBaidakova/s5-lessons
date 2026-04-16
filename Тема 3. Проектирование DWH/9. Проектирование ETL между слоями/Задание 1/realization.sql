INSERT INTO cdm.dm_settlement_report (
    restaurant_id,
    restaurant_name,
    settlement_date,
    orders_count,
    orders_total_sum,
    orders_bonus_payment_sum,
    orders_bonus_granted_sum,
    order_processing_fee,
    restaurant_reward_sum
)
WITH closed_orders AS (
    SELECT 
        o.restaurant_id,
        r.restaurant_name,
        t.date AS settlement_date,
        COUNT(DISTINCT o.order_key) AS orders_count,
        SUM(ps.total_sum) AS orders_total_sum,
        SUM(ps.bonus_payment) AS orders_bonus_payment_sum,
        SUM(ps.bonus_grant) AS orders_bonus_granted_sum
    FROM 
        dds.dm_orders o
    JOIN 
        dds.dm_restaurants r ON o.restaurant_id = r.id
    JOIN 
        dds.dm_timestamps t ON o.timestamp_id = t.id
    JOIN 
        dds.fct_product_sales ps ON o.id = ps.order_id
    WHERE 
        LOWER(o.order_status) = 'closed' 
        -- AND r.active_to > CURRENT_DATE
    GROUP BY 
        o.restaurant_id, r.restaurant_name, settlement_date
)
SELECT 
    co.restaurant_id,
    co.restaurant_name,
    co.settlement_date,
    co.orders_count,
    co.orders_total_sum,
    co.orders_bonus_payment_sum,
    co.orders_bonus_granted_sum,
    0.25 * co.orders_total_sum AS order_processing_fee,
    co.orders_total_sum - 0.25 * co.orders_total_sum - co.orders_bonus_payment_sum AS restaurant_reward_sum
FROM 
    closed_orders co
ON CONFLICT (restaurant_id, settlement_date) DO UPDATE set
	restaurant_name = EXCLUDED.restaurant_name,
    orders_count = EXCLUDED.orders_count,
    orders_total_sum = EXCLUDED.orders_total_sum,
    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
    order_processing_fee = EXCLUDED.order_processing_fee,
    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;