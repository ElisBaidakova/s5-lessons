CREATE TABLE IF NOT EXISTS cdm.dm_settlement_report(
id serial not NULL,
restaurant_id varchar not NULL,
restaurant_name varchar not NULL,
settlement_date date not NULL,
orders_count int not NULL,
orders_total_sum numeric(14, 2) not NULL,
orders_bonus_payment_sum numeric(14, 2) not NULL,
orders_bonus_granted_sum numeric(14, 2) not NULL,
order_processing_fee numeric(14, 2) not null,
restaurant_reward_sum numeric(14, 2) not null
);