/* витрина */
create table if not exists cdm.dm_courier_ledger(
id serial primary key,
courier_id varchar not null,
courier_name varchar not null,
settlement_year int2 not null,
settlement_month int2 not null,
orders_count int2 default 0 not null,
orders_total_sum numeric(14, 2) default 0 not null,
rate_avg float default 0 not null,
order_processing_fee numeric(14, 2) default 0 not null,
courier_order_sum numeric(14, 2) default 0 not null,
courier_tips_sum numeric(14, 2) default 0 not null,
courier_reward_sum numeric(14, 2) default 0 not null
);
/* dds */
create table if not exists dds.dm_couriers(
id serial primary key,
courier_id VARCHAR not null,
courier_name varchar not null
);

create table if not exists dds.dm_deliveries(
delivery_id varchar not null,
courier_id varchar not null,
order_sum numeric(14, 2) default 0 not null,
courier_tips_sum numeric(14, 2) default 0 not null,
courier_rate float default 0 not null
);

alter table dds.dm_couriers add constraint dm_couriers_courier_id_unique unique(courier_id);
alter table dds.dm_deliveries add constraint dm_deliveries_delivery_id_unique unique(delivery_id);

alter table dds.dm_orders add column courier_id int not null;
alter table dds.dm_orders add constraint dm_orders_courier_id_fkey foreign key (courier_id) references dds.dm_couriers (id);

/* stg */
create table if not exists stg.deliverysystem_restaurants(
id serial primary key,
restaurant_id VARCHAR(255) not null,
restaurant_data text not null,
update_ts timestamp not null
);
create table if not exists stg.deliverysystem_couriers(
id serial primary key,
courier_id VARCHAR(255) not null,
courier_data text not null,
update_ts timestamp not null
);
create table if not exists stg.deliverysystem_deliveries(
id serial primary key,
delivery_id VARCHAR(255) not null,
delivery_data text not null,
update_ts timestamp not null
);

ALTER TABLE stg.deliverysystem_restaurants 
ADD CONSTRAINT couriersystem_restaurants_restaurant_id_unique 
UNIQUE (restaurant_id);

ALTER TABLE stg.deliverysystem_couriers 
ADD CONSTRAINT couriersystem_couriers_courier_id_unique
UNIQUE (courier_id);

ALTER TABLE stg.deliverysystem_deliveries 
ADD CONSTRAINT couriersystem_deliveries_delivery_id_unique
UNIQUE (delivery_id);

-- индекс для ускорения
CREATE INDEX idx_restaurants_restaurant_id 
ON stg.deliverysystem_restaurants(restaurant_id);

CREATE INDEX idx_couriers_courier_id 
ON stg.deliverysystem_couriers(courier_id);

CREATE INDEX idx_deliveries_delivery_id 
ON stg.deliverysystem_deliveries(delivery_id);

CREATE UNIQUE INDEX IF NOT EXISTS idx_dm_courier_ledger_unique 
ON cdm.dm_courier_ledger (courier_id, settlement_year, settlement_month);