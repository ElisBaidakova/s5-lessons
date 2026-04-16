create table if not exists stg.ordersystem_users(
id serial primary key,
object_id varchar not null,
object_value text not null,
update_ts timestamp not null
);
create table if not exists stg.ordersystem_restaurants(
id serial primary key,
object_id varchar not null,
object_value text not null,
update_ts timestamp not null
);
create table if not exists stg.ordersystem_orders(
id serial primary key,
object_id varchar not null,
object_value text not null,
update_ts timestamp not null
);