create table if not exists dds.dm_orders(
  id serial primary key,
  user_id integer not null,
  restaurant_id integer not null,
  timestamp_id integer not null,
  order_key varchar not null,
  order_status varchar not null
  );