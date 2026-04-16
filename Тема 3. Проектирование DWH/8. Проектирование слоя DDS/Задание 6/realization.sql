create table if not exists dds.dm_timestamps(
  id serial primary key,
  ts timestamp not null,
  year smallint not null check (year >= 2022 AND year < 2500),
  month smallint not null check (month >= 1 AND month <= 12),
  day smallint not null check (day >= 1 AND day <= 31),
  time time not null,
  date date not null
  );