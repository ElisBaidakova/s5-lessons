CREATE SCHEMA IF NOT EXISTS stg;
CREATE TABLE IF NOT EXISTS stg.bonussystem_users(
    id INTEGER CONSTRAINT bonussystem_users_pkey PRIMARY KEY,
    order_user_id text NOT NULL
);
CREATE TABLE IF NOT EXISTS stg.bonussystem_ranks(
    id INTEGER CONSTRAINT bonussystem_ranks_pkey PRIMARY KEY,
    name VARCHAR(2048) NOT NULL,
    bonus_percent NUMERIC(19, 5) NOT NULL,
    min_payment_threshold NUMERIC(19, 5) NOT NULL
);
CREATE TABLE IF NOT EXISTS stg.bonussystem_events(
    id INTEGER CONSTRAINT bonussystem_events_pkey PRIMARY KEY,
    event_ts TIMESTAMP NOT NULL,
    event_type VARCHAR NOT NULL,
    event_value TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_bonussystem_events__event_ts
ON stg.bonussystem_events(event_ts);