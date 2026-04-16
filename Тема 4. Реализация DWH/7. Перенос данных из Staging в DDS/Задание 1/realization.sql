create table if not exists dds.srv_wf_settings(
id serial primary key,
workflow_key varchar not null,
workflow_settings json not null);