use warehouse DAS42DEV;
use database airflow_db_dev;
use schema transform_stage_teddy_kevin;


CREATE TABLE IF NOT EXISTS TRANSFORM_STAGE_TEDDY_KEVIN.STATE(
  record_type string,
  date string,
  time string,
  event string,
  ipn string,
  iab_flag string,
  config_id string,
  impression_id string,
  ip_address string,
  product string,
  data string,
  impression_guid string,
  file_source string,
  load_timestamp timestamp,
  run_datehour bigint
);

INSERT INTO transform_stage_teddy_kevin.STATE (record_type,date,time,event,ipn, iab_flag, config_id, impression_id ,ip_address, product, data,impression_guid,file_source,load_timestamp,run_datehour) SELECT
record_type,date,time,event,ipn, iab_flag, config_id, impression_id, ip_address, product, data,impression_guid,file_source,load_timestamp,run_datehour FROM raw_stage_teddy_kevin.state_stream WHERE
metadata$action = 'INSERT';