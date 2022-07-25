use warehouse DAS42DEV;
use database airflow_db_dev;
use schema transform_stage_teddy_kevin;



CREATE TABLE IF NOT EXISTS TRANSFORM_STAGE_TEDDY_KEVIN.TRACK(
  record_type string,
  date string,
  time string,
  event string,
  spotlight_id string,
  spotlightgroup_id string,
  track_id string,
  ip_address string,
  product string,
  data string,
  spotlight_request_guid string,
  file_source string,
  load_timestamp timestamp,
  run_datehour bigint);

INSERT INTO transform_stage_teddy_kevin.TRACK (record_type,date,time,event,spotlight_id,spotlightgroup_id,track_id,ip_address,product,data,spotlight_request_guid,file_source,load_timestamp,run_datehour) SELECT
record_type,date,time,event,spotlight_id,spotlightgroup_id,track_id,ip_address,product,data,spotlight_request_guid,file_source,load_timestamp,run_datehour FROM raw_stage_teddy_kevin.track_stream WHERE
metadata$action = 'INSERT';