use warehouse DAS42DEV;
use database airflow_db_dev;
use schema transform_stage_teddy_kevin;


CREATE TABLE IF NOT EXISTS TRANSFORM_STAGE_TEDDY_KEVIN.INFO(
  record_type string,
  date string,
  time string,
  error_severity string,
  error_number string,
  placement_id string,
  error_message string,
  ip_address string,
  referrer string,
  iab_flag string,
  user_agent string,
  file_source string,
  load_timestamp timestamp,
  run_datehour bigint
);

INSERT INTO transform_stage_teddy_kevin.INFO (record_type,date,time,error_severity,error_number,placement_id,error_message,ip_address,referrer,iab_flag,user_agent,file_source,load_timestamp,run_datehour) SELECT 
record_type,date,time,error_severity,error_number,placement_id,error_message,ip_address,referrer,iab_flag,user_agent,file_source,load_timestamp,run_datehour FROM raw_stage_teddy_kevin.info_stream
WHERE metadata$action = 'INSERT'