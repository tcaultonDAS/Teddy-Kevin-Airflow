use warehouse DAS42DEV;
use database airflow_db_dev;
use schema transform_stage_teddy_kevin;


CREATE TABLE IF NOT EXISTS TRANSFORM_STAGE_TEDDY_KEVIN.INT(
  ipa string,
  date_field string,
  time_field string,
  agent_string string,
  idad_index string,
  idcreative_config string,
  placementid string,
  smartclip string,
  idevent_type string,
  sid string,
  guid string,
  unhex_md5_smartclip string,
  options string,
  file_source string,
  load_timestamp string,
  run_datehour bigint
);

INSERT INTO transform_stage_teddy_kevin.INT (ipa,date_field,time_field,agent_string,idad_index,idcreative_config,placementid,smartclip,idevent_type,sid,guid,unhex_md5_smartclip,options,file_source,load_timestamp,run_datehour) SELECT
ipa,date_field,time_field,agent_string,idad_index,idcreative_config ,placementid,smartclip,idevent_type,sid,guid,unhex_md5_smartclip,options,file_source,load_timestamp,run_datehour FROM raw_stage_teddy_kevin.int_stream
WHERE metadata$action = 'INSERT';