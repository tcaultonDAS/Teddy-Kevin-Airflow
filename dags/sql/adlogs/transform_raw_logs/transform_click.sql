use warehouse DAS42DEV;
use database airflow_db_dev;
use schema transform_stage_teddy_kevin;


CREATE TABLE IF NOT EXISTS TRANSFORM_STAGE_TEDDY_KEVIN.CLICK(
  record_TYPE STRING,
  date STRING,
  time STRING,
  idevent_type STRING,
  placementid STRING,
  ipn STRING,
  idcreative STRING,
  configuration_id STRING,
  GUID STRING,
  iab_flag STRING,
  ip_address STRING,
  rule_match STRING,
  custom STRING,
  section STRING,
  keyword STRING,
  privacy STRING,
  parent_time STRING,
  device_id STRING,
  imp_id STRING,
  agent_env STRING,
  user_agent STRING,
  impression_guid STRING,
  unhex_md5_smartclip STRING,
  idcampaign STRING,
  c2 STRING,
  c3 STRING,
  file_source STRING,
  load_timestamp TIMESTAMP,
  run_datehour BIGINT)
  ;


INSERT INTO transform_stage_teddy_kevin.CLICK (record_TYPE,date,time,idevent_type,placementid,ipn,idcreative,configuration_id,
GUID,iab_flag,ip_address,rule_match,custom,section,keyword,privacy,parent_time,device_id,imp_id,agent_env,user_agent,
impression_guid,unhex_md5_smartclip,idcampaign,c2,c3,file_source,load_timestamp,run_datehour) SELECT
record_TYPE,date,time,idevent_type,placementid,ipn,idcreative,configuration_id,
GUID,iab_flag,ip_address,rule_match,custom,section,keyword,privacy,parent_time,device_id,imp_id,agent_env,user_agent,
impression_guid,unhex_md5_smartclip,idcampaign,c2,c3,file_source,load_timestamp,run_datehour FROM raw_stage_teddy_kevin.click_stream WHERE
metadata$action = 'INSERT';


