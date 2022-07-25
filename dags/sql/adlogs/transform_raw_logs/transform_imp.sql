use warehouse DAS42DEV;
use database airflow_db_dev;
use schema transform_stage_teddy_kevin;

CREATE TABLE IF NOT EXISTS TRANSFORM_STAGE_TEDDY_KEVIN.IMP(
  record_type string,
  date string,
  time string,
  idevent_type string,
  placementid string,
  ipn string,
  idcreative string,
  config_id string,
  GUID string,
  geo_db string,
  section string,
  iab_flag string,
  ip_address string,
  rule_match string,
  keyword string,
  custom string,
  privacy string,
  placement_dt string,
  placement_path string,
  creative_dt string,
  creative_path string,
  domain string,
  parent_time string,
  device_id string,
  imp_id string,
  agent_env string,
  user_agent string,
  impression_guid string,
  unhex_md5_smartclip string, --this will be unhex_md5_smartclip
  idcampaign string,
  c2 string,
  c3 string,
  file_source string,
  load_timestamp timestamp,
  run_datehour bigint
);

INSERT INTO transform_stage_teddy_kevin.IMP(record_type, date, time, idevent_type, placementid, ipn, idcreative, config_id, GUID, geo_db, section, iab_flag, ip_address, rule_match, keyword, custom, privacy,
placement_dt,placement_path,creative_dt,domain, parent_time, device_id,imp_id,agent_env,user_agent,impression_guid,unhex_md5_smartclip,idcampaign,c2,c3,file_source,load_timestamp,run_datehour) SELECT
record_type, date, time, idevent_type, placementid, ipn, idcreative, config_id, GUID, geo_db, section, iab_flag, ip_address, rule_match, keyword, custom, privacy,
placement_dt,placement_path,creative_dt,domain, parent_time, device_id,imp_id,agent_env,user_agent,impression_guid,unhex_md5_smartclip,idcampaign,c2,c3,file_source,load_timestamp,run_datehour FROM raw_stage_teddy_kevin.imp_stream
WHERE metadata$action = 'INSERT';

