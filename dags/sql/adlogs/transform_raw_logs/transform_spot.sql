use warehouse DAS42DEV;
use database airflow_db_dev;
use schema transform_stage_teddy_kevin;



CREATE TABLE IF NOT EXISTS TRANSFORM_STAGE_TEDDY_KEVIN.SPOT(
  record_type string,
  date string,
  time string,
  spotlight_id string,
  advertiser_id string,
  spotlightgroup_id string,
  GUID string,
  queryArgs string,
  browser string,
  ip_address string,
  privacy string,
  track_id string,
  user_agent string,
  spotlight_request_guid string,
  file_source string,
  load_timestamp timestamp,
  run_datehour bigint
);

INSERT INTO transform_stage_teddy_kevin.SPOT (record_type,date,time,spotlight_id,advertiser_id,spotlightgroup_id,GUID,queryArgs,browser,ip_address,privacy,track_id,user_agent,spotlight_request_guid,file_source,
load_timestamp,run_datehour) SELECT record_type,date,time,spotlight_id,advertiser_id,spotlightgroup_id,GUID,queryArgs,browser,ip_address,privacy,track_id,user_agent,spotlight_request_guid,file_source,
load_timestamp,run_datehour FROM raw_stage_teddy_kevin.spot_stream WHERE metadata$action = 'INSERT';