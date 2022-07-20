use warehouse DAS42DEV;
use database airflow_db_dev;
use schema transform_stage_teddy_kevin;
drop table if exists TRACK;
create table TRACK as (select * from raw_stage_teddy_kevin.track);