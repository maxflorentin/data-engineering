CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_tie_calendario (
	dt_tie_date	string ,
    dt_tie_datepreviousyear	string ,
    dt_tie_datepreviousmonth	string ,
    dt_tie_datepreviousdate	string ,
    dt_tie_datenextdate string ,
    dt_tie_datepreviousweek	string ,
    dt_tie_nextworkingdate	string ,
    dt_tie_previousworkingdate	string ,
    dt_tie_firstworkingdate	string ,
    dt_tie_lastworkingdate	string ,
    dt_tie_firstdateofmonth	string ,
    dt_tie_lastdateofmonth	string ,
    int_tie_weekofyear	string ,
    int_tie_previousmonthweekofyear	string ,
    int_tie_previousyearweekofyear	string ,
    int_tie_month	int ,
    ds_tie_yearmonth	string ,
    ds_tie_previousmonthyearmonth	string ,
    ds_tie_previousyearyearmonth	string ,
    int_tie_year	int ,
    int_tie_previousyear	int ,
    int_tie_dayofweek	int ,
    int_tie_weekofmonth	int ,
    int_tie_quarter	int ,
    int_tie_trimester	int ,
    int_tie_semester	int ,
    ds_tie_day	string ,
    ds_tie_month	string ,
    ds_tie_holiday	string ,
    flag_tie_weekend	int ,
    flag_tie_holiday	int ,
    flag_tie_workingday	int )
PARTITIONED BY (
    partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/calendario/dim/dim_tie_calendario'