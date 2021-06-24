create external table if not exists bi_corp_bdr.config_bdr_variables
  (
    month_partition_date string,
 -- general
    last_working_day_arda string,
    json_extra_params string
   )
 partitioned by (table string)
 STORED AS parquet location '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/config/config_bdr_variables'