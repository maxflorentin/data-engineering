create external table if not exists bi_corp_staging.ifrs_provisiones(
ctacgbal string,
importe string
)
PARTITIONED BY (partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/manual/exp_no_contr/ifrs_provisiones';