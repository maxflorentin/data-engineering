create external table if not exists bi_corp_staging.bcra_sector(
cod_actividad_f883 string,
sector string
)
PARTITIONED BY (partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/manual/adicional_juridica/bcra_sector';