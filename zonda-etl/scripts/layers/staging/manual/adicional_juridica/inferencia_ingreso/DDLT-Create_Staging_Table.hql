create external table if not exists bi_corp_staging.inferencia_ingreso(
nup string,
ingreso  string
)
PARTITIONED BY (partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/manual/adicional_juridica/inferencia_ingreso';