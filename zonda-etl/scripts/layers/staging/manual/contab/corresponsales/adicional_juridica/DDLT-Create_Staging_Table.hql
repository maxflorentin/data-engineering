create external table if not exists bi_corp_staging.corresponsales_adicional_juridica(
nup string,
banco string,
cargabal string
)
PARTITIONED BY (partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/manual/contab/corresponsales/adicional_juridica';