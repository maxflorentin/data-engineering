CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.normalizacion_id_garantias(
id_cto_bdr string
,source string
,id_cto_source string
)
PARTITIONED BY (partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/normalizacion_id_garantias';