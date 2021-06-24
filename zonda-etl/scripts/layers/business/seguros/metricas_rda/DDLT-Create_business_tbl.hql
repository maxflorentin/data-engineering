CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_business.tbl_metricas_rda
(
fc_contratos_vivos_4meses  bigint,
fc_contratos_vivos_15meses bigint,
fc_contrataciones_4meses   bigint,
fc_contrataciones_15meses  bigint,
fc_cancelaciones_4meses    bigint,
fc_cancelaciones_15meses   bigint)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/business/seguros/metricas_rda'
