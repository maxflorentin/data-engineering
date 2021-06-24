CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_inv_canales_fondos (
cod_inv_agente_colocador int,
cod_inv_canal int,
ds_inv_canal string,
cod_inv_tipo_canal int,
cod_inv_estado_canal int,
flag_inv_bp_canal int,
cod_inv_origen string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/inversiones/dim/dim_inv_canales_fondos'