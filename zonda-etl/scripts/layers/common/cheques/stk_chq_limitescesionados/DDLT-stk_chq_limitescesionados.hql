CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_chq_limitescesionados(
cod_per_nup bigint,
cod_chq_entidad string,
cod_chq_sucursal bigint,
cod_chq_paquete string,
fc_chq_limitecesion decimal(15,2),
fc_chq_montocesionado decimal(15,2),
fc_chq_montopendiente decimal(15,2),
fc_chq_montodisponible decimal(15,2))
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/cheques/stk_chq_limitescesionados'