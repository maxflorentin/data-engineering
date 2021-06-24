CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_omdmreglas (
cod_adm_tramite string,
ds_adm_tporegistro string,
cod_adm_conjregladecision string,
cod_adm_razon string,
cod_adm_secuencia int,
ts_adm_fecproceso string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_omdmreglas';
