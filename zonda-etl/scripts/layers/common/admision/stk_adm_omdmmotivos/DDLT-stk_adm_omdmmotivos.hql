CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_omdmmotivos (
cod_adm_tipotramite string,
cod_adm_tramite string,
cod_adm_index int,
ds_adm_desresult string,
ds_adm_desstatus string,
ds_adm_desnombre string,
ds_adm_destipo string,
cod_adm_rank int,
cod_adm_decision string,
cod_adm_motivo string,
ts_adm_fecproceso string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_omdmmotivos';