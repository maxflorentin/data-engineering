CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_ind_tramiteshash (
cod_adm_tipotramite string,
cod_adm_tramite string,
cod_adm_hash string,
cod_adm_valor string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_ind_tramiteshash'
;