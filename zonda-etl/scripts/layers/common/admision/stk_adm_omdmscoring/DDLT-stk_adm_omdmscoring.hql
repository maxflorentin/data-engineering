CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_omdmscoring (
cod_adm_tramite string,
cod_adm_nrosecscoring int,
ds_adm_nroscorecard string,
fc_adm_finalscore bigint,
ds_adm_scorerecomend string,
fc_cortefinal bigint,
fc_cortemax bigint,
ts_adm_fecproceso string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_omdmscoring';