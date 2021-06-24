CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_ind_omdmbcra (
cod_adm_tipotramite string,
cod_adm_tramite string,
ts_adm_fecproceso string,
cod_adm_tipoparticipante string,
cod_adm_entidadbcra string,
fc_adm_montobcra double,
int_adm_periodobcra int,
cod_adm_situacionbcra string,
ds_adm_json string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_ind_omdmbcra'
;