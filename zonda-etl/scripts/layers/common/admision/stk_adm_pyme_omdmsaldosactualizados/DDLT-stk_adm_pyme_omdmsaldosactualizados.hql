CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_pyme_omdmsaldosactualizados (
cod_adm_tipotramite string,
cod_adm_tramite string,
ts_adm_fecproceso string,
cod_adm_tipoparticipante string,
fc_adm_acctarjeta double,
ds_adm_banco string,
fc_adm_chequescesionados double,
cod_adm_codbanco int,
int_adm_plazorestante int,
fc_adm_saldodeuda double,
ds_adm_json string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_pyme_omdmsaldosactualizados';


