CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_ind_estadossolicitud (
cod_adm_tramite string,
cod_suc_sucursal int,
cod_adm_nrosolicitud bigint,
cod_adm_estado int,
ts_adm_estado string,
ts_adm_hraestado bigint,
cod_adm_usuario string,
cod_adm_sectorusu string,
cod_nro_cuenta bigint,
fc_adm_limvisa bigint,
fc_adm_limacte bigint,
fc_adm_limppp bigint,
fc_adm_limamex bigint,
fc_adm_limcheq bigint,
fc_adm_limptmomon string,
ds_adm_origen string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_ind_estadossolicitud'
;