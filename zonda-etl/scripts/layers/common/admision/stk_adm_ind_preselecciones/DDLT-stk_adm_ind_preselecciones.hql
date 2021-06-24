CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_ind_preselecciones (
ds_adm_codpromocion string,
ds_adm_tpodoc string,
fc_adm_nrodoc bigint,
ts_adm_fecvigencia string,
ds_mar_sexo string,
fc_adm_moningresos bigint,
cod_prod_ofertado string,
ds_adm_cuibatch string,
fc_adm_scoreverazpreseleccion bigint,
fc_adm_scoretriad bigint,
fc_adm_asistenciavisa bigint,
fc_adm_asistenciaamex bigint,
fc_adm_asistenciaac bigint,
fc_adm_asistenciapp bigint,
fc_adm_debtburden bigint,
fc_adm_afectacion bigint,
fc_adm_asistenciamaster bigint,
fc_adm_asistenciavisabusiness bigint,
fc_adm_asistenciapmomono bigint,
fc_adm_asistenciacesionch bigint,
fc_adm_cuotaprestamo bigint,
fc_adm_valorcuotasprestamo bigint,
cod_prod_producto string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_ind_preselecciones'
;