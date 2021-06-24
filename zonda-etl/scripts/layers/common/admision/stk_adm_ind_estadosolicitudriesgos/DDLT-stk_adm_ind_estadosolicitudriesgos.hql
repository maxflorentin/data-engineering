CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_ind_estadosolicitudriesgos (
cod_adm_tramite string,
cod_suc_sucursal int,
cod_adm_nrosolicitud bigint,
cod_adm_legajo string,
ts_adm_fecha string,
ts_adm_hora  string,
cod_adm_estado bigint,
ds_adm_estado  string,
cod_adm_sector string,
cod_adm_usuario string,
ds_adm_obs_estado string,
dt_adm_feciniresol  string,
dt_adm_fecfinresol  string,
ds_adm_motresol1 string,
ds_adm_motresol2 string,
ds_adm_motresol3 string,
ds_adm_motresol4 string,
ds_adm_motresol5 string,
ds_adm_motingresosup1 string,
ds_adm_motingresosup2 string,
ds_adm_motingresosup3 string,
ds_adm_motingresosup4 string,
ds_adm_motingresosup5 string,
ds_adm_motrecalificacion string,
ds_adm_secestado int
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_ind_estadosolicitudriesgos'
;