CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_ind_omdmdatosadicionales (
cod_adm_tipotramite string,
cod_adm_tramite string,
ts_adm_fecproceso string,
cod_adm_tipoparticipante string,
ds_adm_automarca string,
ds_adm_automodelo string,
int_adm_antmesvehiculo int,
int_adm_cantidadhijosedadescolar int,
int_adm_cantidadmesesaltamatriculaprofesional int,
cod_adm_marcagrupomedicinaprepaga string,
flag_adm_marcatno string,
cod_adm_matriculaanio string,
fc_adm_montomescuotacolegio double,
fc_adm_montomesexpensas double,
fc_adm_montomesmedicinaprepaga double,
cod_adm_profesion string,
fc_adm_valorvehiculo double,
ds_adm_json string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_ind_omdmdatosadicionales'
;