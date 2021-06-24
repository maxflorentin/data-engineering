CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_ind_omdmdatosuniversitarios (
cod_adm_tipotramite string,
cod_adm_tramite string,
ts_adm_fecproceso string,
cod_adm_tipoparticipante string,
int_adm_cantidadmateriasfaltantes int,
cod_adm_carrera string,
dt_adm_fechaegreso string,
dt_adm_fechaingreso string,
cod_adm_segmentouniveritario string,
cod_adm_tipouniversidad string,
cod_adm_universidad string,
ds_adm_json string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_ind_omdmdatosuniversitarios';