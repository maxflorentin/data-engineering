CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_omdm (
cod_adm_tipotramite string,
cod_adm_tramite string,
ts_adm_fecproceso string,
ts_adm_fecrequest string,
ts_adm_fecresponse string,
ts_adm_fecfinproc string,
ds_adm_desobservacion string,
cod_adm_decision string,
ds_adm_tipodecision string,
ds_adm_desnombreflujo string,
ds_adm_desultimopaso string,
ds_adm_descategoriaproducto string,
ds_adm_descodproducto string,
ds_adm_indicadorestado string,
ds_adm_desnombreestrategia string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_omdm';



