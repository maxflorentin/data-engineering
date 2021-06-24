CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_adm_ind_concesionario (
cod_adm_concesionario string,
ds_adm_nomconcesionario string,
ts_adm_fecaprobacion string,
ds_adm_marresolcomite string,
ds_adm_martienda string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/dim_adm_ind_concesionario';