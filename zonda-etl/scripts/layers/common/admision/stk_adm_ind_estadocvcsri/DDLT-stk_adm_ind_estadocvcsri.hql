CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_ind_estadocvcsri (
cod_adm_sector string,
cod_suc_sucursal int,
cod_adm_nrosolicitud bigint,
ts_adm_fecingreso string,
ts_adm_feciniresol string,
ts_adm_fecfinresol string,
cod_adm_estado string,
ds_adm_motresol1 string,
ds_adm_motresol2 string,
ds_adm_motresol3 string,
ds_adm_motresol4 string,
ds_adm_motresol5 string,
cod_adm_analista string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_ind_estadocvcsri'
;