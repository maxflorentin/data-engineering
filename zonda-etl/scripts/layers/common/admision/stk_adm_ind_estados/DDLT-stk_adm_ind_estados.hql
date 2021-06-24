CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_ind_estados (
cod_adm_tramite string,
cod_suc_sucursal int,
cod_adm_nrosolicitud bigint,
cod_adm_canal string,
ts_adm_fecingresorio2 string,
ts_adm_fecenviosw1 string,
ts_adm_fecenviosw2 string,
cod_adm_estadosw string,
ts_adm_fecdesicionsw string,
ds_adm_marpedidoveraz string,
ds_adm_marreutilizaveraz string,
ts_adm_fecenvioveraz string,
ts_adm_fecrecepveraz string,
ts_adm_fecingresosrs string,
ts_adm_fecasiganasrs string,
ts_adm_feciniresolsrs string,
ts_adm_fecfinresolsrs string,
ts_adm_fecretrosrs string,
ts_adm_fecreasiganasrs string,
cod_adm_estadosrs string,
ts_adm_fecresolsuc string,
cod_adm_resolsuc string,
ts_adm_fecresolaltas string,
cod_adm_resolaltas string,
ts_adm_fecretro string,
cod_adm_estadoretro string,
cod_adm_estadoactual string,
ts_adm_fecestadoactual string,
cod_adm_estado_asol string,
ts_adm_fecestadoasol string,
ds_adm_nomsectoraltas string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_ind_estados'
;